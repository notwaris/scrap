import asyncio
import logging
import random
import time
import uuid
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Dict, List, Optional
from options.config import PROXY_BASE
from pathlib import Path

import instaloader
import requests
from urllib.parse import urlparse
import re

try:
    from instagrapi import Client
    from instagrapi.exceptions import (
        ClientError, 
        LoginRequired, 
        RateLimitError,
        ClientNotFoundError
    )
    INSTAGRAPI_AVAILABLE = True
except ImportError:
    INSTAGRAPI_AVAILABLE = False
    logger.warning("instagrapi not available, will only use instaloader")

logger = logging.getLogger("extractor.instagram")

COOKIES_FILE = Path(__file__).parent.parent / "assets" / "cookies.txt"

INITIAL_PROXY_POOL_SIZE = 10
DEFAULT_MIN_DELAY = 1.5
DEFAULT_MAX_DELAY = 3.5
DEFAULT_STICKY_SECONDS = 300
INSTALOADER_RETRIES = 2
INSTAGRAPI_RETRIES = 3
DEFAULT_BACKOFF_SECONDS = [15, 30, 45]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


class ProxyManager:
    def __init__(self, base: str, pool_size: int = INITIAL_PROXY_POOL_SIZE) -> None:
        self.base = base
        self.pool: List[str] = []
        self.bad: Dict[str, int] = {}
        self.quarantine: Dict[str, datetime] = {}
        self.sticky_map: Dict[str, Dict[str, Any]] = {}
        self._fill_pool(pool_size)

    def _fill_pool(self, size: int) -> None:
        for _ in range(size):
            self.pool.append(self._build_proxy())

    def _build_proxy(self, session: Optional[str] = None) -> str:
        if not session:
            session = uuid.uuid4().hex[:8]
        return f"{self.base}?session={session}"

    def get(self, sticky_key: Optional[str] = None) -> str:
        now = datetime.utcnow()
        if sticky_key:
            entry = self.sticky_map.get(sticky_key)
            if entry and entry["expires_at"] > now and entry["proxy"] not in self.quarantine:
                return entry["proxy"]
        for _ in range(len(self.pool)):
            p = random.choice(self.pool)
            if p in self.quarantine and self.quarantine[p] > now:
                continue
            if p in self.bad and self.bad[p] > 3:
                self.quarantine[p] = now + timedelta(minutes=30)
                continue
            if sticky_key:
                self.sticky_map[sticky_key] = {
                    "proxy": p,
                    "expires_at": now + timedelta(seconds=DEFAULT_STICKY_SECONDS),
                }
            return p
        p = self._build_proxy()
        if sticky_key:
            self.sticky_map[sticky_key] = {
                "proxy": p,
                "expires_at": now + timedelta(seconds=DEFAULT_STICKY_SECONDS),
            }
        return p

    def mark_bad(self, proxy: str) -> None:
        self.bad[proxy] = self.bad.get(proxy, 0) + 1
        if self.bad[proxy] >= 3:
            self.quarantine[proxy] = datetime.utcnow() + timedelta(minutes=30)

    def rotate(self) -> str:
        p = self._build_proxy()
        self.pool.append(p)
        return p

    def health_check(self, proxy: str, timeout: int = 15) -> bool:
        try:
            r = requests.head(
                "https://www.instagram.com/",
                proxies={"http": proxy, "https": proxy},
                timeout=timeout,
            )
            return r.status_code in (200, 301, 302)
        except Exception:
            return False


class InstagramExtractor:
    def __init__(
        self,
        proxy_base: str = PROXY_BASE,
        pool_size: int = INITIAL_PROXY_POOL_SIZE,
        min_delay: float = DEFAULT_MIN_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
        sticky_seconds: int = DEFAULT_STICKY_SECONDS,
        max_retries: int = 3,
    ) -> None:
        self.proxy_mgr = ProxyManager(proxy_base, pool_size=pool_size)
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.sticky_seconds = sticky_seconds
        self.max_retries = max_retries
        self.L = self._build_instaloader()
        self._instagrapi_client = None
        self._setup_logger()

    def _setup_logger(self) -> None:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.ERROR)
        
        logging.getLogger("instaloader").setLevel(logging.ERROR)
        logging.getLogger("instagrapi").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)

    def _build_instaloader(self) -> instaloader.Instaloader:
        L = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            max_connection_attempts=1,
            request_timeout=15.0,
        )
        
        if COOKIES_FILE.exists():
            try:
                self._import_netscape_cookies(L, COOKIES_FILE)
            except Exception:
                pass
        
        return L
    
    def _import_netscape_cookies(self, loader: instaloader.Instaloader, cookies_file: Path) -> None:
        import http.cookiejar
        
        cookie_jar = http.cookiejar.MozillaCookieJar()
        try:
            cookie_jar.load(str(cookies_file), ignore_discard=True, ignore_expires=True)
        except Exception:
            return
        
        for cookie in cookie_jar:
            if 'instagram.com' in cookie.domain:
                loader.context._session.cookies.set_cookie(cookie)
        
        loader.context._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'X-IG-App-ID': '936619743392459',
            'X-ASBD-ID': '129477',
            'X-IG-WWW-Claim': '0',
        })

    def _set_proxy_and_headers(self, proxy: str, ua: Optional[str] = None) -> None:
        if not ua:
            ua = random.choice(USER_AGENTS)
        self.L.context._session.proxies = {"http": proxy, "https": proxy}
        self.L.context._session.headers.update(
            {
                "User-Agent": ua,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept": "*/*",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }
        )

    def _extract_shortcode(self, url: str) -> Optional[str]:
        path = urlparse(url).path
        matches = re.search(r"/p/([^/]+)|/reel/([^/]+)|/tv/([^/]+)", path)
        if matches:
            return matches.group(1) or matches.group(2) or matches.group(3)
        return None

    def _get_profile(self, username: str) -> Optional[instaloader.Profile]:
        try:
            return instaloader.Profile.from_username(self.L.context, username)
        except Exception:
            return None

    def _get_instagrapi_client(self) -> Optional['Client']:
        if not INSTAGRAPI_AVAILABLE:
            return None
        
        if self._instagrapi_client is not None:
            return self._instagrapi_client
        
        try:
            cl = Client()
            
            cl.set_user_agent("Instagram 269.0.0.18.75 Android (33/13; 480dpi; 1080x2400; Realme; RMX3660; RE58B8L1; qcom; en_US; 455237034)")
            
            proxy = self.proxy_mgr.get()
            if proxy:
                cl.set_proxy(proxy)
            
            if COOKIES_FILE.exists():
                import http.cookiejar
                
                cookie_jar = http.cookiejar.MozillaCookieJar()
                cookie_jar.load(str(COOKIES_FILE), ignore_discard=True, ignore_expires=True)
                
                sessionid = None
                for cookie in cookie_jar:
                    if 'instagram.com' in cookie.domain and cookie.name == 'sessionid':
                        sessionid = cookie.value
                        break
                
                if sessionid:
                    cl.set_settings({"cookies": {"sessionid": sessionid}})
                    try:
                        cl.login_by_sessionid(sessionid)
                        cl.account_info()
                        logger.info("Successfully authenticated with instagrapi using sessionid")
                    except Exception as e:
                        logger.warning(f"Session login failed, attempting fallback: {e}")
                        cl.private_request("accounts/current_user/?edit=true")
            
            self._instagrapi_client = cl
            return cl
            
        except Exception as e:
            logger.error(f"Failed to initialize instagrapi: {e}")
            return None

    async def _extract_with_instagrapi(self, shortcode: str) -> Dict[str, Any]:
        try:
            cl = self._get_instagrapi_client()
            if not cl:
                raise Exception("Instagrapi client not available")
            
            loop = asyncio.get_event_loop()
            
            media_pk = await loop.run_in_executor(None, cl.media_pk_from_code, shortcode)
            
            def get_media_dict():
                response = cl.private_request(f"media/{media_pk}/info/")
                return response['items'][0] if response and 'items' in response and response['items'] else None
            
            media_dict = await loop.run_in_executor(None, get_media_dict)
            
            if not media_dict:
                raise Exception("Failed to get media info")
            
            user_dict = media_dict.get('user', {})
            caption_dict = media_dict.get('caption', {})
            caption_text = caption_dict.get('text', '') if caption_dict else ''
            
            like_count = media_dict.get('like_count', 0)
            comment_count = media_dict.get('comment_count', 0)
            play_count = media_dict.get('play_count') or media_dict.get('ig_play_count', 0)
            
            result = {
                "success": True,
                "platform": "instagram",
                "url": f"https://www.instagram.com/p/{shortcode}/",
                "code": shortcode,
                "data": {
                    "id": str(media_dict.get('pk', 'N/A')),
                    "caption": caption_text or "No caption",
                    "like_count": like_count if like_count is not None else 0,
                    "comment_count": comment_count if comment_count is not None else 0,
                    "play_count": play_count if play_count is not None else 0,
                    "view_count": play_count if play_count is not None else 0,
                    "share_count": 0,
                    "owner": {
                        "username": user_dict.get('username', 'N/A'),
                        "full_name": user_dict.get('full_name', 'N/A'),
                        "is_verified": user_dict.get('is_verified', False),
                        "followers": user_dict.get('follower_count', 0),
                        "following": user_dict.get('following_count', 0),
                        "posts_count": user_dict.get('media_count', 0),
                        "biography": user_dict.get('biography', ''),
                    },
                    "media_type": "video" if media_dict.get('media_type') == 2 else "photo",
                    "is_video": media_dict.get('media_type') == 2,
                    "taken_at": datetime.fromtimestamp(media_dict.get('taken_at', 0)).isoformat() if media_dict.get('taken_at') else "N/A",
                    "hashtags": [f"#{tag.lstrip('#')}" for tag in caption_text.split() if tag.startswith("#")],
                    "mentions": [tag for tag in caption_text.split() if tag.startswith("@")],
                    "location": None,
                    "url": media_dict.get('image_versions2', {}).get('candidates', [{}])[0].get('url', f"https://www.instagram.com/p/{shortcode}/"),
                    "typename": f"Graph{'Video' if media_dict.get('media_type') == 2 else 'Image'}",
                    "is_sponsored": False,
                    "pcaption": None,
                },
            }
            
            logger.info(f"Successfully extracted data using instagrapi for {shortcode}")
            return result
            
        except Exception as e:
            logger.error(f"Instagrapi extraction failed: {e}")
            raise e

    async def login(self, username: str, password: str, sticky_key: Optional[str] = None) -> bool:
        proxy = self.proxy_mgr.get(sticky_key=sticky_key)
        self._set_proxy_and_headers(proxy)
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, partial(self.L.login, username, password)
            )
            return True
        except Exception:
            return False

    async def _extract_with_instaloader(self, shortcode: str, proxy: Optional[str] = None, ua: Optional[str] = None) -> Dict[str, Any]:
        try:
            if proxy and ua:
                self._set_proxy_and_headers(proxy, ua)
            elif ua:
                if not ua:
                    ua = random.choice(USER_AGENTS)
                self.L.context._session.headers.update({
                    "User-Agent": ua,
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept": "*/*",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                })
            
            loop = asyncio.get_event_loop()
            post = await loop.run_in_executor(None, partial(instaloader.Post.from_shortcode, self.L.context, shortcode))
            
            owner_username = post.owner_username if hasattr(post, "owner_username") else None
            owner_profile = None
            if owner_username:
                owner_profile = await loop.run_in_executor(None, self._get_profile, owner_username)
            
            likes = getattr(post, 'likes', None)
            comments = getattr(post, 'comments', None)
            video_play_count = getattr(post, 'video_play_count', None)
            owner_username_val = getattr(post, 'owner_username', None)
            
            result = {
                "success": True,
                "platform": "instagram",
                "url": f"https://www.instagram.com/p/{shortcode}/",
                "code": shortcode,
                "data": {
                    "id": str(post.mediaid) if hasattr(post, "mediaid") else "N/A",
                    "caption": post.caption if hasattr(post, "caption") and post.caption else "No caption",
                    "like_count": likes if likes is not None else 0,
                    "comment_count": comments if comments is not None else 0,
                    "play_count": video_play_count if video_play_count is not None else 0,
                    "view_count": video_play_count if video_play_count is not None else 0,
                    "share_count": 0,
                    "owner": {
                        "username": owner_username_val if owner_username_val is not None else "N/A",
                        "full_name": owner_profile.full_name if owner_profile and owner_profile.full_name else "N/A",
                        "is_verified": owner_profile.is_verified if owner_profile else False,
                        "followers": owner_profile.followers if owner_profile else 0,
                        "following": owner_profile.followees if owner_profile else 0,
                        "posts_count": owner_profile.mediacount if owner_profile else 0,
                        "biography": owner_profile.biography if owner_profile and owner_profile.biography else "",
                    },
                    "media_type": "video" if post.is_video else "photo",
                    "is_video": post.is_video if hasattr(post, "is_video") else False,
                    "taken_at": post.date_utc.isoformat() if hasattr(post, "date_utc") and post.date_utc else "N/A",
                    "hashtags": list(post.caption_hashtags) if hasattr(post, "caption_hashtags") and post.caption_hashtags else [],
                    "mentions": list(post.caption_mentions) if hasattr(post, "caption_mentions") and post.caption_mentions else [],
                    "location": post.location.name if hasattr(post, "location") and post.location else None,
                    "url": post.url if hasattr(post, "url") else f"https://www.instagram.com/p/{shortcode}/",
                    "typename": post.typename if hasattr(post, "typename") else "N/A",
                    "is_sponsored": post.is_sponsored if hasattr(post, "is_sponsored") else False,
                    "pcaption": post.pcaption if hasattr(post, "pcaption") else None,
                },
            }
            return result
        except Exception as e:
            raise e

    async def extract_data(self, url: str, username: Optional[str] = None, password: Optional[str] = None) -> Dict[str, Any]:
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return {"success": False, "error": "Invalid Instagram URL format"}
        
        for attempt in range(INSTALOADER_RETRIES):
            try:
                await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
                
                if attempt == 0:
                    ua = random.choice(USER_AGENTS)
                    result = await self._extract_with_instaloader(shortcode, proxy=None, ua=ua)
                    return result
                else:
                    proxy = self.proxy_mgr.get()
                    ua = random.choice(USER_AGENTS)
                    result = await self._extract_with_instaloader(shortcode, proxy=proxy, ua=ua)
                    return result
                    
            except Exception as e:
                if "403" in str(e) or "Forbidden" in str(e):
                    break
                if attempt < INSTALOADER_RETRIES - 1:
                    await asyncio.sleep(random.choice(DEFAULT_BACKOFF_SECONDS[:1]))
        
        if INSTAGRAPI_AVAILABLE:
            for attempt in range(INSTAGRAPI_RETRIES):
                try:
                    await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
                    
                    if attempt > 0:
                        proxy = self.proxy_mgr.rotate()
                        cl = self._get_instagrapi_client()
                        if cl:
                            cl.set_proxy(proxy)
                    
                    result = await self._extract_with_instagrapi(shortcode)
                    return result
                    
                except Exception as e:
                    if attempt < INSTAGRAPI_RETRIES - 1:
                        backoff = DEFAULT_BACKOFF_SECONDS[min(attempt, len(DEFAULT_BACKOFF_SECONDS) - 1)]
                        await asyncio.sleep(backoff)
                    else:
                        logger.error(f"Instagrapi failed: {e}")
        
        return {
            "success": False,
            "error": "Instagram scraping failed after all attempts",
        }

    async def login(self, username: str, password: str, sticky_key: Optional[str] = None) -> bool:
        proxy = self.proxy_mgr.get(sticky_key=sticky_key)
        self._set_proxy_and_headers(proxy)
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, partial(self.L.login, username, password)
            )
            return True
        except Exception:
            return False

    async def extract_batch(self, urls: List[str], concurrency: int = 3, username: Optional[str] = None, password: Optional[str] = None) -> List[Dict[str, Any]]:
        sem = asyncio.Semaphore(concurrency)
        results: List[Dict[str, Any]] = []

        async def _worker(u: str) -> None:
            async with sem:
                r = await self.extract_data(u, username=username, password=password)
                results.append(r)

        tasks = [_worker(u) for u in urls]
        await asyncio.gather(*tasks)
        return results


if __name__ == "__main__":
    async def main() -> None:
        logging.basicConfig(level=logging.INFO)
        extractor = InstagramExtractor()
        urls = [
            "https://www.instagram.com/reel/DNs1OYfUkoI/",
            "https://www.instagram.com/p/DOvZHDHCZP2/",
        ]
        res = await extractor.extract_batch(urls, concurrency=2)
        print(res)

    asyncio.run(main())
