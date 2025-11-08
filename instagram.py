import asyncio
import logging
import random
import uuid
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Dict, List, Optional
from pathlib import Path
from options.config import PROXY_BASE
from urllib.parse import urlparse
import re
from instagrapi import Client
from instagrapi.exceptions import ClientError, LoginRequired, RateLimitError, ClientNotFoundError

logger = logging.getLogger("extractor.instagram")

COOKIES_FILE = Path(__file__).parent.parent / "assets" / "cookies.txt"

DEFAULT_MIN_DELAY = 3.0
DEFAULT_MAX_DELAY = 6.0
DEFAULT_STICKY_SECONDS = 1800
DEFAULT_BACKOFF_SECONDS = [90, 120]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]


class ProxyManager:
    def __init__(self, base: str = PROXY_BASE, pool_size: int = 3) -> None:
        self.base = base
        self.pool: List[str] = [self._build_proxy() for _ in range(pool_size)]
        self.bad: Dict[str, int] = {}
        self.quarantine: Dict[str, datetime] = {}
        self.sticky_map: Dict[str, Dict[str, Any]] = {}
        self.last_used: Dict[str, datetime] = {}

    def _build_proxy(self, session: Optional[str] = None) -> str:
        if not session:
            session = uuid.uuid4().hex[:8]
        return f"{self.base}?session={session}"

    def get(self, sticky_key: Optional[str] = None) -> str:
        now = datetime.utcnow()
        if sticky_key and sticky_key in self.sticky_map:
            entry = self.sticky_map[sticky_key]
            if entry["expires_at"] > now and entry["proxy"] not in self.quarantine:
                return entry["proxy"]
        available = [
            p
            for p in self.pool
            if p not in self.quarantine or self.quarantine[p] <= now
        ]
        if not available:
            p = self._build_proxy()
        else:
            p = min(
                available,
                key=lambda x: self.last_used.get(x, datetime.min),
            )
        self.last_used[p] = now
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


class InstagramExtractor:
    def __init__(self, proxy_base: str = PROXY_BASE, pool_size: int = 3) -> None:
        self.proxy_mgr = ProxyManager(proxy_base, pool_size)
        self.min_delay = DEFAULT_MIN_DELAY
        self.max_delay = DEFAULT_MAX_DELAY
        self._client = None
        self._setup_logger()

    def _setup_logger(self) -> None:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    def _extract_shortcode(self, url: str) -> Optional[str]:
        path = urlparse(url).path
        matches = re.search(r"/p/([^/]+)|/reel/([^/]+)|/tv/([^/]+)", path)
        if matches:
            return matches.group(1) or matches.group(2) or matches.group(3)
        return None

    def _get_client(self) -> Client:
        if self._client:
            return self._client
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
                if "instagram.com" in cookie.domain and cookie.name == "sessionid":
                    sessionid = cookie.value
                    break
            if sessionid:
                cl.set_settings({"cookies": {"sessionid": sessionid}})
                cl.login_by_sessionid(sessionid)
        self._client = cl
        return cl

    async def extract_data(self, url: str) -> Dict[str, Any]:
        shortcode = self._extract_shortcode(url)
        if not shortcode:
            return {"success": False, "error": "Invalid Instagram URL format"}
        proxy = self.proxy_mgr.get(sticky_key=shortcode)
        cl = self._get_client()
        cl.set_proxy(proxy)
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
        try:
            loop = asyncio.get_event_loop()
            media_pk = await loop.run_in_executor(None, cl.media_pk_from_code, shortcode)

            def get_media():
                response = cl.private_request(f"media/{media_pk}/info/")
                return response["items"][0] if response and "items" in response else None

            media_dict = await loop.run_in_executor(None, get_media)
            if not media_dict:
                return {"success": False, "error": "Failed to fetch media"}

            user_dict = media_dict.get("user", {})
            caption_dict = media_dict.get("caption", {})
            caption_text = caption_dict.get("text", "") if caption_dict else ""
            like_count = media_dict.get("like_count", 0)
            comment_count = media_dict.get("comment_count", 0)
            play_count = media_dict.get("play_count") or media_dict.get("ig_play_count", 0)

            return {
                "success": True,
                "platform": "instagram",
                "url": f"https://www.instagram.com/p/{shortcode}/",
                "code": shortcode,
                "data": {
                    "id": str(media_dict.get("pk", "N/A")),
                    "caption": caption_text or "No caption",
                    "hashtags": [f"#{tag.lstrip('#')}" for tag in caption_text.split() if tag.startswith("#")],
                    "mentions": [tag for tag in caption_text.split() if tag.startswith("@")],
                    "like_count": like_count,
                    "comment_count": comment_count,
                    "view_count": play_count,
                    "owner": {
                        "username": user_dict.get("username", "N/A"),
                        "full_name": user_dict.get("full_name", "N/A"),
                        "is_verified": user_dict.get("is_verified", False),
                    },
                    "media_type": "video" if media_dict.get("media_type") == 2 else "photo",
                    "is_video": media_dict.get("media_type") == 2,
                    "taken_at": datetime.fromtimestamp(media_dict.get("taken_at", 0)).isoformat() if media_dict.get("taken_at") else "N/A",
                    "location": None,
                    "url": media_dict.get("image_versions2", {}).get("candidates", [{}])[0].get("url", f"https://www.instagram.com/p/{shortcode}/"),
                },
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def extract_batch(self, urls: List[str], concurrency: int = 2) -> List[Dict[str, Any]]:
        sem = asyncio.Semaphore(concurrency)
        results: List[Dict[str, Any]] = []

        async def _worker(u: str) -> None:
            async with sem:
                r = await self.extract_data(u)
                results.append(r)

        await asyncio.gather(*[_worker(u) for u in urls])
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
