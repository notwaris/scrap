import asyncio
import logging
import random
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

import yt_dlp
from options.config import PROXY_BASE

logger = logging.getLogger("extractor.youtube")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

DEFAULT_BACKOFF_SECONDS = [60, 90, 120]
DEFAULT_MIN_DELAY = 2.5
DEFAULT_MAX_DELAY = 7.5
DEFAULT_MAX_RETRIES = 3
DEFAULT_STICKY_SECONDS = 300


class ProxyManager:
    def __init__(self, base: str = PROXY_BASE, pool_size: int = 10) -> None:
        self.base = base
        self.pool: List[str] = []
        self.bad: Dict[str, int] = {}
        self.quarantine: Dict[str, datetime] = {}
        self.sticky_map: Dict[str, Dict[str, Any]] = {}
        for _ in range(pool_size):
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


class YoutubeExtractor:
    def __init__(self, proxy_base: str = PROXY_BASE, pool_size: int = 10) -> None:
        self.proxy_mgr = ProxyManager(proxy_base, pool_size)
        self.min_delay = DEFAULT_MIN_DELAY
        self.max_delay = DEFAULT_MAX_DELAY
        self.max_retries = DEFAULT_MAX_RETRIES
        self._setup_logger()

    def _setup_logger(self) -> None:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    def _extract_video_id(self, url: str) -> Optional[str]:
        parsed_url = urlparse(url)
        host = parsed_url.hostname or ""
        path = parsed_url.path
        if "youtube.com" in host or "m.youtube.com" in host:
            if "/watch" in path:
                query = parse_qs(parsed_url.query)
                return query.get("v", [None])[0]
            if "/shorts/" in path:
                return path.split("/shorts/")[-1].split("/")[0]
            if "/embed/" in path:
                return path.split("/embed/")[-1].split("/")[0]
            if "/live/" in path:
                return path.split("/live/")[-1].split("/")[0]
        if "youtu.be" in host:
            return path.strip("/")
        return None

    def _extract_hashtags(self, text: Optional[str]) -> List[str]:
        if not text:
            return []
        return [tag.strip("#") for tag in text.split() if tag.startswith("#")]

    async def _extract_info(self, url: str, proxy: str, ua: str) -> Optional[Dict[str, Any]]:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
            "ignoreerrors": True,
            "skip_download": True,
            "proxy": proxy,
            "http_headers": {"User-Agent": ua},
            "cookiefile": "assets/cookies.txt",
            "extractor_args": {
                "youtube": {
                    "player_client": ["android", "ios", "web"],
                    "skip": ["hls", "dash"]
                }
            },
            "extractor_retries": 3,
            "fragment_retries": 3,
        }
        loop = asyncio.get_event_loop()
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = await loop.run_in_executor(None, ydl.extract_info, url, False)
                if info:
                    logger.info(f"yt-dlp available fields: {list(info.keys())[:20]}")
                    logger.info(f"yt-dlp author fields: uploader={info.get('uploader')}, uploader_id={info.get('uploader_id')}, "
                               f"channel={info.get('channel')}, channel_id={info.get('channel_id')}")
                    logger.info(f"yt-dlp engagement: like_count={info.get('like_count')}, "
                               f"channel_follower_count={info.get('channel_follower_count')}")
                return info
            except Exception as e:
                logger.error(f"Failed to extract info: {e}")
                return None

    async def extract_data(self, url: str) -> Dict[str, Any]:
        video_id = self._extract_video_id(url)
        if not video_id:
            return {"success": False, "error": "Invalid YouTube URL format"}
        for attempt in range(self.max_retries):
            await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
            proxy = self.proxy_mgr.get()
            ua = random.choice(USER_AGENTS)
            try:
                info = await self._extract_info(url, proxy, ua)
                if not info:
                    self.proxy_mgr.mark_bad(proxy)
                    await asyncio.sleep(random.choice(DEFAULT_BACKOFF_SECONDS))
                    continue
                title = info.get("title", "")
                description = info.get("description", "")
                combined_text = f"{title} {description}"
                hashtags = self._extract_hashtags(combined_text)
                caption = description if description else title
                desc_field = description if description else title if title else "No description"
                
                uploader = (
                    info.get("uploader") or 
                    info.get("channel") or 
                    info.get("uploader_id") or 
                    info.get("channel_id") or 
                    "Unknown"
                )
                
                like_count = (
                    info.get("like_count") or 
                    info.get("likes") or 
                    0
                )
                
                subscriber_count = (
                    info.get("channel_follower_count") or 
                    info.get("subscriber_count") or 
                    0
                )
                
                return {
                    "success": True,
                    "platform": "youtube",
                    "url": url,
                    "code": video_id,
                    "data": {
                        "id": info.get("id", "N/A"),
                        "title": title or "N/A",
                        "description": desc_field,
                        "caption": caption or "No caption",
                        "hashtags": hashtags,
                        "view_count": info.get("view_count", 0),
                        "like_count": like_count,
                        "comment_count": info.get("comment_count", 0),
                        "average_rating": info.get("average_rating", 0.0),
                        "owner": {
                            "username": uploader,
                            "channel_id": info.get("channel_id", "N/A"),
                            "channel_url": info.get("channel_url", "N/A"),
                            "subscriber_count": subscriber_count,
                        },
                        "media_type": "video",
                        "duration": info.get("duration", 0),
                        "upload_date": info.get("upload_date", "N/A"),
                        "categories": info.get("categories", []),
                        "tags": info.get("tags", []),
                        "is_live": info.get("is_live", False),
                        "thumbnail": info.get("thumbnail", None),
                        "age_restricted": info.get("age_limit", 0) > 0,
                    },
                }
            except Exception as e:
                self.proxy_mgr.mark_bad(proxy)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(random.choice(DEFAULT_BACKOFF_SECONDS))
                    continue
                return {"success": False, "error": str(e)}
        return {"success": False, "error": "Max retries exceeded"}

    async def extract_batch(self, urls: List[str], concurrency: int = 3) -> List[Dict[str, Any]]:
        sem = asyncio.Semaphore(concurrency)
        results: List[Dict[str, Any]] = []

        async def _worker(u: str) -> None:
            async with sem:
                r = await self.extract_data(u)
                results.append(r)

        tasks = [_worker(u) for u in urls]
        await asyncio.gather(*tasks)
        return results


if __name__ == "__main__":
    async def main() -> None:
        logging.basicConfig(level=logging.INFO)
        extractor = YoutubeExtractor()
        urls = [
            "https://www.youtube.com/watch?v=geKdXi1icoA",
            "https://youtu.be/dQw4w9WgXcQ",
        ]
        res = await extractor.extract_batch(urls, concurrency=2)
        print(res)

    asyncio.run(main())
                                                
