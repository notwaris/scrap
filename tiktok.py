import asyncio
import logging
import random
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
from options.config import PROXY_BASE

logger = logging.getLogger("extractor.tiktok")

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
API_URL = "https://www.tikwm.com/api/"


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


class TiktokExtractor:
    def __init__(self, proxy_base: str = PROXY_BASE, pool_size: int = 10) -> None:
        self.proxy_mgr = ProxyManager(proxy_base, pool_size)
        self.api_url = API_URL
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
        if parsed_url.hostname in ["www.tiktok.com", "tiktok.com", "m.tiktok.com"]:
            parts = parsed_url.path.split("/")
            if "video" in parts:
                i = parts.index("video")
                if i + 1 < len(parts):
                    return parts[i + 1]
            elif "/v/" in parsed_url.path:
                return parsed_url.path.split("/v/")[-1].replace(".html", "")
        elif parsed_url.hostname in ["vm.tiktok.com", "vt.tiktok.com"]:
            return parsed_url.path[1:] if parsed_url.path else None
        return None

    async def _fetch_from_api(self, url: str, proxy: str, ua: str) -> Optional[Dict[str, Any]]:
        headers = {"User-Agent": ua}
        async with httpx.AsyncClient(proxy=proxy, headers=headers, timeout=30.0) as client:
            r = await client.post(self.api_url, data={"url": url, "hd": 1})
            if r.status_code != 200:
                return None
            data = r.json()
            if data.get("code") == 0:
                return data.get("data", {})
            return None

    async def extract_data(self, url: str) -> Dict[str, Any]:
        video_id = self._extract_video_id(url)
        for attempt in range(self.max_retries):
            await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
            proxy = self.proxy_mgr.get()
            ua = random.choice(USER_AGENTS)
            try:
                data = await self._fetch_from_api(url, proxy, ua)
                if not data:
                    self.proxy_mgr.mark_bad(proxy)
                    await asyncio.sleep(random.choice(DEFAULT_BACKOFF_SECONDS))
                    continue
                if not video_id:
                    video_id = data.get("id", data.get("video_id", "N/A"))
                return {
                    "success": True,
                    "platform": "tiktok",
                    "url": url,
                    "code": video_id,
                    "data": {
                        "id": video_id,
                        "title": data.get("title", "N/A"),
                        "description": data.get("title", "No description"),
                        "view_count": data.get("play_count", 0),
                        "like_count": data.get("digg_count", 0),
                        "comment_count": data.get("comment_count", 0),
                        "share_count": data.get("share_count", 0),
                        "download_count": data.get("download_count", 0),
                        "owner": {
                            "username": data.get("author", {}).get("unique_id", "N/A"),
                            "user_id": data.get("author", {}).get("id", "N/A"),
                            "display_name": data.get("author", {}).get("nickname", "N/A"),
                            "avatar": data.get("author", {}).get("avatar", None),
                            "profile_url": f"https://www.tiktok.com/@{data.get('author', {}).get('unique_id', '')}",
                        },
                        "media_type": "video",
                        "duration": data.get("duration", 0),
                        "upload_date": data.get("create_time", "N/A"),
                        "timestamp": data.get("create_time", None),
                        "thumbnail": data.get("cover", data.get("origin_cover", None)),
                        "video_url": data.get("play", None),
                        "music": {
                            "title": data.get("music_info", {}).get("title", None),
                            "author": data.get("music_info", {}).get("author", None),
                            "url": data.get("music", None),
                        },
                        "webpage_url": url,
                        "extractor": "tikwm_api",
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
        extractor = TiktokExtractor()
        urls = [
            "https://www.tiktok.com/@million_sub/video/7552433170243751189",
            "https://www.tiktok.com/@user/video/7253349017220402434",
        ]
        res = await extractor.extract_batch(urls, concurrency=2)
        print(res)

    asyncio.run(main())
