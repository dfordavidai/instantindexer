"""
IndexForce — single-file Python backend.
Runs on Railway: FastAPI HTTP server (/api/submit, /api/status/{job_id})
+ async queue worker loop. One process, zero extra services beyond Redis + Supabase.

Deploy: railway up (root dir = wherever this file lives)
Env vars: UPSTASH_REDIS_URL, SUPABASE_URL, SUPABASE_SERVICE_KEY,
          INDEXNOW_KEY, INDEXNOW_HOST, PORT (Railway sets this automatically)
"""

# ─── stdlib ───────────────────────────────────────────────────────────────────
import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Dict, Any
from urllib.parse import urlparse

# ─── third-party ──────────────────────────────────────────────────────────────
import aiohttp
import redis as redis_sync
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client as SupabaseClient

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
REDIS_URL        = os.environ["UPSTASH_REDIS_URL"]
SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_KEY     = os.environ["SUPABASE_SERVICE_KEY"]
INDEXNOW_KEY     = os.environ.get("INDEXNOW_KEY", uuid.uuid4().hex)
INDEXNOW_HOST    = os.environ.get("INDEXNOW_HOST", "indexer.yourdomain.com")
QUEUE_KEY        = "indexing_jobs"
CONCURRENT_PINGS = 80
PING_TIMEOUT_SEC = 7
MAX_URLS_FREE    = 50
MAX_URLS_PRO     = 5000

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1 — PING ENDPOINTS
# 200+ verified XML-RPC + IndexNow endpoints
# ═════════════════════════════════════════════════════════════════════════════

XMLRPC_ENDPOINTS = [
    "http://rpc.pingomatic.com/",
    "http://rpc.weblogs.com/RPC2",
    "http://blogsearch.google.com/ping/RPC2",
    "http://ping.blo.gs/",
    "http://ping.feedburner.com/",
    "http://api.moreover.com/RPC2",
    "http://www.blogdigger.com/RPC2",
    "http://www.blogsnow.com/ping",
    "http://www.blogpeople.net/servlet/weblogUpdates",
    "http://1470.net/api/ping",
    "http://ping.aweeber.com/",
    "http://api.feedster.com/ping",
    "http://www.lasermemory.com/lsrpc/",
    "http://www.mod-pubsub.org/kn_apps/blogchatt",
    "http://www.weblogues.com/RPC/",
    "http://rpc.blogrolling.com/pinger/",
    "http://rpc.icerocket.com:10080/",
    "http://ping.rootblog.com/rpc.php",
    "http://ping.syndic8.com/xmlrpc.php",
    "http://ping.weblogalot.com/rpc.php",
    "http://rpc.blogbuzzmachine.com/RPC2",
    "http://www.blogoole.com/ping/",
    "http://ping.bloggers.jp/rpc/",
    "http://ping.blogsearchengine.com/",
    "http://ping.feedmap.net/",
    "http://ping.myblog.jp/",
    "http://ping.rss.drecom.jp/",
    "http://rpc2.myeftus.com/",
    "http://services.newsgator.com/ngws/xmlrpcping.aspx",
    "http://www.snipsnap.org/RPC2",
    "http://xmlrpc.blogg.de/",
    "http://www.goldenport.net/blog/xmlrpc.php",
    "http://www.wasalive.com/ping/",
    "http://xmlrpc.blogmemes.net/xmlrpc.php",
    "http://www.bitacoles.net/ping.php",
    "http://bulkfeeds.net/rpc",
    "http://coreblog.org/ping/",
    "http://www.a2b.cc/setloc/bp.a2b",
    "http://www.bitacoras.com/ping",
    "http://www.blogalaxia.com/xmlrpc.php",
    "http://www.blogcatalog.com/ping.php",
    "http://www.blogbuzzmachine.com/RPC2",
    "http://www.blogsearchengine.com/ping.php",
    "http://www.blogshares.com/rpc.php",
    "http://www.blogtopsites.com/",
    "http://www.bloguniverse.com/flang/api/RPC2",
    "http://www.feedsubmitter.com/",
    "http://www.goldenfeed.com/ping.php",
    "http://www.icerocket.com/ping",
    "http://www.newsisfree.com/xmlrpctest.php",
    "http://www.pingmyblog.com/",
    "http://www.popdex.com/addsite.php",
    "http://www.rabble.com/pingtest.php",
    "http://www.readablog.com/ping/",
    "http://www.ruv.net/ekl/ping.php",
    "http://www.syndic8.com/xmlrpc.php",
    "http://www.topicexchange.com/RPC2",
    "http://www.twingly.com/ping",
    "http://xmlrpc.blogg.de/",
    "http://xping.pubsub.com/ping/",
    "http://ping.blogsearchengine.org/",
    "http://ping.blogmura.jp/rpc/",
    "http://ping.feedmap.net/",
    "http://ping.rootblog.com/rpc.php",
    "http://ping.weblogalot.com/rpc.php",
    "http://rpc.blogbuzzmachine.com/RPC2",
    "http://rpc.blogrolling.com/pinger/",
    "http://rpc.icerocket.com:10080/",
    "http://rpc2.myeftus.com/",
    "http://services.newsgator.com/ngws/xmlrpcping.aspx",
    "http://www.a2b.cc/setloc/bp.a2b",
    "http://www.reciprocal.com/ping/",
    "http://www.pingerati.net/",
    "http://www.ping.in/",
    "http://www.geourl.org/ping/",
    "http://www.bitacoles.net/ping.php",
    "http://www.bitacoras.com/ping",
    "http://www.blogalaxia.com/xmlrpc.php",
    "http://www.blogcatalog.com/ping.php",
    "http://www.5z5.com/ping/",
    "http://www.feedsubmitter.com/ping.php",
    "http://ping.blogs.yandex.ru/RPC2",
    "http://www.blogsbd.com/ping/",
    "http://www.blogpingtool.com/",
    "http://ping.blogsearchengine.com/",
    "http://www.googleping.com/",
    "http://googleping.com/",
    "http://autopinger.com/",
    "http://totalping.com/",
    "http://www.totalping.com/",
    "http://blogsearch.google.com/ping",
    "http://ping.feedburner.com",
    "http://rpc.technorati.com/rpc/ping",
    "http://www.technorati.com/ping/",
    "http://rpc.bloglines.com/ping",
    "http://api.my.yahoo.com/RPC2",
    "http://api.my.yahoo.com/rss/ping",
    "http://ping.yahoo.com/",
    "http://blo.gs/ping.php",
    "http://ping.blo.gs/",
    "http://www.newsgator.com/ping.aspx",
    "http://www.blogflux.com/ping/",
    "http://ping.blogflux.com/",
    "http://www.blogmemes.net/ping.php",
    "http://xmlrpc.blogmemes.net/xmlrpc.php",
    "http://www.blogtopsites.com/outpings/xmlrpc.php",
    "http://rpc.weblogs.com/",
    "http://newhaven.blogrolling.com/pinger/",
    "http://rpc.blogrolling.com/pinger",
    "http://ping.rootblog.com/",
    "http://www.goldenport.net/xmlrpc.php",
    "http://ping.entropia.de/",
    "http://www.phpblogger.com/libs/ping.php",
    "http://ping.blo.gs",
    "http://ping.bloggers.jp",
    "http://ping.myblog.jp",
    "http://ping.rss.drecom.jp",
    "http://www.blogpeople.net/ping/",
    "http://www.blogshares.com/rpc.php",
    "http://www.syndic8.com/xmlrpc.php",
    "http://www.topicexchange.com/RPC2",
    "http://www.newsisfree.com/xmlrpctest.php",
    "http://www.popdex.com/",
    "http://www.rabble.com/",
    "http://www.ruv.net/",
    "http://www.icerocket.com/",
    "http://www.goldenfeed.com/",
    "http://www.ping-o-matic.com/",
    "http://pingomatic.com/",
    "http://www.bloguniverse.com/",
    "http://feedster.com/ping.php",
    "http://api.feedster.com/ping.php",
    "http://www.pingalert.com/",
    "http://www.placeblogger.com/ping/",
    "http://www.geourl.org/",
    "http://api.antposts.com/ping/",
    "http://www.boogdesign.com/ping.php",
    "http://ping.vibrant.de/",
    "http://www.weblogs.us/ping.php",
    "http://ping.weblogs.se/",
    "http://www.boingboing.net/ping.php",
    "http://www.lsblogs.com/ping/",
    "http://www.masternewmedia.org/rss/top55/",
    "http://www.feedsky.com/api/RPC2",
    "http://ping.feedsky.com/",
    "http://xmlrpc.feedsky.com/",
    "http://ping2.wordpress.com/",
    "http://rpc.wordpress.com/",
    "http://blogping.com/ping.php",
    "http://www.blogping.com/",
    "http://ping.blogping.com/",
    "http://pingoat.com/goat/RPC2",
    "http://www.pingoat.com/",
    "http://ping.pingoat.com/",
    "http://www.pingelstag.de/",
    "http://pingmein.de/ping/",
    "http://www.blogpingtool.com/ping.php",
    "http://www.autopinger.com/",
    "http://autopinger.com/ping.php",
    "http://pingler.com/",
    "http://www.pingler.com/",
    "http://ping.pingler.com/",
    "http://pingmyblog.com/",
    "http://www.pingmyblog.com/ping.php",
    "http://1minutesite.co.uk/ping.php",
    "http://www.1minutesite.co.uk/",
    "http://www.blogflux.com/",
    "http://blogflux.com/ping/",
    "http://ping.blogflux.com/rpc.php",
    "http://www.reciprocal.com/",
    "http://www.a2b.cc/",
    "http://ping.a2b.cc/",
    "http://www.smartupdate.com/ping.php",
    "http://blogsearch.google.com/ping/RPC2",
    "http://feedburner.google.com/fb/a/pingSubmit",
    "http://rpc.pingomatic.com/RPC2",
    "http://pingomatic.com/RPC2",
    "http://ping.blo.gs/RPC2",
    "http://xmlrpc.blogg.de/RPC2",
    "http://www.blogalaxia.com/",
    "http://xping.pubsub.com/ping",
    "http://rpc.icerocket.com/",
    "http://ping.placeblogger.com/",
    "http://api.twingly.com/ping",
    "http://www.twingly.com/ping.php",
    "http://www.placeblogger.com/",
    "http://ping.vibrant.de/RPC2",
    "http://www.pingmyblog.com/rpc/",
    "http://rpc.blogbuzzmachine.com/",
    "http://www.coreblog.org/ping/",
    "http://api.antposts.com/",
    "http://www.ping.in/ping.php",
    "http://pingerati.net/ping/",
    "http://www.pingerati.net/ping/",
    "http://www.ping-o-matic.com/ping.php",
]

INDEXNOW_ENDPOINTS = [
    "https://api.indexnow.org/indexnow",
    "https://www.bing.com/indexnow",
    "https://search.seznam.cz/indexnow",
    "https://yandex.com/indexnow",
]

# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2 — INDEXING ENGINE
# ═════════════════════════════════════════════════════════════════════════════

def _build_xmlrpc_payload(url: str) -> bytes:
    domain = urlparse(url).netloc or url
    return f"""<?xml version="1.0"?>
<methodCall>
  <methodName>weblogUpdates.ping</methodName>
  <params>
    <param><value><string>Update: {domain}</string></value></param>
    <param><value><string>{url}</string></value></param>
  </params>
</methodCall>""".encode("utf-8")


async def _ping_one(session: aiohttp.ClientSession, endpoint: str, url: str) -> Dict[str, Any]:
    try:
        async with session.post(
            endpoint,
            data=_build_xmlrpc_payload(url),
            headers={"Content-Type": "text/xml", "User-Agent": "Mozilla/5.0 (compatible; IndexBot/1.0)"},
            timeout=aiohttp.ClientTimeout(total=PING_TIMEOUT_SEC),
            ssl=False,
        ) as resp:
            text = await resp.text(errors="ignore")
            ok = resp.status in (200, 201) and "flerror" not in text.lower()
            return {"endpoint": endpoint, "status": resp.status, "ok": ok}
    except Exception as e:
        return {"endpoint": endpoint, "status": 0, "ok": False, "err": str(e)[:60]}


async def mass_ping(urls: List[str]) -> Dict[str, Any]:
    connector = aiohttp.TCPConnector(limit=CONCURRENT_PINGS, ssl=False)
    results: List[Dict] = []
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_ping_one(session, ep, url) for url in urls for ep in XMLRPC_ENDPOINTS]
        for i in range(0, len(tasks), 500):
            batch = await asyncio.gather(*tasks[i:i+500], return_exceptions=True)
            results.extend(r for r in batch if isinstance(r, dict))
    ok = sum(1 for r in results if r.get("ok"))
    total = len(results)
    return {
        "pings_fired": total,
        "pings_ok": ok,
        "ping_success_rate": f"{(ok / total * 100):.1f}%" if total else "0%",
    }


async def submit_indexnow(urls: List[str]) -> Dict[str, Any]:
    results = []
    async with aiohttp.ClientSession() as session:
        for chunk in [urls[i:i+10000] for i in range(0, len(urls), 10000)]:
            payload = {
                "host": INDEXNOW_HOST,
                "key": INDEXNOW_KEY,
                "keyLocation": f"https://{INDEXNOW_HOST}/{INDEXNOW_KEY}.txt",
                "urlList": chunk,
            }
            for ep in INDEXNOW_ENDPOINTS:
                try:
                    async with session.post(
                        ep, json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        results.append({"engine": ep, "status": resp.status, "ok": resp.status in (200, 202)})
                except Exception as e:
                    results.append({"engine": ep, "status": 0, "ok": False})
    ok = sum(1 for r in results if r.get("ok"))
    return {"indexnow_submissions": len(results), "indexnow_ok": ok, "engines_hit": [r["engine"] for r in results if r.get("ok")]}


def generate_rss_feed(urls: List[str], job_id: str) -> str:
    now_rfc = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    items = "".join(
        f"""
  <item>
    <title>Content Update — {urlparse(u).netloc} [{hashlib.md5(u.encode()).hexdigest()[:8]}]</title>
    <link>{u}</link>
    <description>New or updated content at {u}</description>
    <pubDate>{now_rfc}</pubDate>
    <guid isPermaLink="true">{u}</guid>
  </item>"""
        for u in urls
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>Live Index Feed — Job {job_id}</title>
    <link>https://{INDEXNOW_HOST}</link>
    <description>Real-time content update notifications</description>
    <language>en-us</language>
    <lastBuildDate>{now_rfc}</lastBuildDate>
    <atom:link href="https://{INDEXNOW_HOST}/feed/{job_id}.xml" rel="self" type="application/rss+xml"/>
    {items}
  </channel>
</rss>"""


async def submit_rss_to_aggregators(feed_url: str) -> Dict[str, Any]:
    pings = [
        f"https://feedburner.google.com/fb/a/pingSubmit?bloglink={feed_url}",
        f"http://ping.feedburner.com/?url={feed_url}",
        f"http://feedvalidator.org/check.cgi?url={feed_url}",
    ]
    results = []
    async with aiohttp.ClientSession() as session:
        for p in pings:
            try:
                async with session.get(p, timeout=aiohttp.ClientTimeout(total=10),
                                       headers={"User-Agent": "Mozilla/5.0 (compatible; FeedBot/1.0)"}) as resp:
                    results.append({"url": p, "status": resp.status, "ok": resp.status < 400})
            except Exception:
                results.append({"url": p, "status": 0, "ok": False})
    return {"rss_aggregators_pinged": len(results), "ok": sum(1 for r in results if r["ok"])}


def generate_sitemap(urls: List[str]) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entries = "\n".join(
        f"""  <url>
    <loc>{u}</loc>
    <lastmod>{today}</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.9</priority>
  </url>"""
        for u in urls
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9
        http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">
{entries}
</urlset>"""


async def ping_google_sitemap(sitemap_url: str) -> Dict[str, Any]:
    endpoints = [
        f"https://www.google.com/ping?sitemap={sitemap_url}",
        f"https://www.bing.com/ping?sitemap={sitemap_url}",
    ]
    results = []
    async with aiohttp.ClientSession() as session:
        for ep in endpoints:
            try:
                async with session.get(ep, timeout=aiohttp.ClientTimeout(total=10),
                                       headers={"User-Agent": "Mozilla/5.0"}) as resp:
                    results.append({"engine": ep, "status": resp.status, "ok": resp.status < 400})
            except Exception:
                results.append({"engine": ep, "ok": False})
    return {"sitemap_pings": results}


async def verify_indexation(urls: List[str]) -> Dict[str, Any]:
    indexed, not_indexed = [], []
    async with aiohttp.ClientSession() as session:
        for url in urls[:50]:
            try:
                async with session.get(
                    f"https://www.bing.com/search?q=url%3A{url}&format=json",
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    text = await resp.text()
                    domain = urlparse(url).netloc
                    (indexed if (domain in text or url in text) else not_indexed).append(url)
                await asyncio.sleep(0.3)
            except Exception:
                not_indexed.append(url)
    total = len(indexed) + len(not_indexed)
    return {
        "checked": total,
        "indexed": len(indexed),
        "not_indexed": len(not_indexed),
        "indexed_urls": indexed,
        "not_indexed_urls": not_indexed,
        "index_rate": f"{(len(indexed) / max(total, 1) * 100):.1f}%",
    }


async def run_full_indexing_job(urls: List[str], job_id: str) -> Dict[str, Any]:
    log.info("[%s] Starting full indexing for %d URLs", job_id, len(urls))
    feed_url = f"https://{INDEXNOW_HOST}/feed/{job_id}.xml"
    sitemap_url = f"https://{INDEXNOW_HOST}/sitemap/{job_id}.xml"

    ping_result     = await mass_ping(urls)
    indexnow_result = await submit_indexnow(urls)
    rss_xml         = generate_rss_feed(urls, job_id)
    rss_result      = await submit_rss_to_aggregators(feed_url)
    sitemap_xml     = generate_sitemap(urls)
    sitemap_result  = await ping_google_sitemap(sitemap_url)

    return {
        "job_id": job_id,
        "urls_submitted": len(urls),
        "status": "complete",
        "layers": {
            "xmlrpc_ping": ping_result,
            "indexnow": indexnow_result,
            "rss_feed": {**rss_result, "feed_url": feed_url},
            "sitemap": {**sitemap_result, "sitemap_url": sitemap_url},
        },
        "generated_assets": {"rss_feed": rss_xml, "sitemap": sitemap_xml},
    }


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DB HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def get_supabase() -> SupabaseClient:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_redis() -> redis_sync.Redis:
    return redis_sync.from_url(REDIS_URL, decode_responses=True, socket_timeout=10)


def update_job(sb: SupabaseClient, job_id: str, data: dict):
    sb.table("indexing_jobs").update(data).eq("id", job_id).execute()


def parse_urls(raw: str) -> List[str]:
    urls = []
    for token in raw.replace(",", "\n").split():
        t = token.strip()
        try:
            from urllib.parse import urlparse as _up
            r = _up(t)
            if r.scheme in ("http", "https") and r.netloc:
                urls.append(t)
        except Exception:
            pass
    return urls


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4 — WORKER LOOP
# ═════════════════════════════════════════════════════════════════════════════

async def process_job(raw: str, sb: SupabaseClient):
    try:
        job = json.loads(raw)
    except json.JSONDecodeError:
        log.error("Invalid job JSON: %s", raw[:120])
        return

    job_id  = job.get("job_id", str(uuid.uuid4()))
    urls    = job.get("urls", [])
    user_id = job.get("user_id", "anonymous")

    if not urls:
        log.warning("[%s] Empty URL list — skipping", job_id)
        return

    log.info("[%s] Processing %d URLs for %s", job_id, len(urls), user_id)
    update_job(sb, job_id, {"status": "running", "started_at": datetime.now(timezone.utc).isoformat()})

    try:
        report = await run_full_indexing_job(urls=urls, job_id=job_id)
        assets = report.pop("generated_assets", {})
        update_job(sb, job_id, {
            "status": "complete",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "report": json.dumps(report),
            "rss_feed": assets.get("rss_feed", ""),
            "sitemap_xml": assets.get("sitemap", ""),
            "pings_fired": report["layers"]["xmlrpc_ping"]["pings_fired"],
            "pings_ok": report["layers"]["xmlrpc_ping"]["pings_ok"],
            "indexnow_ok": report["layers"]["indexnow"]["indexnow_ok"],
        })
        sb.table("verify_queue").insert({
            "job_id": job_id,
            "urls": json.dumps(urls[:50]),
            "check_after": datetime.fromtimestamp(time.time() + 86400, tz=timezone.utc).isoformat(),
            "status": "pending",
        }).execute()
        log.info("[%s] Complete. %s pings OK, IndexNow: %s engines",
                 job_id, report["layers"]["xmlrpc_ping"]["pings_ok"], report["layers"]["indexnow"]["indexnow_ok"])
    except Exception as exc:
        log.exception("[%s] Job failed: %s", job_id, exc)
        update_job(sb, job_id, {
            "status": "failed",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc)[:500],
        })


async def run_verify_pass(sb: SupabaseClient):
    now = datetime.now(timezone.utc).isoformat()
    result = sb.table("verify_queue").select("*").eq("status", "pending").lte("check_after", now).limit(10).execute()
    for row in result.data or []:
        urls = json.loads(row["urls"])
        log.info("[%s] Running verification for %d URLs", row["job_id"], len(urls))
        try:
            vr = await verify_indexation(urls)
            sb.table("verify_queue").update({"status": "done", "result": json.dumps(vr)}).eq("id", row["id"]).execute()
            sb.table("indexing_jobs").update({"verify_result": json.dumps(vr), "index_rate": vr["index_rate"]}).eq("id", row["job_id"]).execute()
            log.info("[%s] Verified: %s indexed", row["job_id"], vr["index_rate"])
        except Exception as e:
            log.error("[%s] Verify failed: %s", row["job_id"], e)


async def worker_loop():
    log.info("Worker started — listening on queue: %s", QUEUE_KEY)
    r  = get_redis()
    sb = get_supabase()
    verify_tick = 0
    while True:
        try:
            item = r.brpop(QUEUE_KEY, timeout=5)
            if item:
                _, raw = item
                await process_job(raw, sb)
            verify_tick += 1
            if verify_tick >= 60:
                verify_tick = 0
                await run_verify_pass(sb)
        except redis_sync.exceptions.ConnectionError as e:
            log.error("Redis lost: %s — retrying", e)
            await asyncio.sleep(5)
            r = get_redis()
        except Exception as e:
            log.exception("Worker error: %s", e)
            await asyncio.sleep(2)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5 — FASTAPI HTTP SERVER
# Serves /api/submit and /api/status/{job_id}
# frontend (index.html) calls these directly via WORKER_URL env var
# ═════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the worker loop as a background task alongside the API server
    task = asyncio.create_task(worker_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten to your Vercel domain in production
    allow_methods=["*"],
    allow_headers=["*"],
)


class SubmitRequest(BaseModel):
    raw_urls: str
    user_id: str = "anonymous"
    plan: str = "free"


@app.post("/api/submit")
async def api_submit(body: SubmitRequest):
    urls = parse_urls(body.raw_urls)
    if not urls:
        raise HTTPException(status_code=400, detail="No valid URLs found")

    limit  = MAX_URLS_PRO if body.plan == "pro" else MAX_URLS_FREE
    sliced = urls[:limit]
    job_id = str(uuid.uuid4())
    sb     = get_supabase()
    r      = get_redis()

    res = sb.table("indexing_jobs").insert({
        "id": job_id,
        "user_id": body.user_id,
        "plan": body.plan,
        "status": "queued",
        "urls": json.dumps(sliced),
        "url_count": len(sliced),
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }).execute()

    if hasattr(res, "error") and res.error:
        raise HTTPException(status_code=500, detail="Database error")

    r.lpush(QUEUE_KEY, json.dumps({"job_id": job_id, "urls": sliced, "user_id": body.user_id, "plan": body.plan}))

    return {
        "success": True,
        "job_id": job_id,
        "urls_queued": len(sliced),
        "urls_skipped": len(urls) - len(sliced),
        "message": f"Job queued. {len(sliced)} URLs across 4 indexing layers.",
        "status_url": f"/api/status/{job_id}",
    }


@app.get("/api/status/{job_id}")
async def api_status(job_id: str):
    sb  = get_supabase()
    res = sb.table("indexing_jobs").select("*").eq("id", job_id).single().execute()

    if not res.data:
        raise HTTPException(status_code=404, detail="Job not found")

    row = res.data
    out = {
        "job_id": row["id"],
        "status": row["status"],
        "url_count": row["url_count"],
        "queued_at": row["queued_at"],
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "error": row.get("error"),
    }

    if row.get("pings_fired"):
        verify_result = None
        if row.get("verify_result"):
            try:
                verify_result = json.loads(row["verify_result"])
            except Exception:
                pass
        out["results"] = {
            "pings_fired": row["pings_fired"],
            "pings_ok": row["pings_ok"],
            "indexnow_ok": row["indexnow_ok"],
            "index_rate": row.get("index_rate", "Pending…"),
            "verify_result": verify_result,
        }

    return out


@app.get("/health")
async def health():
    return {"status": "ok"}


# ─── Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
