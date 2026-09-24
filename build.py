#!/usr/bin/env python3
"""
Generate videos.json for the WACS Archive viewer.

- Loads video IDs from the yt-dlp archive record plus any already in videos.json
- Fetches fresh YouTube metadata (title, date, thumbnail, duration) for every ID
  each run, so title/date corrections made on YouTube show up on the site.
  Videos YouTube no longer returns (deleted/private) keep their cached metadata.
- Gets SharePoint file IDs via rclone lsjson (one call)
- Creates anonymous sharing links via Graph API $batch (20 at a time)
- Matches YouTube videos to SharePoint files by normalized title AND upload
  date (closest date wins; each file is used by at most one video)
- De-duplicates videos by YouTube ID
- Outputs videos.json sorted newest-first

The YouTube API key is read from $WACS_YT_API_KEY or
~/scripts/.wacs_youtube_api_key (never commit the key to this repo).
"""

import json
import os
import re
import subprocess
import time
import unicodedata
import urllib.request
import urllib.error
from datetime import date
from pathlib import Path

ARCHIVE_FILE = Path("/Users/colindabkowski/scripts/.yt-dlp-archive.txt")
API_KEY_FILE = Path("/Users/colindabkowski/scripts/.wacs_youtube_api_key")
OUTPUT = Path(__file__).parent / "videos.json"
LINKS_CACHE = Path(__file__).parent / "links_cache.json"
RCLONE_CONF = Path.home() / ".config/rclone/rclone.conf"
GRAPH_BATCH = "https://graph.microsoft.com/v1.0/$batch"
MAX_DATE_GAP_DAYS = 400   # beyond this, a same-title file is treated as a different video


# ── helpers ──────────────────────────────────────────────────────────────────

def get_api_key() -> str:
    key = os.environ.get("WACS_YT_API_KEY", "").strip()
    if not key and API_KEY_FILE.exists():
        key = API_KEY_FILE.read_text().strip()
    if not key:
        raise RuntimeError(f"No YouTube API key: set WACS_YT_API_KEY or create {API_KEY_FILE}")
    return key


def normalize(s: str) -> str:
    import html
    s = html.unescape(s)
    s = unicodedata.normalize("NFKC", s)
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_duration(iso: str) -> str:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso)
    if not m:
        return ""
    h, mi, s = (int(x or 0) for x in m.groups())
    if h:
        return f"{h}:{mi:02d}:{s:02d}"
    return f"{mi}:{s:02d}"


def api_get(url: str, token: str = None) -> dict:
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def api_post(url: str, body: dict, token: str) -> dict:
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


# ── step 1: load video IDs ────────────────────────────────────────────────────

def load_video_ids() -> list[str]:
    ids = []
    for line in ARCHIVE_FILE.read_text().splitlines():
        parts = line.strip().split()
        if len(parts) == 2 and parts[0] == "youtube":
            ids.append(parts[1])
    return list(dict.fromkeys(ids))  # drop repeats, keep order


# ── step 2: fetch YouTube metadata ───────────────────────────────────────────

def fetch_youtube_metadata(video_ids: list[str]) -> list[dict]:
    key = get_api_key()
    videos = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        url = (
            "https://www.googleapis.com/youtube/v3/videos"
            f"?part=snippet,contentDetails&id={','.join(batch)}&key={key}"
        )
        data = api_get(url)
        for item in data.get("items", []):
            snippet = item["snippet"]
            thumb = snippet.get("thumbnails", {})
            thumb_url = (
                thumb.get("maxres", {}).get("url")
                or thumb.get("high", {}).get("url")
                or thumb.get("medium", {}).get("url", "")
            )
            videos.append({
                "id": item["id"],
                "title": snippet.get("title", ""),
                "date": snippet.get("publishedAt", "")[:10],
                "thumbnail": thumb_url,
                "description": snippet.get("description", "")[:400].strip(),
                "duration": parse_duration(item["contentDetails"].get("duration", "")),
            })
        print(f"  YouTube API: {min(i+50, len(video_ids))}/{len(video_ids)}", flush=True)
    return videos


def refresh_metadata(ids: list[str], cached: list[dict]) -> list[dict]:
    """Fresh metadata for every ID; cached entry kept for IDs YouTube no longer returns.
    Earlier titles are remembered in 'alt_titles' so files named under an old
    title still match after a rename on YouTube."""
    cache = {}
    for v in cached:
        cache.setdefault(v["id"], v)  # first copy wins if the cache has repeats
    all_ids = list(dict.fromkeys(ids + list(cache)))
    fresh = {v["id"]: v for v in fetch_youtube_metadata(all_ids)}
    out = []
    for vid in all_ids:
        old = cache.get(vid)
        new = fresh.get(vid)
        if new is None:
            if old is None:
                continue
            out.append(dict(old))
            continue
        alts = list((old or {}).get("alt_titles", []))
        if old and old.get("title") and old["title"] != new["title"] and old["title"] not in alts:
            alts.append(old["title"])
        if alts:
            new["alt_titles"] = alts
        out.append(new)
    gone = len([i for i in all_ids if i not in fresh and i in cache])
    print(f"     {len(fresh)} refreshed from YouTube, {gone} kept from cache (not returned by API)", flush=True)
    return out


# ── step 3: list SharePoint files + read token from (now-refreshed) config ───

def read_rclone_config() -> tuple[str, str]:
    """Return (access_token, drive_id) from rclone config in one read.
    Call after rclone lsjson so the token is guaranteed fresh."""
    conf = RCLONE_CONF.read_text()
    m = re.search(r'\[onedrive\].*?token\s*=\s*(\{.*?\})', conf, re.DOTALL)
    if not m:
        raise RuntimeError("Could not find onedrive token in rclone config")
    token = json.loads(m.group(1))["access_token"]
    dm = re.search(r'drive_id\s*=\s*(\S+)', conf)
    drive_id = dm.group(1) if dm else ""
    return token, drive_id


# ── step 4: list SharePoint files via rclone lsjson ──────────────────────────

def list_sharepoint_files() -> list[dict]:
    """Returns list of {name, id} for all mp4s in WACS Archive."""
    result = subprocess.run(
        ["/opt/homebrew/bin/rclone", "lsjson", "onedrive:General - ACSD-Multimedia Production/WACS Archive", "--files-only"],
        capture_output=True, text=True
    )
    items = json.loads(result.stdout)
    return [{"name": x["Name"], "id": x["ID"].split("#")[-1]} for x in items if x["Name"].endswith(".mp4")]


# ── step 5: create sharing links via Graph API $batch ────────────────────────

def _to_direct_download(url: str) -> str:
    """Convert a SharePoint share URL to a direct download link."""
    if not url:
        return url
    url = url.replace("?download=1", "")
    parts = url.split("/")
    # Expected: https://liveedualdenschools.sharepoint.com/:v:/g/personal/USER/TOKEN
    # Target:   https://liveedualdenschools.sharepoint.com/personal/USER/_layouts/15/download.aspx?share=TOKEN
    if len(parts) < 7 or "/_layouts/" in url:
        return url
    domain = parts[0] + "//" + parts[2]
    token = parts[-1]
    user_path = parts[5] + "/" + parts[6]
    return f"{domain}/{user_path}/_layouts/15/download.aspx?share={token}"


def create_sharing_links(files: list[dict], token: str, drive_id: str) -> dict[str, str]:
    """Returns {filename: share_url} for all files. Caches results to avoid re-requesting."""
    links = {}
    if LINKS_CACHE.exists():
        links = json.loads(LINKS_CACHE.read_text())
        print(f"  Loaded {len(links)} cached links", flush=True)

    todo = [f for f in files if f["name"] not in links]
    if not todo:
        print("  All links already cached.", flush=True)
        return links

    print(f"  {len(todo)} files need links generated...", flush=True)
    batch_size = 20
    total = len(todo)

    for i in range(0, total, batch_size):
        batch = todo[i : i + batch_size]
        requests = [
            {
                "id": str(j),
                "method": "POST",
                "url": f"/drives/{drive_id}/items/{f['id']}/createLink",
                "headers": {"Content-Type": "application/json"},
                "body": {"type": "view", "scope": "anonymous"},
            }
            for j, f in enumerate(batch)
        ]
        retry = 0
        while retry < 5:
            try:
                resp = api_post(GRAPH_BATCH, {"requests": requests}, token)
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    wait = 10 * (2 ** retry)
                    print(f"  Rate limited, waiting {wait}s...", flush=True)
                    time.sleep(wait)
                    retry += 1
                else:
                    print(f"  Batch error at {i}: {e.code}", flush=True)
                    break
        else:
            print(f"  Skipping batch at {i} after max retries", flush=True)
            continue

        retry_items = []
        for r in resp.get("responses", []):
            idx = int(r["id"])
            if r["status"] in (200, 201):
                url = r["body"].get("link", {}).get("webUrl", "")
                if url:
                    links[batch[idx]["name"]] = _to_direct_download(url)
            elif r["status"] == 429:
                retry_items.append(batch[idx])
            else:
                print(f"  Warning: {batch[idx]['name']}: {r['status']}", flush=True)

        if retry_items:
            time.sleep(5)
            for f in retry_items:
                try:
                    single = api_post(GRAPH_BATCH, {"requests": [{"id": "0", "method": "POST", "url": f"/drives/{drive_id}/items/{f['id']}/createLink", "headers": {"Content-Type": "application/json"}, "body": {"type": "view", "scope": "anonymous"}}]}, token)
                    url = single["responses"][0].get("body", {}).get("link", {}).get("webUrl", "")
                    if url:
                        links[f["name"]] = _to_direct_download(url)
                except Exception:
                    pass

        LINKS_CACHE.write_text(json.dumps(links))

        done = min(i + batch_size, total)
        print(f"  SharePoint links: {done}/{total} ({len(links)} total)", flush=True)

    return links


# ── step 6: match YouTube videos to SharePoint files ─────────────────────────

def extract_title_from_stem(stem: str) -> str:
    stem = re.sub(r"^\d{4}\.\d{2}\.\d{2}\s*", "", stem)
    stem = re.sub(r"\.f\d+$", "", stem)  # strip yt-dlp format suffix (.f137 etc)
    return stem.strip()


def _file_date(filename: str):
    m = re.match(r"(\d{4})\.(\d{2})\.(\d{2})", filename)
    if not m:
        return None
    try:
        return date(*map(int, m.groups()))
    except ValueError:
        return None


def match_and_merge(yt_videos: list[dict], sp_links: dict[str, str]) -> list[dict]:
    # index files by normalized title
    by_title = {}
    for filename in sp_links:
        key = normalize(extract_title_from_stem(Path(filename).stem))
        if key:
            by_title.setdefault(key, []).append(filename)

    # every plausible (video, file) pair, scored by date gap
    pairs = []
    for vi, v in enumerate(yt_videos):
        titles = {normalize(v["title"])} | {normalize(t) for t in v.get("alt_titles", [])}
        try:
            vdate = date.fromisoformat(v["date"])
        except ValueError:
            vdate = None
        seen = set()
        for t in titles:
            for f in by_title.get(t, []):
                if f in seen:
                    continue
                seen.add(f)
                fdate = _file_date(f)
                gap = abs((fdate - vdate).days) if (fdate and vdate) else MAX_DATE_GAP_DAYS
                if gap <= MAX_DATE_GAP_DAYS:
                    pairs.append((gap, vi, f))

    # greedy: closest dates first; each video and each file used once
    pairs.sort()
    assigned, used = {}, set()
    for gap, vi, f in pairs:
        if vi in assigned or f in used:
            continue
        assigned[vi] = f
        used.add(f)

    results = []
    for vi, v in enumerate(yt_videos):
        f = assigned.get(vi)
        rec = {
            "id": v["id"],
            "title": v["title"],
            "date": v["date"],
            "thumbnail": v["thumbnail"],
            "description": v["description"],
            "duration": v["duration"],
            "download_url": sp_links[f] if f else "",
            "filename": f or "",
            "section": "daily" if v["title"].lower().startswith("wacs daily") else "archive",
        }
        if v.get("alt_titles"):
            rec["alt_titles"] = v["alt_titles"]
        results.append(rec)

    results.sort(key=lambda v: v["date"], reverse=True)
    unmatched = sum(1 for r in results if not r["filename"])
    print(f"  Matched {len(results) - unmatched}/{len(results)} videos to SharePoint files", flush=True)
    if unmatched:
        print(f"  {unmatched} videos have no SharePoint file", flush=True)
    return results


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    print("1/5  Loading video IDs from archive...")
    ids = load_video_ids()
    print(f"     {len(ids)} IDs found\n", flush=True)

    cached = json.loads(OUTPUT.read_text()) if OUTPUT.exists() else []
    print(f"2/5  Refreshing YouTube metadata ({len(cached)} cached entries)...")
    yt_videos = refresh_metadata(ids, cached)
    print(f"     Total: {len(yt_videos)} videos\n", flush=True)

    print("3/5  Listing SharePoint files (refreshes OAuth token as side effect)...")
    sp_files = list_sharepoint_files()
    print(f"     {len(sp_files)} files found\n", flush=True)

    token, drive_id = read_rclone_config()

    print("4/5  Creating sharing links via Graph API batch...")
    sp_links = create_sharing_links(sp_files, token, drive_id)
    # only link to files that still exist on SharePoint
    live_names = {f["name"] for f in sp_files}
    sp_links = {k: v for k, v in sp_links.items() if k in live_names}
    print(f"     {len(sp_links)} links ready\n", flush=True)

    print("5/5  Matching and merging...")
    videos = match_and_merge(yt_videos, sp_links)

    OUTPUT.write_text(json.dumps(videos, indent=2, ensure_ascii=False))
    print(f"\nDone. Wrote {len(videos)} videos to {OUTPUT}", flush=True)


if __name__ == "__main__":
    main()
