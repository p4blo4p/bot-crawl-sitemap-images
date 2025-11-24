import requests
import os
import sys
import json
import re
import time
import hashlib
from urllib.parse import urljoin, urlparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configuration
SITES_FILE = "sites.txt"
DATA_DIR = "sitemaps_data" 
STATE_FILE = os.path.join(DATA_DIR, "state.json")
USER_AGENT = "Mozilla/5.0 (compatible; SitemapHunterBot/1.0)"
MAX_WORKERS = 5 
# GitHub Actions limit is usually 60 mins. We stop at 50 mins to allow time for git push.
TIME_LIMIT_SECONDS = 50 * 60 
START_TIME = time.time()

# Regex
RE_LOC = re.compile(r'<loc>(.*?)</loc>', re.IGNORECASE)
RE_SITEMAP_INDEX = re.compile(r'<sitemapindex', re.IGNORECASE)

# Relaxed Regex: Matches the presence of specific "rich" namespaces or tags
# We look for image:caption, image:title, news:title, video:title/description.
RE_RICH_METADATA = re.compile(r'(image:caption|image:title|news:title|video:title|video:description|<title>)', re.IGNORECASE)

def get_elapsed_time():
    return time.time() - START_TIME

def load_state():
    default_state = {"file_meta": {}, "queues": {}, "visited": {}}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                if "file_meta" not in data: data["file_meta"] = {}
                if "queues" not in data: data["queues"] = {}
                if "visited" not in data: data["visited"] = {}
                return data
        except:
            return default_state
    return default_state

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def get_initial_sitemaps(domain):
    """Extracts initial sitemap URLs from robots.txt."""
    if not domain.startswith("http"):
        domain = "https://" + domain
        
    robots_url = urljoin(domain, "/robots.txt")
    sitemaps = []
    print(f"[*] Checking robots.txt: {robots_url}")
    try:
        response = requests.get(robots_url, headers={"User-Agent": USER_AGENT}, timeout=10)
        if response.status_code == 200:
            for line in response.text.splitlines():
                if line.lower().strip().startswith("sitemap:"):
                    sitemaps.append(line.split(":", 1)[1].strip())
    except Exception as e:
        print(f"[!] Error robots.txt {domain}: {e}")
    
    if not sitemaps:
        sitemaps.append(urljoin(domain, "/sitemap.xml"))
        
    return sitemaps

def classify_content(url, text_content):
    """
    Decides if the sitemap is 'Indices', 'Rich' (searchable text), or 'Raw' (junk/IDs).
    """
    # 1. Check for Index
    if RE_SITEMAP_INDEX.search(text_content):
        return "indices", "Index"

    # 2. Check for Rich Metadata Tags (High Value)
    if RE_RICH_METADATA.search(text_content):
        return "content_rich", "RICH (Metadata tags found)"

    # 3. Check for Descriptive URLs (SEO Slugs)
    # If URLs contain hyphens or underscores and are readable, they are valuable for search.
    # e.g. /manga/dragon-ball-super-chapter-1 (Good) vs /post/12345 (Bad)
    locs = RE_LOC.findall(text_content)
    if locs:
        # Filter for "slug-like" URLs (contain - or _ and represent paths)
        seo_urls = [u for u in locs if ('-' in u or '_' in u) and len(u.split('/')[-1]) > 5]
        
        # If a good portion of URLs look descriptive, treat as rich
        if len(seo_urls) > 0:
             return "content_rich", "RICH (SEO-friendly URLs detected)"

    # 4. Fallback -> Raw (Just pure links/IDs or images with no text)
    return "content_raw", "RAW (No descriptive text/slugs found)"

def process_url(url, domain_folder, state):
    """
    Downloads a single URL if modified and classifies it.
    """
    headers = {"User-Agent": USER_AGENT}
    
    # Incremental Check
    cached_meta = state['file_meta'].get(url, {})
    if 'etag' in cached_meta and cached_meta['etag']:
        headers['If-None-Match'] = cached_meta['etag']
    if 'last_modified' in cached_meta and cached_meta['last_modified']:
        headers['If-Modified-Since'] = cached_meta['last_modified']

    try:
        response = requests.get(url, headers=headers, timeout=15)
        
        # 304 Not Modified -> Skip
        if response.status_code == 304:
            return (url, True, cached_meta.get('is_index', False), None, [])

        if response.status_code != 200:
            print(f"    [ERR] {response.status_code} for {url}")
            return (url, False, False, None, [])

        content = response.content
        text_content = response.text
        
        # Classification Logic
        subfolder, reason = classify_content(url, text_content)
        is_index = (subfolder == "indices")
        is_rich = (subfolder == "content_rich")
        
        print(f"    [CLASS] {url} -> {reason}")
        
        # Save File
        parsed = urlparse(url)
        name = os.path.basename(parsed.path) or "sitemap"
        if not name.endswith(".xml"): name += ".xml"
        url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
        
        save_dir = os.path.join(domain_folder, subfolder)
        os.makedirs(save_dir, exist_ok=True)
        
        filename = f"{name}_{url_hash}.xml"
        save_path = os.path.join(save_dir, filename)
        
        with open(save_path, "wb") as f:
            f.write(content)
            
        children = []
        if is_index:
            children = [c.strip() for c in RE_LOC.findall(text_content)]
            
        new_meta = {
            'etag': response.headers.get('ETag'),
            'last_modified': response.headers.get('Last-Modified'),
            'is_index': is_index,
            'is_rich': is_rich,
            'local_path': save_path,
            'classification': subfolder,
            'last_check': datetime.now().isoformat()
        }
        
        return (url, True, is_index, new_meta, children)

    except Exception as e:
        print(f"    [EXC] {url}: {e}")
        return (url, False, False, None, [])

def process_site(domain, state):
    print(f"\n=== Processing Domain: {domain} ===")
    domain_safe = domain.replace("http://", "").replace("https://", "").replace("/", "_")
    domain_folder = os.path.join(DATA_DIR, "domains", domain_safe)
    
    saved_queue = state['queues'].get(domain, [])
    if saved_queue:
        print(f"[*] Resuming {len(saved_queue)} URLs from previous run...")
        queue = deque(saved_queue)
    else:
        queue = deque(get_initial_sitemaps(domain))
        
    visited = set(state['visited'].get(domain, []))
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while queue:
            if get_elapsed_time() > TIME_LIMIT_SECONDS:
                print(f"\n[!] Time limit reached ({TIME_LIMIT_SECONDS}s). Saving state and exiting gracefully.")
                state['queues'][domain] = list(queue)
                state['visited'][domain] = list(visited)
                save_state(state)
                sys.exit(0)

            current_batch = []
            while queue and len(current_batch) < MAX_WORKERS * 2:
                u = queue.popleft()
                if u not in visited:
                    visited.add(u)
                    current_batch.append(u)
            
            if not current_batch:
                continue
                
            print(f"  -> Batch: {len(current_batch)} URLs (Queue: {len(queue)})")
            
            future_to_url = {
                executor.submit(process_url, url, domain_folder, state): url 
                for url in current_batch
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    r_url, success, is_index, meta, children = future.result()
                    if success:
                        if meta: 
                            state['file_meta'][url] = meta
                        
                        if is_index and children:
                            for child in children:
                                if child not in visited:
                                    queue.append(child)
                except Exception as exc:
                    print(f"    [ERR] Thread exception {url}: {exc}")
            
            state['queues'][domain] = list(queue)
            state['visited'][domain] = list(visited)
            save_state(state)

    if domain in state['queues']:
        del state['queues'][domain]

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    state = load_state()
    
    try:
        with open(SITES_FILE, "r") as f:
            sites = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("No sites.txt found.")
        return

    for site in sites:
        process_site(site, state)
        save_state(state)
        
        if get_elapsed_time() > TIME_LIMIT_SECONDS:
             print(f"\n[!] Global time limit reached. Stopping.")
             sys.exit(0)

    print("\n=== Job Complete ===")

if __name__ == "__main__":
    main()
