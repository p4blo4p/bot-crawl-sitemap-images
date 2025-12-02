import requests
import os
import sys
import json
import re
import time
import random
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, parsedatetime

# Configuration
SITES_FILE = "sites.txt"
DATA_DIR = "sitemaps_data" 
STATE_FILE = os.path.join(DATA_DIR, "state.json")
USER_AGENT = "Mozilla/5.0 (compatible; SitemapHunterBot/2.0; +https://github.com/p4blo4p/bot-crawl-sitemap-images)"
MAX_WORKERS = 5 
TIME_LIMIT_SECONDS = 50 * 60 

# Efficiency & Politeness
MAX_URL_RETRIES = 3 
DOMAIN_FAILURE_LIMIT = 20 
DEFAULT_CRAWL_DELAY = 1.0 # Seconds

START_TIME = time.time()

# Regex
RE_LOC = re.compile(r'<loc>(.*?)</loc>', re.IGNORECASE)
RE_SITEMAP_INDEX = re.compile(r'<sitemapindex', re.IGNORECASE)
RE_RICH_METADATA = re.compile(r'(image:caption|image:title|news:title|video:title|video:description|<title>)', re.IGNORECASE)

def get_elapsed_time():
    return time.time() - START_TIME

def parse_date(date_str):
    if not date_str: return None
    try:
        # Attempt to parse common HTTP date formats
        return datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')
    except:
        return None

def load_state():
    default_state = {
        "file_meta": {}, 
        "queues": {}, 
        "visited": {},
        "errors": {},
        "domain_stats": {} # { "domain": { "total_files": 0, "newest_mod": null, "oldest_mod": null } }
    }
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                for k in default_state:
                    if k not in data: data[k] = default_state[k]
                return data
        except:
            return default_state
    return default_state

def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[!] Critical error saving state: {e}")

def get_robots_parser(domain):
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(urljoin(domain, "/robots.txt"))
    try:
        rp.read()
        return rp
    except:
        return None

def get_initial_sitemaps(domain, rp):
    """Extracts initial sitemap URLs from robots.txt using RobotFileParser."""
    sitemaps = []
    if rp:
        sitemaps = rp.site_maps() or []
    
    if not sitemaps:
        # Fallback common locations
        common = ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"]
        for path in common:
            sitemaps.append(urljoin(domain, path))
            
    return list(set(sitemaps)) # Dedup

def classify_content(url, text_content):
    if RE_SITEMAP_INDEX.search(text_content):
        return "indices", "Index"

    if RE_RICH_METADATA.search(text_content):
        return "content_rich", "RICH (Metadata tags found)"

    locs = RE_LOC.findall(text_content)
    if locs:
        # Heuristic: Check for SEO friendly URLs (hyphens/underscores)
        seo_urls = [u for u in locs if ('-' in u or '_' in u) and len(u.split('/')[-1]) > 5]
        if len(seo_urls) > 0:
             return "content_rich", "RICH (SEO-friendly URLs detected)"

    return "content_raw", "RAW (No descriptive text/slugs found)"

def update_domain_stats(state, domain, file_date_str):
    if domain not in state['domain_stats']:
        state['domain_stats'][domain] = {"total_files": 0, "newest_mod": None, "oldest_mod": None}
    
    stats = state['domain_stats'][domain]
    stats['total_files'] += 1
    
    current_date = parse_date(file_date_str)
    if current_date:
        # Update Newest
        if not stats['newest_mod'] or current_date > datetime.fromisoformat(stats['newest_mod']):
            stats['newest_mod'] = current_date.isoformat()
        # Update Oldest
        if not stats['oldest_mod'] or current_date < datetime.fromisoformat(stats['oldest_mod']):
            stats['oldest_mod'] = current_date.isoformat()

def process_url(url, domain_folder, state, crawl_delay):
    # Polite Delay
    time.sleep(crawl_delay + random.uniform(0, 0.5))

    headers = {"User-Agent": USER_AGENT}
    
    error_count = state['errors'].get(url, 0)
    if error_count >= MAX_URL_RETRIES:
        return (url, False, False, None, [], "MAX_RETRIES_EXCEEDED")

    cached_meta = state['file_meta'].get(url, {})
    if 'etag' in cached_meta and cached_meta['etag']:
        headers['If-None-Match'] = cached_meta['etag']
    if 'last_modified' in cached_meta and cached_meta['last_modified']:
        headers['If-Modified-Since'] = cached_meta['last_modified']

    try:
        response = requests.get(url, headers=headers, timeout=20)
        
        if response.status_code == 304:
            return (url, True, cached_meta.get('is_index', False), None, [], "NOT_MODIFIED")

        if response.status_code != 200:
            return (url, False, False, None, [], f"HTTP_{response.status_code}")

        content = response.content
        text_content = response.text
        
        subfolder, reason = classify_content(url, text_content)
        is_index = (subfolder == "indices")
        is_rich = (subfolder == "content_rich")
        
        # Save File
        parsed = urlparse(url)
        name = os.path.basename(parsed.path) or "sitemap"
        if not name.endswith(".xml"): name += ".xml"
        # Short hash to avoid filename collisions but keep readable name
        url_hash = hex(abs(hash(url)))[2:][:6] 
        
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
        
        return (url, True, is_index, new_meta, children, "DOWNLOADED")

    except Exception as e:
        return (url, False, False, None, [], str(e))

def process_site(domain, state):
    print(f"\n=== Processing Domain: {domain} ===")
    
    if not domain.startswith("http"): domain = "https://" + domain
    
    # 1. Setup Robot Parser
    rp = get_robots_parser(domain)
    crawl_delay = DEFAULT_CRAWL_DELAY
    if rp:
        rate = rp.request_rate(USER_AGENT)
        if rate:
            crawl_delay = rate.seconds / rate.requests
        elif rp.crawl_delay(USER_AGENT):
            crawl_delay = rp.crawl_delay(USER_AGENT)
    
    print(f"[*] Polite Delay: {crawl_delay:.2f}s")

    # 2. Setup Folder
    domain_safe = domain.replace("http://", "").replace("https://", "").replace("/", "_")
    domain_folder = os.path.join(DATA_DIR, "domains", domain_safe)
    
    # 3. Load Queue
    saved_queue = state['queues'].get(domain, [])
    if saved_queue:
        print(f"[*] Resuming {len(saved_queue)} URLs...")
        queue = deque(saved_queue)
    else:
        queue = deque(get_initial_sitemaps(domain, rp))
        
    visited = set(state['visited'].get(domain, []))
    consecutive_failures = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while queue:
            if get_elapsed_time() > TIME_LIMIT_SECONDS:
                print(f"\n[!] Global time limit reached.")
                state['queues'][domain] = list(queue)
                state['visited'][domain] = list(visited)
                save_state(state)
                sys.exit(0)

            if consecutive_failures >= DOMAIN_FAILURE_LIMIT:
                print(f"[!] Circuit breaker triggered for {domain}. Skipping.")
                state['queues'][domain] = list(queue)
                state['visited'][domain] = list(visited)
                save_state(state)
                return 

            current_batch = []
            while queue and len(current_batch) < MAX_WORKERS:
                u = queue.popleft()
                if u not in visited:
                    visited.add(u)
                    current_batch.append(u)
            
            if not current_batch: continue
                
            future_to_url = {
                executor.submit(process_url, url, domain_folder, state, crawl_delay): url 
                for url in current_batch
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    r_url, success, is_index, meta, children, status = future.result()
                    
                    if success or status == "NOT_MODIFIED":
                        consecutive_failures = 0
                        if url in state['errors']: del state['errors'][url]
                        
                        if success:
                            print(f"    [OK] {status}: {url}")
                            if meta: 
                                state['file_meta'][url] = meta
                                update_domain_stats(state, domain, meta.get('last_modified'))
                            if is_index and children:
                                for child in children:
                                    if child not in visited: queue.append(child)
                        else:
                            print(f"    [OK] {status}: {url}")

                    elif status == "MAX_RETRIES_EXCEEDED":
                         print(f"    [DROP] {url} (Max Retries)")
                    else:
                        consecutive_failures += 1
                        print(f"    [ERR] {url}: {status}")
                        state['errors'][url] = state['errors'].get(url, 0) + 1

                except Exception as exc:
                    print(f"    [ERR] Thread exception {url}: {exc}")
                    consecutive_failures += 1
            
            # Checkpoint every batch
            state['queues'][domain] = list(queue)
            state['visited'][domain] = list(visited)
            save_state(state)

    if domain in state['queues']: del state['queues'][domain]

def main():
    try:
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
            
    except KeyboardInterrupt:
        print("\n[!] Interrupted.")
    except Exception as e:
        print(f"\n[!] Unexpected Crash: {e}")
    finally:
        if 'state' in locals(): save_state(state)
        print("\n=== Job Complete ===")

if __name__ == "__main__":
    main()
