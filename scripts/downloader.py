import requests
import os
import sys
import json
import re
import time
import random
import gzip
import shutil
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email.utils import parsedate_to_datetime

# Configuration
SITES_FILE = "sites.txt"
DATA_DIR = "sitemaps_data" 
GLOBAL_STATE_FILE = os.path.join(DATA_DIR, "global_state.json")
USER_AGENT = "Mozilla/5.0 (compatible; SitemapHunterBot/2.1; +https://github.com/p4blo4p/bot-crawl-sitemap-images)"
MAX_WORKERS = 5 
TIME_LIMIT_SECONDS = 40 * 60  # 40 Minutes
MIN_DISK_FREE_BYTES = 512 * 1024 * 1024 # 512MB Buffer
MAX_FILES_PER_RUN = 100 

# Efficiency & Politeness
MAX_URL_RETRIES = 3 
DOMAIN_FAILURE_LIMIT = 20 
DEFAULT_CRAWL_DELAY = 1.0 

START_TIME = time.time()
FILES_PROCESSED_THIS_RUN = 0

# Regex
RE_LOC = re.compile(r'<loc>(.*?)</loc>', re.IGNORECASE)
RE_SITEMAP_INDEX = re.compile(r'<sitemapindex', re.IGNORECASE)
RE_RICH_METADATA = re.compile(r'(image:caption|image:title|news:title|video:title|video:description|<title>)', re.IGNORECASE)

def get_elapsed_time():
    return time.time() - START_TIME

def check_disk_space():
    try:
        total, used, free = shutil.disk_usage(DATA_DIR)
        return free > MIN_DISK_FREE_BYTES
    except:
        return True

def parse_date(date_str):
    if not date_str: return None
    try:
        return parsedate_to_datetime(date_str)
    except:
        return None

# --- STATE MANAGEMENT (SHARDED) ---

def load_global_state():
    """Loads only the high-level stats needed for sorting domains."""
    if os.path.exists(GLOBAL_STATE_FILE):
        try:
            with open(GLOBAL_STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"domain_stats": {}}

def save_global_state(state):
    try:
        with open(GLOBAL_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[!] Error saving global state: {e}")

def get_domain_state_path(domain):
    # Sanitize domain for folder name
    domain_safe = domain.replace("http://", "").replace("https://", "").replace("/", "_")
    return os.path.join(DATA_DIR, "domains", domain_safe, "state.json")

def load_domain_state(domain):
    """Loads the heavy state specific to one domain."""
    path = get_domain_state_path(domain)
    default_state = {
        "file_meta": {}, 
        "queues": [], 
        "visited": [],
        "errors": {}
    }
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                # Convert list back to deque for queue if needed, but JSON uses lists
                return data
        except:
            pass
    return default_state

def save_domain_state(domain, state):
    path = get_domain_state_path(domain)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[!] Error saving state for {domain}: {e}")

# --- CRAWLER LOGIC ---

def get_robots_parser(domain):
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(urljoin(domain, "/robots.txt"))
    try:
        rp.read()
        return rp
    except:
        return None

def get_initial_sitemaps(domain, rp):
    sitemaps = []
    if rp:
        sitemaps = rp.site_maps() or []
    
    if not sitemaps:
        common = ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"]
        for path in common:
            sitemaps.append(urljoin(domain, path))
            
    return list(set(sitemaps))

def classify_content(url, text_content):
    if RE_SITEMAP_INDEX.search(text_content):
        return "indices", "Index"
    if RE_RICH_METADATA.search(text_content):
        return "content_rich", "RICH (Metadata tags)"
    locs = RE_LOC.findall(text_content)
    if locs:
        seo_urls = [u for u in locs if ('-' in u or '_' in u) and len(u.split('/')[-1]) > 5]
        if len(seo_urls) > 0:
             return "content_rich", "RICH (SEO URLs)"
    return "content_raw", "RAW"

def update_global_stats(global_state, domain, file_date_str):
    if 'domain_stats' not in global_state: global_state['domain_stats'] = {}
    if domain not in global_state['domain_stats']:
        global_state['domain_stats'][domain] = {"total_files": 0, "newest_mod": None, "last_crawl": None}
    
    stats = global_state['domain_stats'][domain]
    stats['total_files'] += 1
    
    current_date = parse_date(file_date_str)
    if current_date:
        if not stats['newest_mod'] or current_date > datetime.fromisoformat(stats['newest_mod']):
            stats['newest_mod'] = current_date.isoformat()

def mark_domain_crawled(global_state, domain):
    if 'domain_stats' not in global_state: global_state['domain_stats'] = {}
    if domain not in global_state['domain_stats']: global_state['domain_stats'][domain] = {}
    global_state['domain_stats'][domain]['last_crawl'] = datetime.now().isoformat()
    save_global_state(global_state)

def process_url(url, domain_folder, domain_state, crawl_delay):
    time.sleep(crawl_delay + random.uniform(0, 0.5))
    headers = {"User-Agent": USER_AGENT}
    
    # Check retries
    if url in domain_state['errors'] and domain_state['errors'][url] >= MAX_URL_RETRIES:
        return (url, False, False, None, [], "MAX_RETRIES")

    # Check Cache
    cached_meta = domain_state['file_meta'].get(url, {})
    if cached_meta.get('etag'): headers['If-None-Match'] = cached_meta['etag']
    if cached_meta.get('last_modified'): headers['If-Modified-Since'] = cached_meta['last_modified']

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
        if name.lower().endswith(".xml"): name = name[:-4]
        elif name.lower().endswith(".xml.gz"): name = name[:-7]
        url_hash = hex(abs(hash(url)))[2:][:6] 
        
        save_dir = os.path.join(domain_folder, subfolder)
        os.makedirs(save_dir, exist_ok=True)
        filename = f"{name}_{url_hash}.xml.gz"
        save_path = os.path.join(save_dir, filename)
        
        with gzip.open(save_path, "wb") as f:
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

def process_site(domain, global_state):
    global FILES_PROCESSED_THIS_RUN
    print(f"\n=== Processing Domain: {domain} ===")
    
    if not domain.startswith("http"): domain = "https://" + domain
    
    rp = get_robots_parser(domain)
    crawl_delay = DEFAULT_CRAWL_DELAY
    if rp:
        rate = rp.request_rate(USER_AGENT)
        if rate: crawl_delay = rate.seconds / rate.requests
        elif rp.crawl_delay(USER_AGENT): crawl_delay = rp.crawl_delay(USER_AGENT)
    
    print(f"[*] Polite Delay: {crawl_delay:.2f}s")

    # Load Domain Specific State
    domain_state = load_domain_state(domain)
    
    # Setup Folder
    domain_safe = domain.replace("http://", "").replace("https://", "").replace("/", "_")
    domain_folder = os.path.join(DATA_DIR, "domains", domain_safe)
    
    # Initialize Queue
    if domain_state['queues']:
        print(f"[*] Resuming {len(domain_state['queues'])} URLs from saved state...")
        queue = deque(domain_state['queues'])
    else:
        queue = deque(get_initial_sitemaps(domain, rp))
        
    visited = set(domain_state['visited'])
    consecutive_failures = 0
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while queue:
                # --- EXIT CONDITIONS ---
                if FILES_PROCESSED_THIS_RUN >= MAX_FILES_PER_RUN:
                    print(f"\n[!] BATCH LIMIT ({MAX_FILES_PER_RUN}). Saving & Exiting.")
                    raise SystemExit
                if get_elapsed_time() > TIME_LIMIT_SECONDS:
                    print(f"\n[!] TIME LIMIT (40m). Saving & Exiting.")
                    mark_domain_crawled(global_state, domain)
                    raise SystemExit
                if not check_disk_space():
                    print(f"\n[!] DISK FULL. Saving & Exiting.")
                    mark_domain_crawled(global_state, domain)
                    raise SystemExit
                if consecutive_failures >= DOMAIN_FAILURE_LIMIT:
                    print(f"[!] Circuit breaker triggered for {domain}.")
                    mark_domain_crawled(global_state, domain)
                    break

                current_batch = []
                while queue and len(current_batch) < MAX_WORKERS:
                    u = queue.popleft()
                    if u not in visited:
                        visited.add(u)
                        current_batch.append(u)
                
                if not current_batch: continue
                    
                future_to_url = {
                    executor.submit(process_url, url, domain_folder, domain_state, crawl_delay): url 
                    for url in current_batch
                }
                
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        r_url, success, is_index, meta, children, status = future.result()
                        
                        if success or status == "NOT_MODIFIED":
                            consecutive_failures = 0
                            if url in domain_state['errors']: del domain_state['errors'][url]
                            
                            if success:
                                FILES_PROCESSED_THIS_RUN += 1
                                print(f"    [OK] {status} (#{FILES_PROCESSED_THIS_RUN}): {url}")
                                if meta: 
                                    domain_state['file_meta'][url] = meta
                                    update_global_stats(global_state, domain, meta.get('last_modified'))
                                if is_index and children:
                                    for child in children:
                                        if child not in visited: queue.append(child)
                            else:
                                print(f"    [OK] {status}: {url}")
                        else:
                            consecutive_failures += 1
                            print(f"    [ERR] {url}: {status}")
                            domain_state['errors'][url] = domain_state['errors'].get(url, 0) + 1
                    except:
                        consecutive_failures += 1
                
                # Checkpoint domain state periodically
                domain_state['queues'] = list(queue)
                domain_state['visited'] = list(visited)
                save_domain_state(domain, domain_state)

    except SystemExit:
        # Save before exiting
        domain_state['queues'] = list(queue)
        domain_state['visited'] = list(visited)
        save_domain_state(domain, domain_state)
        save_global_state(global_state)
        sys.exit(0)
    finally:
        # Ensure cleanup save
        domain_state['queues'] = list(queue)
        domain_state['visited'] = list(visited)
        save_domain_state(domain, domain_state)
        save_global_state(global_state)

    # Finished domain
    if domain_state['queues']: # If items left (circuit breaker), don't delete
         pass 
    else:
         domain_state['queues'] = []
    
    mark_domain_crawled(global_state, domain)

def main():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        global_state = load_global_state()
        
        try:
            with open(SITES_FILE, "r") as f:
                sites = [line.strip() for line in f if line.strip()]
            
            # Sort sites by 'last_crawl' timestamp from global state
            sites.sort(key=lambda s: global_state.get('domain_stats', {}).get(s, {}).get('last_crawl', "1970-01-01"))
            print(f"Loaded {len(sites)} sites. Sorted by staleness.")

        except FileNotFoundError:
            print("No sites.txt found.")
            return

        for site in sites:
            process_site(site, global_state)
            
    except KeyboardInterrupt:
        print("\n[!] Interrupted.")
    except Exception as e:
        print(f"\n[!] Crash: {e}")
    finally:
        print("\n=== Job Complete ===")

if __name__ == "__main__":
    main()
