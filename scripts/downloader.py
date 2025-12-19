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
import logging
import signal
import socket
from urllib.parse import urljoin, urlparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email.utils import parsedate_to_datetime

# --- Configuration ---
SITES_FILE = os.getenv("SITES_FILE", "sites.txt")
DATA_DIR = "sitemaps_data" 
GLOBAL_STATE_FILE = os.path.join(DATA_DIR, "global_state.json")
LOG_FILE = "downloader.log"

# Lista de User-Agents para rotar y evitar bloqueos
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0"
]

MAX_WORKERS = 5 
TIME_LIMIT_SECONDS = 340 * 60 # 5.6 hours (GHA limit is 6h)
MIN_DISK_FREE_BYTES = 512 * 1024 * 1024 
MAX_FILES_PER_RUN = 500
TIMEOUT = 25

# Efficiency & Politeness
MAX_URL_RETRIES = 3 
DOMAIN_FAILURE_LIMIT = 25 
DEFAULT_CRAWL_DELAY = 2.0  # Aumentado para ser más respetuoso

COMMON_PATHS = ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml", "/sitemap.php", "/sitemap.xml.gz"]

START_TIME = time.time()
FILES_PROCESSED_THIS_RUN = 0

# --- Logging Setup ---
sys.stdout.reconfigure(line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE)]
)
logger = logging.getLogger(__name__)

# --- Regex ---
RE_LOC = re.compile(r'<loc>(.*?)</loc>', re.IGNORECASE)
RE_SITEMAP_INDEX = re.compile(r'<sitemapindex', re.IGNORECASE)
RE_RICH_METADATA = re.compile(r'(image:caption|image:title|news:title|video:title|video:description|<title>)', re.IGNORECASE)

def get_elapsed_time():
    return time.time() - START_TIME

def check_disk_space():
    try:
        total, used, free = shutil.disk_usage(".")
        return free > MIN_DISK_FREE_BYTES
    except:
        return True

def atomic_write_json(filepath, data):
    try:
        temp_path = filepath + ".tmp"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(temp_path, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(temp_path, filepath)
    except Exception as e:
        logger.error(f"Failed to save state to {filepath}: {e}")

# --- STATE & STATS MANAGEMENT ---

def ensure_state_keys(state):
    if not isinstance(state, dict):
        return {"domain_stats": {}}
    if "domain_stats" not in state:
        state["domain_stats"] = {}
    return state

def load_global_state():
    state = {"domain_stats": {}}
    if os.path.exists(GLOBAL_STATE_FILE):
        try:
            with open(GLOBAL_STATE_FILE, 'r') as f:
                loaded = json.load(f)
                state = ensure_state_keys(loaded)
        except Exception as e:
            logger.error(f"Global state file corrupted ({e}). Starting fresh.")
    return state

def save_global_state(state):
    state = ensure_state_keys(state)
    atomic_write_json(GLOBAL_STATE_FILE, state)

def get_domain_state_path(domain):
    domain_safe = domain.replace("http://", "").replace("https://", "").replace("/", "_").replace(".", "_")
    return os.path.join(DATA_DIR, "domains", domain_safe, "state.json")

def load_domain_state(domain):
    path = get_domain_state_path(domain)
    default_state = {"file_meta": {}, "queues": [], "visited": [], "errors": {}}
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    return loaded
        except: pass
    return default_state

def save_domain_state(domain, state):
    path = get_domain_state_path(domain)
    atomic_write_json(path, state)

def update_stats(global_state, domain, key, increment=1):
    if "domain_stats" not in global_state:
        global_state["domain_stats"] = {}
        
    if domain not in global_state['domain_stats']:
        global_state['domain_stats'][domain] = {
            "sitemaps_downloaded": 0, "urls_discovered": 0, "errors_total": 0,
            "index_count": 0, "rich_content_count": 0, "bytes_processed": 0,
            "last_crawl": None, "avg_download_time": 0
        }
    
    stats = global_state['domain_stats'][domain]
    if key in stats:
        stats[key] += increment

def normalize_domain(domain):
    """Normaliza el dominio para evitar duplicados HTTP/HTTPS"""
    if not domain.startswith(('http://', 'https://')):
        domain = 'https://' + domain
    parsed = urlparse(domain)
    return f"{parsed.scheme}://{parsed.netloc}"

def validate_url(url, base_domain):
    """Valida que una URL pertenezca al dominio base"""
    try:
        parsed = urlparse(url)
        base_parsed = urlparse(base_domain)
        return parsed.netloc == base_parsed.netloc
    except:
        return False

def get_random_user_agent():
    """Selecciona un User-Agent aleatorio de la lista"""
    return random.choice(USER_AGENTS)

def check_domain_exists(domain):
    """
    Verifica si un dominio existe y es accesible antes de procesarlo.
    Returns:
        tuple: (exists, error_message)
    """
    try:
        parsed = urlparse(domain)
        hostname = parsed.netloc
        
        # Verificar resolución DNS
        socket.gethostbyname(hostname)
        
        # Intentar una conexión simple
        response = requests.get(
            domain, 
            headers={"User-Agent": get_random_user_agent()}, 
            timeout=10,
            allow_redirects=True
        )
        
        if response.status_code >= 400:
            return False, f"Domain returned HTTP {response.status_code}"
            
        return True, None
    except socket.gaierror:
        return False, "DNS resolution failed"
    except requests.exceptions.RequestException as e:
        return False, f"Connection error: {str(e)}"
    except Exception as e:
        return False, f"Unknown error: {str(e)}"

def parse_robots_txt(domain):
    """
    Parsea el archivo robots.txt y extrae los sitemaps declarados.
    Returns:
        tuple: (sitemaps, crawl_delay)
    """
    sitemaps = []
    crawl_delay = DEFAULT_CRAWL_DELAY
    
    try:
        robots_url = urljoin(domain, "/robots.txt")
        logger.info(f"Checking robots.txt at {robots_url}")
        
        response = requests.get(
            robots_url, 
            headers={"User-Agent": get_random_user_agent()}, 
            timeout=TIMEOUT
        )
        
        if response.status_code == 200:
            content = response.text
            logger.info(f"robots.txt found for {domain}")
            
            # Extraer sitemaps del robots.txt
            for line in content.splitlines():
                line = line.strip()
                if line.lower().startswith("sitemap:"):
                    sitemap_url = line.split(":", 1)[1].strip()
                    if sitemap_url:
                        sitemaps.append(sitemap_url)
                elif line.lower().startswith("crawl-delay"):
                    try:
                        delay = float(line.split(":", 1)[1].strip())
                        if delay > 0:
                            crawl_delay = max(crawl_delay, delay)
                            logger.info(f"Crawl delay set to {crawl_delay}s from robots.txt")
                    except ValueError:
                        pass
        else:
            logger.warning(f"robots.txt not found (HTTP {response.status_code}) for {domain}")
    except Exception as e:
        logger.warning(f"Error parsing robots.txt for {domain}: {str(e)}")
    
    return sitemaps, crawl_delay

def get_with_retry(url, headers, max_retries=MAX_URL_RETRIES):
    """Implementa un retraso exponencial para reintentos con User-Agent aleatorio"""
    for attempt in range(max_retries):
        try:
            # Añadir User-Agent aleatorio en cada intento
            headers["User-Agent"] = get_random_user_agent()
            
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
            
            # Manejar específicamente el código 429 (Too Many Requests)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 2 ** attempt))
                logger.warning(f"Rate limited. Waiting {retry_after}s before retrying...")
                time.sleep(retry_after)
                continue
                
            # Manejar específicamente el código 403 (Forbidden)
            if response.status_code == 403:
                logger.warning(f"Access forbidden (403) for {url}. Waiting before retry...")
                time.sleep(5 + (2 ** attempt))
                continue
                
            return response
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    return None

# --- CRAWLER LOGIC ---

def process_url(url, domain_folder, domain_state, crawl_delay, base_domain):
    time.sleep(crawl_delay + random.uniform(0, 0.5))  # Aumentado el random para más variabilidad
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    
    cached_meta = domain_state.get('file_meta', {}).get(url, {})
    if cached_meta.get('etag'): headers['If-None-Match'] = cached_meta['etag']
    if cached_meta.get('last_modified'): headers['If-Modified-Since'] = cached_meta['last_modified']

    start_time = time.time()
    try:
        response = get_with_retry(url, headers)
        download_time = time.time() - start_time
        
        if response.status_code == 304:
            return (url, True, cached_meta.get('is_index', False), None, [], "NOT_MODIFIED", 0, download_time)
        
        if response.status_code != 200:
            return (url, False, False, None, [], f"HTTP_{response.status_code}", 0, download_time)

        content = response.content
        text_content = content.decode('utf-8', 'ignore')
        
        is_index = bool(RE_SITEMAP_INDEX.search(text_content))
        is_rich = bool(RE_RICH_METADATA.search(text_content))
        subfolder = "indices" if is_index else ("content_rich" if is_rich else "content_raw")
        
        parsed = urlparse(url)
        name = os.path.basename(parsed.path) or "sitemap"
        url_hash = hex(abs(hash(url)))[2:][:6] 
        save_dir = os.path.join(domain_folder, subfolder)
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, f"{name}_{url_hash}.xml.gz")
        
        with gzip.open(save_path, "wb") as f:
            f.write(content)
            
        locs = [l.strip() for l in RE_LOC.findall(text_content)]
        # Filtrar URLs para mantener solo las del mismo dominio
        valid_locs = [l for l in locs if validate_url(l, base_domain)]
        
        new_meta = {
            'etag': response.headers.get('ETag'),
            'last_modified': response.headers.get('Last-Modified'),
            'is_index': is_index,
            'is_rich': is_rich,
            'urls_count': len(valid_locs),
            'last_check': datetime.now().isoformat()
        }
        
        return (url, True, is_index, new_meta, valid_locs, "DOWNLOADED", len(content), download_time)
    except requests.exceptions.Timeout:
        return (url, False, False, None, [], "TIMEOUT", 0, time.time() - start_time)
    except requests.exceptions.ConnectionError:
        return (url, False, False, None, [], "CONNECTION_ERROR", 0, time.time() - start_time)
    except Exception as e:
        return (url, False, False, None, [], str(e), 0, time.time() - start_time)

def process_site(domain, global_state):
    global FILES_PROCESSED_THIS_RUN
    logger.info(f"=== Site: {domain} ===")
    
    # Normalizar dominio
    domain = normalize_domain(domain)
    
    # Verificar si el dominio existe antes de procesarlo
    domain_exists, error_msg = check_domain_exists(domain)
    if not domain_exists:
        logger.error(f"Domain {domain} is not accessible: {error_msg}")
        update_stats(global_state, domain, "errors_total", 5)  # Penalizar errores de dominio
        return
    
    domain_state = load_domain_state(domain)
    domain_name_clean = urlparse(domain).netloc or domain.replace("https://", "").replace("http://", "")
    domain_folder = os.path.join(DATA_DIR, "domains", domain_name_clean.replace(".", "_"))
    
    # Discovery - Parsear robots.txt primero
    logger.info(f"Discovering sitemaps for {domain}")
    sitemaps_from_robots, crawl_delay = parse_robots_txt(domain)
    
    # Crear conjunto de URLs de sitemap
    seeds = set()
    
    # Añadir sitemaps encontrados en robots.txt
    for s in sitemaps_from_robots:
        if validate_url(s, domain):
            seeds.add(s)
            logger.info(f"Found sitemap in robots.txt: {s}")
    
    # Fallback Discovery - si no se encontraron sitemaps en robots.txt
    if not seeds:
        logger.info(f"No sitemaps found in robots.txt, trying common paths for {domain}")
        for path in COMMON_PATHS:
            seeds.add(urljoin(domain, path))

    if domain_state.get('queues'):
        queue = deque(domain_state['queues'])
    else:
        queue = deque(list(seeds))
        
    visited = set(domain_state.get('visited', []))
    consecutive_failures = 0
    
    logger.info(f"Using crawl delay of {crawl_delay}s for {domain}")
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while queue and FILES_PROCESSED_THIS_RUN < MAX_FILES_PER_RUN:
                if get_elapsed_time() > TIME_LIMIT_SECONDS or not check_disk_space():
                    break
                
                batch = []
                while queue and len(batch) < MAX_WORKERS:
                    u = queue.popleft()
                    if u not in visited:
                        visited.add(u)
                        batch.append(u)
                
                if not batch: continue
                
                futures = {executor.submit(process_url, u, domain_folder, domain_state, crawl_delay, domain): u for u in batch}
                for future in as_completed(futures):
                    url = futures[future]
                    try:
                        res = future.result()
                        url, success, is_index, meta, locs, status, b_size, download_time = res
                        
                        # Actualizar tiempo promedio de descarga
                        if "domain_stats" in global_state and domain in global_state["domain_stats"]:
                            stats = global_state["domain_stats"][domain]
                            if "avg_download_time" in stats:
                                # Calcular nuevo promedio
                                current_avg = stats["avg_download_time"]
                                count = stats.get("sitemaps_downloaded", 0)
                                if count > 0:
                                    stats["avg_download_time"] = (current_avg * count + download_time) / (count + 1)
                                else:
                                    stats["avg_download_time"] = download_time
                        
                        update_stats(global_state, domain, "bytes_processed", b_size)
                        
                        if success or status == "NOT_MODIFIED":
                            consecutive_failures = 0
                            if status != "NOT_MODIFIED":
                                FILES_PROCESSED_THIS_RUN += 1
                                update_stats(global_state, domain, "sitemaps_downloaded")
                                update_stats(global_state, domain, "urls_discovered", meta['urls_count'])
                                if is_index: update_stats(global_state, domain, "index_count")
                                if meta['is_rich']: update_stats(global_state, domain, "rich_content_count")
                                
                                if 'file_meta' not in domain_state: domain_state['file_meta'] = {}
                                domain_state['file_meta'][url] = meta
                                if is_index:
                                    for l in locs:
                                        if l not in visited: queue.append(l)
                                logger.info(f"  [OK] {url} (+{meta['urls_count']} urls, {download_time:.2f}s)")
                            else:
                                logger.info(f"  [CACHE] {url}")
                        else:
                            update_stats(global_state, domain, "errors_total")
                            consecutive_failures += 1
                            
                            # Manejo específico para errores 403
                            if "HTTP_403" in status:
                                logger.warning(f"Access forbidden (403) for {url}. Increasing delay...")
                                # Aumentar el retraso para este dominio
                                crawl_delay = min(crawl_delay * 1.5, 10.0)
                                logger.warning(f"New crawl delay for {domain}: {crawl_delay}s")
                            
                            logger.warning(f"  [ERR] {url}: {status}")
                    except Exception as e:
                        logger.error(f"Error processing future for {url}: {e}")

                if consecutive_failures > DOMAIN_FAILURE_LIMIT:
                    logger.error(f"Circuit breaker triggered for {domain}")
                    break
    finally:
        if "domain_stats" not in global_state: global_state["domain_stats"] = {}
        if domain not in global_state["domain_stats"]:
            update_stats(global_state, domain, "bytes_processed", 0)
        
        global_state['domain_stats'][domain]['last_crawl'] = datetime.now().isoformat()
        domain_state['queues'] = list(queue)
        domain_state['visited'] = list(visited)
        save_domain_state(domain, domain_state)
        save_global_state(global_state)

def main():
    logger.info("Downloader Job Started")
    state = load_global_state()
    
    def handler(sig, frame):
        logger.info("Termination signal received. Saving state...")
        save_global_state(state)
        sys.exit(0)
        
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    
    try:
        if not os.path.exists(SITES_FILE):
            logger.error(f"Sites file {SITES_FILE} not found!")
            return

        with open(SITES_FILE, "r") as f:
            sites = [l.strip() for l in f if l.strip()]
            
        # Normalizar dominios para evitar duplicados
        normalized_sites = []
        seen_domains = set()
        for site in sites:
            normalized = normalize_domain(site)
            domain = urlparse(normalized).netloc
            if domain not in seen_domains:
                normalized_sites.append(normalized)
                seen_domains.add(domain)
        
        # Ordenar sitios por última fecha de rastreo
        normalized_sites.sort(key=lambda s: state.get('domain_stats', {}).get(s, {}).get('last_crawl') or '1970')
        
        for site in normalized_sites:
            if get_elapsed_time() > TIME_LIMIT_SECONDS:
                logger.info("Time limit reached. Stopping crawler.")
                break
            try:
                process_site(site, state)
            except Exception as e:
                logger.error(f"Failed to process site {site}: {e}", exc_info=True)
                
    except Exception as e:
        logger.critical(f"Global Crash: {e}", exc_info=True)
    finally:
        save_global_state(state)
        logger.info("Job Complete")

if __name__ == "__main__":
    main()