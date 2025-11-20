import requests
import os
import sys
import datetime
import re
from urllib.parse import urljoin, urlparse
from collections import deque

# Configuration
SITES_FILE = "sites.txt"
USER_AGENT = "Mozilla/5.0 (compatible; SitemapHunterBot/1.0)"

# Regex for extracting URLs
RE_LOC = re.compile(r'<loc>(.*?)</loc>', re.IGNORECASE)
RE_SITEMAP_INDEX = re.compile(r'<sitemapindex', re.IGNORECASE)

def get_initial_sitemaps(domain):
    """Extracts initial sitemap URLs from robots.txt."""
    if not domain.startswith("http"):
        domain = "https://" + domain
        
    robots_url = urljoin(domain, "/robots.txt")
    sitemaps = []
    try:
        print(f"[*] Checking robots.txt: {robots_url}")
        response = requests.get(robots_url, headers={"User-Agent": USER_AGENT}, timeout=10)
        if response.status_code == 200:
            for line in response.text.splitlines():
                if line.lower().strip().startswith("sitemap:"):
                    sitemap_url = line.split(":", 1)[1].strip()
                    sitemaps.append(sitemap_url)
    except Exception as e:
        print(f"[!] Error fetching robots.txt for {domain}: {e}")
    
    # Fallback if none found in robots.txt
    if not sitemaps:
        print(f"[-] No sitemaps in robots.txt for {domain}, trying default /sitemap.xml")
        sitemaps.append(urljoin(domain, "/sitemap.xml"))
        
    return sitemaps

def save_xml(content, url, folder):
    """Saves XML content to file."""
    try:
        parsed = urlparse(url)
        # Create safe filename: domain_path_hash.xml
        domain_safe = parsed.netloc.replace(".", "_").replace(":", "")
        name_safe = os.path.basename(parsed.path)
        if not name_safe: name_safe = "sitemap"
        if not name_safe.endswith(".xml"): name_safe += ".xml"
        
        # Add a simple hash/timestamp component to ensure uniqueness if filenames clash
        import hashlib
        url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
        filename = f"{domain_safe}_{url_hash}_{name_safe}"
        
        path = os.path.join(folder, filename)
        with open(path, "wb") as f:
            f.write(content)
        return path
    except Exception as e:
        print(f"[!] Error saving file: {e}")
        return None

def process_site(domain, base_dir):
    print(f"\n=== Processing Domain: {domain} ===")
    
    # Prepare folders
    dir_indices = os.path.join(base_dir, "sitemap_indices")
    dir_content = os.path.join(base_dir, "final_content")
    os.makedirs(dir_indices, exist_ok=True)
    os.makedirs(dir_content, exist_ok=True)

    # Queue for recursion
    queue = deque(get_initial_sitemaps(domain))
    visited = set()
    
    count_indices = 0
    count_leafs = 0

    while queue:
        url = queue.popleft()
        
        if url in visited:
            continue
        visited.add(url)
        
        print(f"  -> Fetching: {url}")
        try:
            response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
            if response.status_code != 200:
                print(f"     [!] Failed {response.status_code}")
                continue
                
            content = response.content
            text_content = response.text
            
            # Determine if it is an Index or a Leaf
            if RE_SITEMAP_INDEX.search(text_content):
                # It is a Sitemap Index -> Save to indices, Extract children, Add to queue
                save_path = save_xml(content, url, dir_indices)
                print(f"     [TYPE: INDEX] Saved to {os.path.basename(dir_indices)}/")
                
                # Extract child sitemaps
                children = RE_LOC.findall(text_content)
                new_children = [c.strip() for c in children if c.strip() not in visited]
                print(f"     [+] Found {len(new_children)} nested sitemaps.")
                queue.extend(new_children)
                count_indices += 1
            else:
                # It is a regular sitemap (Leaf) -> Save to final_content
                save_path = save_xml(content, url, dir_content)
                print(f"     [TYPE: LEAF] Saved to {os.path.basename(dir_content)}/")
                count_leafs += 1
                
        except Exception as e:
            print(f"     [!] Error: {e}")

    print(f"Done. Indices: {count_indices}, Final Maps: {count_leafs}")

def main():
    # 1. Setup Date-based Directory
    today = datetime.date.today().isoformat()
    base_dir = os.path.join("sitemaps_archive", today)
    
    print(f"=== Starting Recursive Job for {today} ===")
    print(f"Output Directory: {base_dir}\n")

    # 2. Read Sites
    try:
        with open(SITES_FILE, "r") as f:
            sites = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: {SITES_FILE} not found.")
        sys.exit(1)

    # 3. Process Each Site
    for site in sites:
        process_site(site, base_dir)

if __name__ == "__main__":
    main()
