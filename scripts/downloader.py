import requests
import os
import sys
import datetime
from urllib.parse import urljoin, urlparse

# Configuration
SITES_FILE = "sites.txt"
USER_AGENT = "Mozilla/5.0 (compatible; SitemapDownloader/1.0)"

def get_sitemaps_from_robots(domain):
    """Extracts sitemap URLs from robots.txt."""
    if not domain.startswith("http"):
        domain = "https://" + domain
        
    robots_url = urljoin(domain, "/robots.txt")
    sitemaps = []
    try:
        print(f"[*] Checking {robots_url}...")
        response = requests.get(robots_url, headers={"User-Agent": USER_AGENT}, timeout=10)
        if response.status_code == 200:
            for line in response.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sitemap_url = line.split(":", 1)[1].strip()
                    sitemaps.append(sitemap_url)
    except Exception as e:
        print(f"[!] Error fetching robots.txt for {domain}: {e}")
    
    # Fallback
    if not sitemaps:
        print(f"[-] No sitemaps found in robots.txt for {domain}, trying default.")
        sitemaps.append(urljoin(domain, "/sitemap.xml"))
        
    return sitemaps

def save_sitemap(url, folder):
    """Downloads and saves a sitemap XML file."""
    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        if response.status_code == 200:
            # Create a safe filename: domain_filename.xml
            parsed = urlparse(url)
            domain_safe = parsed.netloc.replace(".", "_").replace(":", "")
            name_safe = os.path.basename(parsed.path)
            if not name_safe.endswith(".xml"):
                name_safe += ".xml"
            
            filename = f"{domain_safe}_{name_safe}"
            path = os.path.join(folder, filename)
            
            with open(path, "wb") as f:
                f.write(response.content)
            print(f"[+] Saved: {path}")
            return True
    except Exception as e:
        print(f"[!] Failed to download {url}: {e}")
    return False

def main():
    # 1. Setup Date-based Directory
    today = datetime.date.today().isoformat()
    download_dir = os.path.join("sitemaps_archive", today)
    os.makedirs(download_dir, exist_ok=True)
    
    print(f"=== Starting Download Job for {today} ===")
    print(f"Output Directory: {download_dir}\n")

    # 2. Read Sites
    try:
        with open(SITES_FILE, "r") as f:
            sites = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: {SITES_FILE} not found.")
        sys.exit(1)

    # 3. Process
    for site in sites:
        print(f"\n--- Processing {site} ---")
        sitemap_urls = get_sitemaps_from_robots(site)
        
        for sm_url in sitemap_urls:
            save_sitemap(sm_url, download_dir)

if __name__ == "__main__":
    main()
