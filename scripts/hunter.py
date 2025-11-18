import requests
import sys
import os
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
import re

# Configuration
SEARCH_PHRASE = "Black Friday Sale"
SITES_FILE = "sites.txt"
USER_AGENT = "Mozilla/5.0 (compatible; SitemapHunter/1.0)"

def get_sitemaps_from_robots(domain):
    """Extracts sitemap URLs from robots.txt."""
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
    
    return sitemaps

def search_sitemap(sitemap_url):
    """Fetches a sitemap and searches for the phrase in the response content."""
    try:
        print(f"  -> Reading Sitemap: {sitemap_url}")
        response = requests.get(sitemap_url, headers={"User-Agent": USER_AGENT}, timeout=15)
        
        if response.status_code == 200:
            # Simple string check (Case insensitive)
            if SEARCH_PHRASE.lower() in response.text.lower():
                print(f"[SUCCESS] Phrase '{SEARCH_PHRASE}' found in {sitemap_url}")
                return True
            else:
                # Optional: Parse XML to check loc tags specifically if needed
                pass
    except Exception as e:
        print(f"[!] Error reading sitemap {sitemap_url}: {e}")
    
    return False

def main():
    try:
        with open(SITES_FILE, "r") as f:
            sites = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: {SITES_FILE} not found.")
        sys.exit(1)

    results = []

    for site in sites:
        # Normalize URL
        if not site.startswith("http"):
            site = "https://" + site
            
        print(f"\n--- Processing {site} ---")
        sitemaps = get_sitemaps_from_robots(site)
        
        if not sitemaps:
            print(f"[-] No sitemaps found in robots.txt for {site}")
            # Fallback: try standard sitemap.xml
            fallback = urljoin(site, "/sitemap.xml")
            sitemaps.append(fallback)

        found = False
        for sm in sitemaps:
            if search_sitemap(sm):
                results.append(f"{site} -> Found in {sm}")
                found = True
                break # Stop checking other sitemaps for this site if found
        
        if not found:
            print(f"[-] Phrase not found for {site}")

    # --- Save Results to File ---
    # Create a safe filename from the search phrase
    safe_phrase = re.sub(r'[^\w\-_]', '_', SEARCH_PHRASE)
    output_filename = f"{safe_phrase}_results.txt"
    
    print("\n=== SUMMARY ===")
    with open(output_filename, "w") as f:
        if results:
            for res in results:
                print(res)
                f.write(res + "\n")
            print(f"\n[+] Results saved to {output_filename}")
        else:
            msg = "Phrase not found in any configured sitemaps."
            print(msg)
            f.write(msg + "\n")

if __name__ == "__main__":
    main()
