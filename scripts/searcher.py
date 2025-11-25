import os
import sys
import re
import datetime

# Configuration
DEFAULT_PHRASE = "Black Friday Sale"
SEARCH_PHRASE = os.getenv("SEARCH_PHRASE", DEFAULT_PHRASE)
DATA_DIR = "sitemaps_data" # Matches downloader

# Regex to extract URLs from XML content
RE_LOC = re.compile(r'<loc>(.*?)</loc>', re.IGNORECASE)

def search_files(directory, phrase):
    results = []
    scanned_count = 0
    
    print(f"[*] Searching for '{phrase}' in '{directory}' (Optimized)...")
    
    if not os.path.exists(directory):
        print(f"[!] Data directory '{directory}' not found.")
        return [], 0

    for root, dirs, files in os.walk(directory):
        # OPTIMIZATION: Skip hidden folders AND 'content_raw' folders
        if 'content_raw' in dirs:
            dirs.remove('content_raw')
        
        # Also skip git
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        for file in files:
            if file.endswith(".xml") or file.endswith(".txt"):
                scanned_count += 1
                path = os.path.join(root, file)
                
                if scanned_count % 200 == 0:
                    print(f"    Scanning [{scanned_count}]: {file}...")

                try:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read() # Read as original case first for URL extraction
                        
                        # Extract all URLs first
                        urls = RE_LOC.findall(content)
                        if not urls:
                            # Fallback for plain text lists (if any)
                            urls = [w for w in content.split() if w.startswith('http')]

                        phrase_lower = phrase.lower()
                        
                        for url in urls:
                            url_lower = url.lower()
                            match_found = False
                            match_type = ""

                            # 1. Exact Match on the URL string
                            if phrase_lower in url_lower:
                                match_found = True
                                match_type = "Exact"
                            else:
                                # 2. Normalized Match (Slugs)
                                # Replaces -, _, / with spaces. "dragon-ball" -> "dragon ball"
                                normalized = url_lower.replace('-', ' ').replace('_', ' ').replace('/', ' ')
                                if phrase_lower in normalized:
                                    match_found = True
                                    match_type = "Normalized (Slug)"

                            if match_found:
                                print(f"    >>> MATCH [{match_type}]: {url}")
                                # Store ONLY the URL for gallery-dl compatibility
                                results.append(url)
                            
                except Exception as e:
                    pass
    
    return results, scanned_count

def main():
    today = datetime.date.today().isoformat()
    safe_phrase = re.sub(r'[^\w\-_]', '_', SEARCH_PHRASE)
    
    hits, count = search_files(DATA_DIR, SEARCH_PHRASE)
    
    print(f"\n=== Summary ===")
    print(f"Total Files Scanned (High Value Only): {count}")
    print(f"Matches Found: {len(hits)}")
    
    output_filename = f"results_{safe_phrase}_{today}.txt"
    with open(output_filename, "w") as f:
        if hits:
            # PURE LIST: No headers, no footers, just URLs.
            # Ideally suited for 'gallery-dl -i input.txt'
            unique_hits = sorted(list(set(hits)))
            for hit in unique_hits:
                f.write(hit + "\n")
        else:
             # If empty, write nothing or just a single comment line (which gallery-dl ignores usually)
             pass
            
    print(f"Results saved to: {output_filename}")

if __name__ == "__main__":
    main()
