import os
import sys
import re
import datetime

# Configuration
DEFAULT_PHRASE = "Black Friday Sale"
SEARCH_PHRASE = os.getenv("SEARCH_PHRASE", DEFAULT_PHRASE)
DATA_DIR = "sitemaps_data" # Matches downloader

def search_files(directory, phrase):
    results = []
    scanned_count = 0
    
    print(f"[*] Searching for '{phrase}' in '{directory}' (Optimized)...")
    
    if not os.path.exists(directory):
        print(f"[!] Data directory '{directory}' not found.")
        return [], 0

    for root, dirs, files in os.walk(directory):
        # OPTIMIZATION: Skip hidden folders AND 'content_raw' folders
        # content_raw contains sitemaps with no titles/captions (low value)
        if 'content_raw' in dirs:
            # print(f"    [SKIP] Ignoring raw content folder in {root}")
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
                        content = f.read()
                        if phrase.lower() in content.lower():
                            print(f"    >>> MATCH: {file}")
                            results.append(f"File: {file}\nPath: {path}\nMatch: Found phrase inside file.\n" + "-"*30)
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
            f.write(f"Search Results for '{SEARCH_PHRASE}' on {today}\n")
            f.write("="*50 + "\n\n")
            for hit in hits:
                f.write(hit + "\n")
        else:
            f.write(f"No matches found for '{SEARCH_PHRASE}'. Scanned {count} high-value files.\n")
            
    print(f"Results saved to: {output_filename}")

if __name__ == "__main__":
    main()
