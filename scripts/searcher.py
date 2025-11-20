import os
import sys
import datetime
import re

# Configuration
# If SEARCH_PHRASE env var is set (via GitHub Actions Input), use it. 
# Otherwise use the default hardcoded phrase.
DEFAULT_PHRASE = "Black Friday Sale"
SEARCH_PHRASE = os.getenv("SEARCH_PHRASE", DEFAULT_PHRASE)
SEARCH_DIR = "sitemaps_archive"

def search_files(directory, phrase):
    results = []
    print(f"[*] Searching for '{phrase}' in {directory}...")
    
    if not os.path.exists(directory):
        print(f"[!] Directory {directory} does not exist. Did the download job run?")
        return [], 0

    scanned_count = 0
    # Walk through the date-based structure
    for root, dirs, files in os.walk(directory):
        # Skip hidden directories like .git
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        for file in files:
            if file.endswith(".xml") or file.endswith(".txt"):
                scanned_count += 1
                path = os.path.join(root, file)
                
                # Verbose logging to prove we are searching
                try:
                    size = os.path.getsize(path)
                    print(f"[{scanned_count}] Scanning: {file} ({size} bytes)")
                    
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        if phrase.lower() in content.lower():
                            print(f"   >>> MATCH FOUND in {file}!")
                            results.append(f"File: {file}\nPath: {path}\nMatch: Found phrase inside content\n" + "-"*30)
                except Exception as e:
                    print(f"[!] Error reading {path}: {e}")
    
    if scanned_count == 0:
        print("[!] WARNING: No files found to scan! The directory exists but contains no .xml or .txt files.")
        
    return results, scanned_count

def main():
    # We assume the directory structure is preserved from the artifact
    # sitemaps_archive/YYYY-MM-DD/file.xml
    
    today = datetime.date.today().isoformat()
    
    hits, count = search_files(SEARCH_DIR, SEARCH_PHRASE)
    
    # Save Results
    safe_phrase = re.sub(r'[^\w\-_]', '_', SEARCH_PHRASE)
    output_filename = f"results_{safe_phrase}_{today}.txt"
    
    print(f"\n=== Summary ===")
    print(f"Total files scanned: {count}")
    
    with open(output_filename, "w") as f:
        if hits:
            f.write(f"Search Results for '{SEARCH_PHRASE}' on {today}\n")
            f.write("="*50 + "\n\n")
            for hit in hits:
                f.write(hit + "\n")
            print(f"[+] {len(hits)} matches found. Saved to {output_filename}")
        else:
            msg = f"No matches found for '{SEARCH_PHRASE}' in {count} scanned files."
            f.write(msg + "\n")
            print(msg)

if __name__ == "__main__":
    main()
