import os
import sys
import re
import datetime
import difflib
import gzip

# Configuration
DEFAULT_PHRASE = "Dragon Ball"
SEARCH_PHRASE = os.getenv("SEARCH_PHRASE", DEFAULT_PHRASE)
DATA_DIR = "sitemaps_data" 
FUZZY_THRESHOLD = 0.8 # 80% similarity required

# Regex patterns to extract specific content from tags
RE_EXTRACT_CONTENT = re.compile(r'<(loc|title|image:caption|image:title|news:title|video:title|video:description)[^>]*>(.*?)</\1>', re.IGNORECASE)

def normalize_text(text):
    # Convert 'dragon-ball-super_chapter-1' to 'dragon ball super chapter 1'
    return text.lower().replace('-', ' ').replace('_', ' ').replace('/', ' ').strip()

def fuzzy_match(query, text):
    """
    Returns (is_match, confidence, match_type)
    """
    query_norm = normalize_text(query)
    text_norm = normalize_text(text)
    
    # 1. Exact Substring Match (High Confidence)
    if query_norm in text_norm:
        return True, 1.0, "Substring"
    
    # 2. Fuzzy Match (Parody/Typo detection)
    if len(text_norm) < 300: 
        ratio = difflib.SequenceMatcher(None, query_norm, text_norm).ratio()
        if ratio >= FUZZY_THRESHOLD:
            return True, ratio, f"Fuzzy ({int(ratio*100)}%)"
            
    return False, 0.0, None

def search_files(directory, phrase):
    results = []
    scanned_count = 0
    
    print(f"[*] Searching for '{phrase}' in '{directory}' (Fuzzy > {FUZZY_THRESHOLD*100}%)")
    
    if not os.path.exists(directory):
        print(f"[!] Data directory '{directory}' not found.")
        return [], 0

    for root, dirs, files in os.walk(directory):
        if 'content_raw' in dirs: dirs.remove('content_raw')
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        for file in files:
            # Support both raw XML (legacy) and GZ (new)
            if file.endswith(".xml") or file.endswith(".xml.gz"):
                scanned_count += 1
                path = os.path.join(root, file)
                
                if scanned_count % 1000 == 0: print(f"    Scanning [{scanned_count}]...")

                try:
                    open_func = gzip.open if file.endswith(".gz") else open
                    with open_func(path, "rt", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        
                        potential_matches = RE_EXTRACT_CONTENT.findall(content)
                        
                        # Also extract URLs specifically to report them
                        urls = [p[1] for p in potential_matches if p[0].lower() == 'loc']
                        
                        for tag_type, text in potential_matches:
                            is_match, conf, m_type = fuzzy_match(phrase, text)
                            if is_match:
                                # If we found a match in a title/caption, associate it with the first available URL in the file
                                associated_url = text if tag_type == 'loc' else (urls[0] if urls else "No URL found in sitemap")
                                
                                results.append({
                                    "url": associated_url.strip(),
                                    "match_text": text.strip(),
                                    "confidence": conf,
                                    "type": m_type,
                                    "file": file
                                })
                            
                except Exception as e:
                    # print(f"Error reading {file}: {e}")
                    pass
    
    return results, scanned_count

def main():
    today = datetime.date.today().isoformat()
    safe_phrase = re.sub(r'[^\w\-_]', '_', SEARCH_PHRASE)
    
    hits, count = search_files(DATA_DIR, SEARCH_PHRASE)
    
    print(f"\n=== Summary ===")
    print(f"Files Scanned: {count}")
    print(f"Matches Found: {len(hits)}")
    
    # Deduplicate by URL
    unique_hits = {}
    for h in hits:
        u = h['url']
        if u not in unique_hits or h['confidence'] > unique_hits[u]['confidence']:
            unique_hits[u] = h
            
    sorted_hits = sorted(unique_hits.values(), key=lambda x: x['confidence'], reverse=True)
    
    # 1. Generate TXT (Clean for gallery-dl)
    txt_filename = f"results_{safe_phrase}_{today}.txt"
    with open(txt_filename, "w") as f:
        if sorted_hits:
            for hit in sorted_hits:
                if hit['url'].startswith('http'):
                    f.write(hit['url'] + "\n")
    print(f"TXT saved: {txt_filename}")

    # 2. Generate MD (Detailed Report)
    md_filename = f"results_{safe_phrase}_{today}.md"
    with open(md_filename, "w") as f:
        f.write(f"# Search Results: {SEARCH_PHRASE}\n")
        f.write(f"**Date:** {today} | **Files Scanned:** {count} | **Hits:** {len(sorted_hits)}\n\n")
        
        if sorted_hits:
            f.write("| Confidence | Match Type | URL / Content | Source |\n")
            f.write("|------------|------------|---------------|--------|\n")
            for hit in sorted_hits:
                conf_str = f"{int(hit['confidence']*100)}%"
                url_display = f"[{hit['url']}]({hit['url']})" if hit['url'].startswith('http') else hit['url']
                context = ""
                if hit['match_text'] != hit['url']:
                    context = f"<br/>*Match: {hit['match_text'][:50]}...*"
                    
                f.write(f"| {conf_str} | {hit['type']} | {url_display}{context} | {hit['file']} |\n")
        else:
            f.write("_No matches found._\n")
    print(f"MD saved: {md_filename}")

if __name__ == "__main__":
    main()
