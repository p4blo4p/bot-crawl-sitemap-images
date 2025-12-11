import os
import sys
import re
import datetime
import difflib
import gzip
import json
import time

# Configuration
DEFAULT_PHRASE = "Dragon Ball"
SEARCH_PHRASE = os.getenv("SEARCH_PHRASE", DEFAULT_PHRASE)
DATA_DIR = "sitemaps_data"
# Stores "filepath": mtime_of_last_scan
SEARCH_STATE_FILE = os.path.join(DATA_DIR, "search_state.json")

FUZZY_THRESHOLD = 0.8
TIME_LIMIT_SECONDS = 40 * 60 # 40 Minutes strict limit
START_TIME = time.time()

RE_EXTRACT_CONTENT = re.compile(r'<(loc|title|image:caption|image:title|news:title|video:title|video:description)[^>]*>(.*?)</\1>', re.IGNORECASE)

def get_elapsed_time():
    return time.time() - START_TIME

def load_search_state():
    if os.path.exists(SEARCH_STATE_FILE):
        try:
            with open(SEARCH_STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"phrase": SEARCH_PHRASE, "scanned": {}}

def save_search_state(state):
    try:
        with open(SEARCH_STATE_FILE, 'w') as f:
            json.dump(state, f)
    except:
        print("[!] Failed to save search state")

def normalize_text(text):
    return text.lower().replace('-', ' ').replace('_', ' ').replace('/', ' ').strip()

def fuzzy_match(query, text):
    query_norm = normalize_text(query)
    text_norm = normalize_text(text)
    if query_norm in text_norm:
        return True, 1.0, "Substring"
    if len(text_norm) < 300: 
        ratio = difflib.SequenceMatcher(None, query_norm, text_norm).ratio()
        if ratio >= FUZZY_THRESHOLD:
            return True, ratio, f"Fuzzy ({int(ratio*100)}%)"
    return False, 0.0, None

def search_files(directory, phrase, state):
    results = []
    scanned_count = 0
    skipped_count = 0
    
    # If phrase changed, reset cache
    if state.get("phrase") != phrase:
        print("[*] Phrase changed. Resetting scan history.")
        state["phrase"] = phrase
        state["scanned"] = {}

    print(f"[*] Searching for '{phrase}' in '{directory}' (Incremental)")
    
    if not os.path.exists(directory):
        print(f"[!] Data directory not found.")
        return [], 0

    for root, dirs, files in os.walk(directory):
        if 'content_raw' in dirs: dirs.remove('content_raw')
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        for file in files:
            if get_elapsed_time() > TIME_LIMIT_SECONDS:
                print("[!] Time limit reached during search.")
                save_search_state(state)
                return results, scanned_count

            if file.endswith(".xml") or file.endswith(".xml.gz"):
                path = os.path.join(root, file)
                mtime = os.path.getmtime(path)
                
                # Check if already scanned and not modified
                last_scan = state["scanned"].get(path, 0)
                if last_scan >= mtime:
                    skipped_count += 1
                    continue

                scanned_count += 1
                if scanned_count % 1000 == 0: print(f"    Scanning [{scanned_count}]...")

                try:
                    open_func = gzip.open if file.endswith(".gz") else open
                    with open_func(path, "rt", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        potential_matches = RE_EXTRACT_CONTENT.findall(content)
                        urls = [p[1] for p in potential_matches if p[0].lower() == 'loc']
                        
                        for tag_type, text in potential_matches:
                            is_match, conf, m_type = fuzzy_match(phrase, text)
                            if is_match:
                                associated_url = text if tag_type == 'loc' else (urls[0] if urls else "No URL")
                                results.append({
                                    "url": associated_url.strip(),
                                    "match_text": text.strip(),
                                    "confidence": conf,
                                    "type": m_type,
                                    "file": file
                                })
                    
                    # Mark as scanned
                    state["scanned"][path] = mtime
                    
                except Exception as e:
                    pass
    
    save_search_state(state)
    print(f"Stats: {scanned_count} new/modified files scanned. {skipped_count} skipped (unchanged).")
    return results, scanned_count

def main():
    state = load_search_state()
    hits, count = search_files(DATA_DIR, SEARCH_PHRASE, state)
    
    print(f"\n=== Summary ===")
    print(f"New Matches Found: {len(hits)}")
    
    today = datetime.date.today().isoformat()
    safe_phrase = re.sub(r'[^\w\-_]', '_', SEARCH_PHRASE)
    
    # Process Hits
    unique_hits = {}
    for h in hits:
        u = h['url']
        if u not in unique_hits or h['confidence'] > unique_hits[u]['confidence']:
            unique_hits[u] = h
    sorted_hits = sorted(unique_hits.values(), key=lambda x: x['confidence'], reverse=True)

    if sorted_hits:
        # APPEND Mode (a) for results to build up over time
        txt_filename = f"results_{safe_phrase}_{today}.txt"
        with open(txt_filename, "a") as f:
             for hit in sorted_hits:
                if hit['url'].startswith('http'):
                    f.write(hit['url'] + "\n")
        print(f"Results appended to: {txt_filename}")

        md_filename = f"results_{safe_phrase}_{today}.md"
        # Check if file exists to add header only once
        new_file = not os.path.exists(md_filename)
        with open(md_filename, "a") as f:
            if new_file:
                f.write(f"# Search Results: {SEARCH_PHRASE} ({today})\n")
                f.write("| Confidence | Match Type | URL / Content | Source |\n")
                f.write("|------------|------------|---------------|--------|\n")
            
            for hit in sorted_hits:
                conf_str = f"{int(hit['confidence']*100)}%"
                url_display = f"[{hit['url']}]({hit['url']})" if hit['url'].startswith('http') else hit['url']
                context = ""
                if hit['match_text'] != hit['url']:
                    context = f"<br/>*Match: {hit['match_text'][:50]}...*"
                f.write(f"| {conf_str} | {hit['type']} | {url_display}{context} | {hit['file']} |\n")
        print(f"Report appended to: {md_filename}")
    else:
        print("No new matches found in this run.")

if __name__ == "__main__":
    main()
