import os
import sys
import re
import datetime
import difflib
import gzip
import json
import time
import logging
import signal

# --- Configuration ---
DEFAULT_PHRASE = "Dragon Ball"
SEARCH_PHRASE = os.getenv("SEARCH_PHRASE", DEFAULT_PHRASE)
DATA_DIR = "sitemaps_data"
SEARCH_STATE_FILE = os.path.join(DATA_DIR, "search_state.json")
LOG_FILE = "searcher.log"

FUZZY_THRESHOLD = 0.8
TIME_LIMIT_SECONDS = 40 * 60  # 40 Minutes
START_TIME = time.time()

# --- Logging Setup ---
# Flush stdout to ensure logs appear immediately in CI environments
sys.stdout.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)

# --- Regex ---
# Added re.DOTALL to match multi-line content inside tags
RE_EXTRACT_CONTENT = re.compile(r'<(loc|title|image:caption|image:title|news:title|video:title|video:description)[^>]*>(.*?)</\1>', re.IGNORECASE | re.DOTALL)

def get_elapsed_time():
    return time.time() - START_TIME

def load_search_state():
    if os.path.exists(SEARCH_STATE_FILE):
        try:
            with open(SEARCH_STATE_FILE, 'r') as f:
                state = json.load(f)
                logger.info(f"Loaded search state. Scanned files history: {len(state.get('scanned', {}))}")
                return state
        except json.JSONDecodeError:
            logger.error("Corrupted search state file. Starting fresh.")
        except Exception as e:
            logger.error(f"Error loading state: {e}")
    
    return {"phrase": SEARCH_PHRASE, "scanned": {}}

def save_search_state(state):
    try:
        # Atomic save
        temp_file = SEARCH_STATE_FILE + ".tmp"
        with open(temp_file, 'w') as f:
            json.dump(state, f, indent=2)
        os.replace(temp_file, SEARCH_STATE_FILE)
        logger.info("Search state saved successfully.")
    except Exception as e:
        logger.error(f"Failed to save search state: {e}")

def normalize_text(text):
    return text.lower().replace('-', ' ').replace('_', ' ').replace('/', ' ').strip()

def fuzzy_match(query, text):
    query_norm = normalize_text(query)
    text_norm = normalize_text(text)
    
    if not text_norm:
        return False, 0.0, None

    if query_norm in text_norm:
        return True, 1.0, "Substring"
        
    if len(text_norm) < 500: 
        ratio = difflib.SequenceMatcher(None, query_norm, text_norm).ratio()
        if ratio >= FUZZY_THRESHOLD:
            return True, ratio, f"Fuzzy ({int(ratio*100)}%)"
            
    return False, 0.0, None

def search_files(directory, phrase, state):
    results = []
    scanned_count = 0
    skipped_count = 0
    errors_count = 0
    
    # State validation
    if state.get("phrase") != phrase:
        logger.warning(f"Search phrase changed from '{state.get('phrase')}' to '{phrase}'. Resetting scan history.")
        state["phrase"] = phrase
        state["scanned"] = {}

    logger.info(f"Starting search for '{phrase}' in '{directory}'")
    
    if not os.path.exists(directory):
        logger.error(f"Data directory '{directory}' not found.")
        return [], 0

    try:
        for root, dirs, files in os.walk(directory):
            # Optimizations
            if '.git' in dirs: dirs.remove('.git')
            if 'content_raw' in dirs: dirs.remove('content_raw') 
            
            for file in files:
                # Time Check
                if get_elapsed_time() > TIME_LIMIT_SECONDS:
                    logger.warning("Time limit reached. Stopping search.")
                    raise TimeoutError("Time limit reached")

                if file.endswith(".xml") or file.endswith(".xml.gz"):
                    path = os.path.join(root, file)
                    
                    try:
                        mtime = os.path.getmtime(path)
                        
                        # Incremental check
                        last_scan = state["scanned"].get(path, 0)
                        if last_scan >= mtime:
                            skipped_count += 1
                            continue

                        scanned_count += 1
                        if scanned_count % 1000 == 0: 
                            logger.info(f"Progress: Scanned {scanned_count} files...")

                        # Process File
                        open_func = gzip.open if file.endswith(".gz") else open
                        with open_func(path, "rt", encoding="utf-8", errors="ignore") as f:
                            content = f.read()
                            
                            potential_matches = RE_EXTRACT_CONTENT.findall(content)
                            # Pre-calculate URLs to associate with metadata
                            urls = [p[1] for p in potential_matches if p[0].lower() == 'loc']
                            first_url = urls[0].strip() if urls else "No URL found"

                            for tag_type, text in potential_matches:
                                is_match, conf, m_type = fuzzy_match(phrase, text)
                                if is_match:
                                    associated_url = text if tag_type == 'loc' else first_url
                                    
                                    results.append({
                                        "url": associated_url.strip(),
                                        "match_text": text.strip(),
                                        "confidence": conf,
                                        "type": m_type,
                                        "file": file
                                    })
                        
                        state["scanned"][path] = mtime

                    except (OSError, EOFError, gzip.BadGzipFile) as e:
                        logger.warning(f"Corrupt/Unreadable file {path}: {e}")
                        errors_count += 1
                    except Exception as e:
                        logger.error(f"Unexpected error processing {path}: {e}")
                        errors_count += 1

    except TimeoutError:
        pass
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    except Exception as e:
        logger.critical(f"Critical crawler crash: {e}", exc_info=True)
    finally:
        logger.info(f"Scan finished. New: {scanned_count}, Skipped: {skipped_count}, Errors: {errors_count}")
        save_search_state(state)

    return results, scanned_count

def write_results(hits, phrase):
    if not hits:
        logger.info("No new matches found.")
        return

    today = datetime.date.today().isoformat()
    safe_phrase = re.sub(r'[^\w\-_]', '_', phrase)
    
    # Deduplicate and Sort
    unique_hits = {}
    for h in hits:
        u = h['url']
        if u not in unique_hits or h['confidence'] > unique_hits[u]['confidence']:
            unique_hits[u] = h
    
    sorted_hits = sorted(unique_hits.values(), key=lambda x: x['confidence'], reverse=True)
    logger.info(f"Writing {len(sorted_hits)} unique matches.")

    # TXT Output
    txt_filename = f"results_{safe_phrase}_{today}.txt"
    try:
        with open(txt_filename, "a") as f:
             for hit in sorted_hits:
                if hit['url'].startswith('http'):
                    f.write(hit['url'] + "\n")
        logger.info(f"Results appended to {txt_filename}")
    except IOError as e:
        logger.error(f"Failed to write TXT results: {e}")

    # MD Output
    md_filename = f"results_{safe_phrase}_{today}.md"
    try:
        new_file = not os.path.exists(md_filename)
        with open(md_filename, "a") as f:
            if new_file:
                f.write(f"# Search Results: {phrase} ({today})\n")
                f.write("| Confidence | Match Type | URL / Content | Source |\n")
                f.write("|------------|------------|---------------|--------|\n")
            
            for hit in sorted_hits:
                conf_str = f"{int(hit['confidence']*100)}%"
                url_display = f"[{hit['url']}]({hit['url']})" if hit['url'].startswith('http') else hit['url']
                context = ""
                if hit['match_text'] != hit['url']:
                    clean_text = hit['match_text'].replace('|', '\|').replace('\n', ' ')[:50]
                    context = f"<br/>*Match: {clean_text}...*"
                f.write(f"| {conf_str} | {hit['type']} | {url_display}{context} | {hit['file']} |\n")
        logger.info(f"Report appended to {md_filename}")
    except IOError as e:
        logger.error(f"Failed to write Markdown results: {e}")

def main():
    logger.info("--- Search Job Started ---")
    state = load_search_state()
    
    # Graceful exit handler
    def signal_handler(sig, frame):
        logger.warning("Signal received, saving state...")
        save_search_state(state)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    hits, count = search_files(DATA_DIR, SEARCH_PHRASE, state)
    
    if hits:
        write_results(hits, SEARCH_PHRASE)
    
    logger.info("--- Search Job Completed ---")

if __name__ == "__main__":
    main()
