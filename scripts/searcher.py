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
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuración Pro ---
DEFAULT_PHRASE = "Dragon Ball"
SEARCH_PHRASE = os.getenv("SEARCH_PHRASE", DEFAULT_PHRASE)
DATA_DIR = "sitemaps_data"
SEARCH_STATE_FILE = os.path.join(DATA_DIR, "search_state.json")
LOG_FILE = "searcher.log"

MAX_WORKERS = 8  # Procesamiento paralelo para máxima velocidad
FUZZY_THRESHOLD = 0.85
TIME_LIMIT_SECONDS = 50 * 60 
START_TIME = time.time()

# Configuración de Logging
sys.stdout.reconfigure(line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE)]
)
logger = logging.getLogger(__name__)

# Regex para extraer contenido relevante de sitemaps (namespaces incluidos)
RE_CONTENT_BLOCKS = re.compile(r'<(loc|title|image:caption|image:title|news:title|video:title|video:description|video:tag)[^>]*>(.*?)</\1>', re.IGNORECASE | re.DOTALL)

def slugify(text):
    """Normaliza texto para comparación de URLs y slugs."""
    if not text: return ""
    text = unquote(text).lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text).strip()
    return text

def normalize_strict(text):
    """Normalización extrema para ignorar separadores."""
    return slugify(text).replace(' ', '')

def advanced_match(query, target):
    """
    Lógica de coincidencia multinivel de alto rendimiento.
    """
    q_slug = slugify(query)
    t_slug = slugify(target)
    if not q_slug or not t_slug: return False, 0, None

    # 1. Match Directo de Términos
    if q_slug in t_slug:
        return True, 1.0, "Direct"

    # 2. Match de Términos Colapsados (ej: dragonball == dragon ball)
    if normalize_strict(query) in normalize_strict(target):
        return True, 0.95, "Collapsed"

    # 3. Match Difuso para variaciones menores
    if len(t_slug) < (len(q_slug) * 5):
        ratio = difflib.SequenceMatcher(None, q_slug, t_slug).ratio()
        if ratio >= FUZZY_THRESHOLD:
            return True, ratio, f"Fuzzy ({int(ratio*100)}%)"

    return False, 0, None

def process_single_file(path, phrase):
    """Procesa un solo archivo sitemap y devuelve los hallazgos."""
    results = []
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
            content = f.read()
            blocks = RE_CONTENT_BLOCKS.findall(content)
            
            current_url = "N/A"
            for tag, text in blocks:
                tag_clean = tag.lower()
                if tag_clean == 'loc':
                    current_url = text.strip()
                
                is_hit, conf, m_type = advanced_match(phrase, text)
                if is_hit:
                    results.append({
                        "url": current_url,
                        "tag": tag,
                        "text": text.strip(),
                        "conf": conf,
                        "type": m_type,
                        "file": os.path.basename(path)
                    })
        return path, results, True
    except Exception as e:
        return path, [], False

def load_search_state():
    if os.path.exists(SEARCH_STATE_FILE):
        try:
            with open(SEARCH_STATE_FILE, 'r') as f:
                return json.load(f)
        except: pass
    return {"phrase": SEARCH_PHRASE, "scanned_files": {}}

def save_search_state(state):
    try:
        with open(SEARCH_STATE_FILE + ".tmp", 'w') as f:
            json.dump(state, f, indent=2)
        os.replace(SEARCH_STATE_FILE + ".tmp", SEARCH_STATE_FILE)
    except Exception as e:
        logger.error(f"Error guardando estado: {e}")

def main():
    logger.info(f"=== INICIANDO MOTOR DE BÚSQUEDA PRO: {SEARCH_PHRASE} ===")
    state = load_search_state()
    
    # Manejo de cambio de frase: si la frase es distinta, resetear historial
    if state.get("phrase") != SEARCH_PHRASE:
        logger.info(f"Frase cambiada de '{state.get('phrase')}' a '{SEARCH_PHRASE}'. Forzando re-escaneo.")
        state = {"phrase": SEARCH_PHRASE, "scanned_files": {}}

    def signal_handler(sig, frame):
        logger.warning("Señal de terminación recibida. Guardando estado y saliendo...")
        save_search_state(state)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    all_files_to_scan = []
    for root, _, files in os.walk(DATA_DIR):
        for file in files:
            if not file.endswith(".xml.gz"): continue
            path = os.path.join(root, file)
            mtime = os.path.getmtime(path)
            if state["scanned_files"].get(path, 0) < mtime:
                all_files_to_scan.append(path)

    if not all_files_to_scan:
        logger.info("No hay archivos nuevos para escanear.")
        return

    logger.info(f"Escaneando {len(all_files_to_scan)} archivos sitemaps en paralelo...")
    total_hits = []
    scanned_successfully = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_file, path, SEARCH_PHRASE): path for path in all_files_to_scan}
        for future in as_completed(futures):
            path = futures[future]
            if time.time() - START_TIME > TIME_LIMIT_SECONDS:
                logger.warning("Límite de tiempo alcanzado. Deteniendo procesamiento paralelo.")
                break
                
            path, file_hits, success = future.result()
            if success:
                scanned_successfully += 1
                total_hits.extend(file_hits)
                state["scanned_files"][path] = os.path.getmtime(path)
            
            if scanned_successfully % 100 == 0:
                logger.info(f"Progreso: {scanned_successfully}/{len(all_files_to_scan)} archivos...")

    if total_hits:
        # Deduplicación y ordenación por relevancia
        unique_results = {}
        for h in total_hits:
            key = h['url']
            if key not in unique_results or h['conf'] > unique_results[key]['conf']:
                unique_results[key] = h
        
        final_results = sorted(unique_results.values(), key=lambda x: x['conf'], reverse=True)
        today = datetime.date.today().isoformat()
        safe_phrase = re.sub(r'[^a-z0-9]', '_', SEARCH_PHRASE.lower())

        # Exportar TXT (Solo URLs únicas)
        txt_filename = f"hits_{safe_phrase}_{today}.txt"
        with open(txt_filename, "w") as f:
            for h in final_results: f.write(f"{h['url']}\n")

        # Exportar MD (Reporte enriquecido)
        md_filename = f"report_{safe_phrase}_{today}.md"
        with open(md_filename, "w") as f:
            f.write(f"# Informe de Rastreo: {SEARCH_PHRASE}\n\n")
            f.write(f"- **Fecha:** {today}\n")
            f.write(f"- **Archivos procesados:** {scanned_successfully}\n")
            f.write(f"- **Hallazgos únicos:** {len(final_results)}\n\n")
            f.write(f"| Confianza | Tipo | Etiqueta | Coincidencia | Fuente | Enlace |\n")
            f.write(f"|---|---|---|---|---|---|\n")
            for h in final_results:
                txt_snippet = (h['text'][:50] + '...') if len(h['text']) > 50 else h['text']
                txt_snippet = txt_snippet.replace('|', '\\|').replace('\n', ' ')
                f.write(f"| {int(h['conf']*100)}% | {h['type']} | `{h['tag']}` | {txt_snippet} | {h['file']} | [Abrir URL]({h['url']}) |\n")
        
        logger.info(f"Búsqueda finalizada. Hallazgos: {len(final_results)}. Reportes: {txt_filename}, {md_filename}")
    else:
        logger.info("No se encontraron coincidencias en los nuevos archivos.")

    save_search_state(state)

if __name__ == "__main__":
    main()
