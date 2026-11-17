#!/usr/bin/env python3
"""
ARAMIS fast ID-scan scraper — avoids the slow search POST entirely.

Strategy: scan all ProjectIDs 1→MAX_PID as plain GETs to /Beteiligte/?ProjectID=X.
Each Beteiligte page contains the Projektnummer, so we can match it against our CSV.
Then fetch /Texte/?ProjectID=X for matched projects.

Advantages:
  • No ViewState session management needed — pure stateless GETs
  • 404 (invalid ID) responses return in <0.5s so gaps are cheap
  • Can run 30+ workers safely (no shared session state)
  • Resumable via the same checkpoint file as scrape_aramis.py

Usage:
  python3 scrape_aramis_scan.py
"""

import asyncio, csv, os, re, sys, time

try:
    import aiohttp
except ImportError:
    sys.exit("pip install aiohttp")

INPUT_CSV  = "aramis_enriched_full.csv"
OUTPUT_CSV = "aramis_scraped.csv"
CHECKPOINT = "aramis_scraped_checkpoint.csv"

MAX_PID     = 61000   # scan IDs 1 … MAX_PID
CONCURRENCY = 24      # simultaneous HTTP requests (no session state, so safe to go high)
TIMEOUT_S   = 35
BASE_URL    = "https://www.aramis.admin.ch"
HEADERS     = {"User-Agent": "Mozilla/5.0 (compatible; ARAMIS-research-scraper/2.0)"}

NEW_FIELDS = [
    "project_id", "wirtschaftspartner", "wirt_uid_url", "wirt_email",
    "wissensch_leitung", "forschungspartner",
    "texte_abstract_de", "texte_abstract_fr", "texte_abstract_it", "texte_abstract_en",
    "texte_keywords_de", "texte_keywords_en",
]

# ── Parsers ───────────────────────────────────────────────────────────────────

def _cells(html):
    html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)
    html = re.sub(r"<style[^>]*>.*?</style>",  "", html, flags=re.DOTALL)
    raw  = re.findall(r"<td[^>]*>(.*?)</td>", html, re.DOTALL)
    return raw, [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip() for c in raw]


# See scrape_aramis.py for the rationale: Beteiligte rows nest a <table>
# inside each <td>, which breaks a naive global <td>...</td> regex. Pair the
# role label (lblBeteiligungsart) directly with its value span (lblPerson)
# instead, and keep the value as a raw multi-line block (same shape as
# Kontaktperson) for client-side parsing.
_BETEILIGTE_ROW_RE = re.compile(
    r'lblBeteiligungsart"[^>]*>(.*?)</span>.*?lblPerson">(.*?)</span>',
    re.DOTALL,
)


def _clean_text(s):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s)).strip()


def parse_beteiligte(html):
    raw_html = html
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL)
    html = re.sub(r"<style[^>]*>.*?</style>",  " ", html, flags=re.DOTALL)

    r = {k: "" for k in ["wirtschaftspartner", "wirt_uid_url", "wirt_email",
                          "wissensch_leitung", "forschungspartner"]}
    wirt_blocks, forsch_blocks = [], []
    projekt_nr = ""

    clean_text = re.sub(r"<[^>]+>", " ", html)
    m = re.search(r'Projektnummer\s+(\S+)', clean_text)
    if m:
        projekt_nr = m.group(1).strip()

    for role_raw, person_raw in _BETEILIGTE_ROW_RE.findall(html):
        role = _clean_text(role_raw).lower()
        uid_m  = re.search(r'href="(https?://(?:www\.)?uid\.admin\.ch[^"]+)"', person_raw)
        mail_m = re.search(r'href="mailto:([^"]+)"', person_raw)
        lines = [_clean_text(l) for l in re.split(r"<br\s*/?>", person_raw)]
        block = "\n".join(l for l in lines if l)
        if not block:
            continue
        if role == "wirtschaftspartner":
            wirt_blocks.append(block)
            if uid_m and not r["wirt_uid_url"]: r["wirt_uid_url"] = uid_m.group(1)
            if mail_m and not r["wirt_email"]: r["wirt_email"] = mail_m.group(1)
        elif role == "wissenschaftliche leitung":
            if not r["wissensch_leitung"]: r["wissensch_leitung"] = block
        elif role in ("forschungspartner", "umsetzungspartner"):
            forsch_blocks.append(block)

    r["wirtschaftspartner"] = " ||| ".join(wirt_blocks)
    r["forschungspartner"]  = " ||| ".join(forsch_blocks)
    return projekt_nr, r


def parse_texte(html):
    _, clean = _cells(html)
    r = {k: "" for k in ["texte_abstract_de","texte_abstract_fr","texte_abstract_it",
                          "texte_abstract_en","texte_keywords_de","texte_keywords_en"]}
    lang_map = {"deutsch":"de","französisch":"fr","italiano":"it",
                "englisch":"en","english":"en","french":"fr","german":"de"}
    for i, c in enumerate(clean):
        cl = c.lower()
        nxt = clean[i+1] if i+1 < len(clean) else ""
        for lw, lc in lang_map.items():
            if lw not in cl: continue
            if "abstract" in cl:
                r[f"texte_abstract_{lc}"] = nxt
            elif "schlüsselwört" in cl or "keyword" in cl or "mots-clés" in cl:
                k = f"texte_keywords_{lc}"
                if k in r: r[k] = nxt
    return r


# ── HTTP helper ───────────────────────────────────────────────────────────────

async def fetch(session: aiohttp.ClientSession, url: str) -> str | None:
    """Returns HTML string or None on 404. Raises on other errors."""
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)
    for attempt in range(3):
        try:
            async with session.get(url, timeout=timeout) as r:
                if r.status == 404:
                    return None
                r.raise_for_status()
                return await r.text(errors="replace")
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return None
            raise
        except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError,
                aiohttp.ClientOSError):
            if attempt == 2:
                raise
            await asyncio.sleep(2 ** attempt)


# ── Worker ────────────────────────────────────────────────────────────────────

async def process_pid(session, pid, known_nrs, done_nrs, writer, ckpt_f,
                      counters, lock):
    """Fetch Beteiligte for one ProjectID. If it matches a known project,
    also fetch Texte. Thread-safe checkpoint writing via lock."""
    url_b = f"{BASE_URL}/Beteiligte/?ProjectID={pid}"
    try:
        b_html = await fetch(session, url_b)
    except Exception as e:
        async with lock:
            counters["errors"] += 1
        return

    if b_html is None:
        return  # 404 — not a valid ID

    projekt_nr, b_data = parse_beteiligte(b_html)
    if not projekt_nr or projekt_nr not in known_nrs:
        return  # not in our CSV

    if projekt_nr in done_nrs:
        return  # already scraped

    # Fetch Texte
    t_data = {}
    try:
        t_html = await fetch(session, f"{BASE_URL}/Texte/?ProjectID={pid}")
        if t_html:
            t_data = parse_texte(t_html)
    except Exception:
        pass  # abstracts optional

    row = {
        "Projektnummer": projekt_nr,
        "project_id": str(pid),
        **b_data,
        **t_data,
    }

    async with lock:
        writer.writerow(row)
        ckpt_f.flush()
        done_nrs.add(projekt_nr)
        if b_data.get("wirtschaftspartner"):
            counters["wirt"] += 1
        counters["found"] += 1

    return projekt_nr


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    if not os.path.exists(INPUT_CSV):
        sys.exit(f"ERROR: {INPUT_CSV} not found.")

    print(f"Reading {INPUT_CSV}…")
    with open(INPUT_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_rows    = list(reader)
        orig_fields = reader.fieldnames or []
    known_nrs = {r["Projektnummer"] for r in all_rows}
    print(f"  {len(known_nrs):,} projects in CSV")

    done_nrs: set[str] = set()
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done_nrs.add(row["Projektnummer"])
        print(f"  {len(done_nrs):,} already in checkpoint — will skip")

    todo_nrs = known_nrs - done_nrs
    print(f"  {len(todo_nrs):,} remaining\n")

    if not todo_nrs:
        print("Nothing to do — all projects already scraped.")
        _merge(all_rows, orig_fields)
        return

    out_fields = ["Projektnummer"] + NEW_FIELDS
    write_header = not os.path.exists(CHECKPOINT) or len(done_nrs) == 0
    ckpt_f = open(CHECKPOINT, "a", encoding="utf-8", newline="")
    writer = csv.DictWriter(ckpt_f, fieldnames=out_fields, extrasaction="ignore")
    if write_header:
        writer.writeheader()

    lock = asyncio.Lock()
    counters = {"found": 0, "errors": 0, "wirt": 0}
    total_pids = MAX_PID

    connector = aiohttp.TCPConnector(limit=CONCURRENCY + 4, limit_per_host=CONCURRENCY + 4)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def bounded(pid):
            async with sem:
                return await process_pid(session, pid, known_nrs, done_nrs,
                                         writer, ckpt_f, counters, lock)

        start_t = time.time()
        tasks = [asyncio.create_task(bounded(pid)) for pid in range(1, MAX_PID + 1)]

        n_checked = 0
        report_every = max(1, CONCURRENCY * 4)
        for coro in asyncio.as_completed(tasks):
            await coro
            n_checked += 1
            if n_checked % report_every == 0:
                elapsed = time.time() - start_t
                rate_found = counters["found"] / elapsed
                remaining_to_find = len(todo_nrs) - counters["found"]
                # estimate: we're n_checked/total_pids through the ID range
                frac = n_checked / total_pids
                eta_h = (1 - frac) * elapsed / 3600 if frac > 0 else 0
                print(
                    f"[ID scan {n_checked:>6,}/{total_pids:,}  found {counters['found']:,}/{len(todo_nrs):,}]"
                    f"  {rate_found:.2f} found/s  ~{eta_h:.1f}h left"
                    f"  partners: {counters['wirt']}",
                    flush=True,
                )

    ckpt_f.close()
    print(f"\nScan complete. Found {counters['found']:,} of {len(todo_nrs):,} remaining projects.")
    _merge(all_rows, orig_fields)


def _merge(all_rows, orig_fields):
    print(f"Merging into {OUTPUT_CSV}…")
    scraped = {}
    with open(CHECKPOINT, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            scraped[row["Projektnummer"]] = row

    out_fields_full = list(orig_fields) + NEW_FIELDS
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_fields_full, extrasaction="ignore")
        w.writeheader()
        for row in all_rows:
            merged = dict(row)
            s = scraped.get(row["Projektnummer"], {})
            for k in NEW_FIELDS:
                merged[k] = s.get(k, "")
            w.writerow(merged)

    print(f"Done! → {OUTPUT_CSV}")
    print(f"  Projects with Wirtschaftspartner: {sum(1 for v in scraped.values() if v.get('wirtschaftspartner')):,}")
    print(f"  Projects with English abstract:   {sum(1 for v in scraped.values() if v.get('texte_abstract_en')):,}")


if __name__ == "__main__":
    asyncio.run(main())
