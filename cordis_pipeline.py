#!/usr/bin/env python3
"""
CORDIS → cordis_enriched.csv pipeline
======================================
Downloads the CORDIS bulk data ZIP (or reads a local copy), filters to
Swiss-involved projects, fetches full project details from CORDIS HTML pages,
and enriches with Mistral Small (keywords + SATW 2025 tags).

Data source
-----------
The Horizon Europe ZIP at:
  https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip

contains two key files:
  project.csv      — all ~22k Horizon Europe projects
  organization.csv — all participating organisations (with country=CH for Swiss)

Usage:
  python3 cordis_pipeline.py --no-llm --sample 20   # test join/filter only
  python3 cordis_pipeline.py --sample 100            # test enrichment (~$0.01)
  python3 cordis_pipeline.py                         # full run
  python3 cordis_pipeline.py --resume                # resume after interruption
  python3 cordis_pipeline.py --zip /path/to/my.zip  # use pre-downloaded ZIP
"""

import argparse, csv, io, json, os, re, sys, time, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    sys.exit("pip install requests")

# ── Constants ─────────────────────────────────────────────────────────────────
HE_ZIP_URL   = "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip"
DEFAULT_ZIP  = "cordis-HORIZONprojects-csv.zip"
OUTPUT_CSV   = "cordis_enriched.csv"
CHECKPOINT   = "cordis_enriched_checkpoint.csv"
BASE_URL     = "https://cordis.europa.eu"
MISTRAL_KEY  = "pvckWV1JQS1Ts9PrAmVOffjxb6VykwIt"
MISTRAL_URL  = "https://api.mistral.ai/v1"
MISTRAL_MDL  = "mistral-small-latest"
MAX_WORKERS  = 6   # parallel HTML fetches

OUTPUT_COLS = [
    "source", "project_id", "rcn", "acronym", "title_en", "objective",
    "keywords", "satw_2025", "confidence", "enrich_status",
    "programme", "pillar", "instrument", "funding_scheme", "call_id", "topics",
    "total_cost", "ec_contribution", "currency", "start_date", "end_date", "status",
    "swiss_organisations", "swiss_is_coordinator", "swiss_ec_contribution",
    "participant_countries", "cordis_url",
    # ARAMIS-compatible aliases
    "Forschungsstelle", "Projektnummer", "Projekttitel englisch",
    "Kurzbeschreibung", "Projektstatus", "Projektstart", "Projektende",
    "Gesamtkosten bewilligt",
]


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="CORDIS → cordis_enriched.csv")
    p.add_argument("--zip",     default=DEFAULT_ZIP,
                   help=f"Path to CORDIS HE ZIP (default: {DEFAULT_ZIP}; auto-downloaded if missing)")
    p.add_argument("--output",  default=OUTPUT_CSV)
    p.add_argument("--sample",  type=int, default=0,
                   help="Only process N Swiss projects (0 = all)")
    p.add_argument("--resume",  action="store_true",
                   help="Skip rows already in checkpoint file")
    p.add_argument("--no-llm", action="store_true",
                   help="Skip Mistral enrichment (fast test mode)")
    return p.parse_args()


# ── Step 1: Ensure ZIP ────────────────────────────────────────────────────────
def ensure_zip(path):
    if os.path.exists(path):
        print(f"Using local ZIP: {path} ({os.path.getsize(path)//1_000_000} MB)")
        return path
    print(f"Downloading CORDIS HE ZIP (~31 MB) → {path} …")
    urllib.request.urlretrieve(HE_ZIP_URL, path)
    print(f"  Done ({os.path.getsize(path)//1_000_000} MB)")
    return path


# ── Step 2: Read ZIP contents ─────────────────────────────────────────────────
def read_csv_from_zip(zf, name, delimiter=";", quotechar='"'):
    """Return list of dicts from a semicolon-delimited CSV inside the ZIP."""
    with zf.open(name) as f:
        text = f.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter, quotechar=quotechar)
    return list(reader)


# ── Step 3: Build Swiss project set ──────────────────────────────────────────
def build_swiss_data(orgs):
    """
    Returns:
      swiss_ids   — set of projectID strings with at least one CH org
      swiss_meta  — dict projectID → {swiss_organisations, swiss_is_coordinator,
                                       swiss_ec_contribution, participant_countries}
    """
    by_project = {}
    for row in orgs:
        pid = row.get("projectID", "").strip()
        if not pid:
            continue
        if pid not in by_project:
            by_project[pid] = []
        by_project[pid].append(row)

    swiss_ids = set()
    swiss_meta = {}

    for pid, rows in by_project.items():
        ch_rows = [r for r in rows if r.get("country", "").strip() == "CH"]
        if not ch_rows:
            continue
        swiss_ids.add(pid)

        ch_names = sorted(set(r.get("name", "").strip() for r in ch_rows if r.get("name", "").strip()))
        is_coord = any(r.get("role", "").upper() in ("COORDINATOR", "COORD") for r in ch_rows)
        try:
            ch_contrib = sum(float(r.get("ecContribution", 0) or 0) for r in ch_rows)
        except Exception:
            ch_contrib = 0.0

        all_countries = sorted(set(r.get("country", "").strip() for r in rows if r.get("country", "").strip()))

        swiss_meta[pid] = {
            "swiss_organisations":    "; ".join(ch_names),
            "swiss_is_coordinator":   str(is_coord),
            "swiss_ec_contribution":  str(round(ch_contrib, 2)),
            "participant_countries":  "; ".join(all_countries),
        }

    return swiss_ids, swiss_meta


# ── Step 4: Fetch full project details from CORDIS HTML ──────────────────────
def fetch_project_html(rcn, retries=3):
    url = f"{BASE_URL}/project/rcn/{rcn}"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; CORDIS-research-pipeline/1.0)"}
    for attempt in range(retries):
        try:
            time.sleep(0.3 + attempt * 1.5)
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception as e:
            if attempt == retries - 1:
                return None
    return None


def parse_project_html(html):
    """
    Extract objective text from the SSR-rendered CORDIS project page.
    Returns {"objective": str}
    """
    if not html:
        return {"objective": ""}

    # Remove scripts / styles
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL)
    html = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.DOTALL)

    # Objective section appears after an "Objective" heading
    obj_m = re.search(
        r'Objective\s*</[^>]+>\s*(.*?)(?=<h[23\b]|Funding Scheme|<section|Participants)',
        html, re.DOTALL
    )
    if obj_m:
        raw = obj_m.group(1)[:8000]
        clean = re.sub(r"<[^>]+>", " ", raw)
        clean = re.sub(r"&amp;", "&", clean)
        clean = re.sub(r"&[a-z]+;", " ", clean)
        clean = re.sub(r"\s+", " ", clean).strip()
        return {"objective": clean}
    return {"objective": ""}


# ── Step 5: Pillar / instrument helpers ──────────────────────────────────────
def derive_pillar(fs, call_id):
    fs, cid = fs.upper(), call_id.upper()
    if any(x in fs or x in cid for x in ["ERC", "MSCA", "RI-", "WIDERA", "INFRA"]):
        return "Pillar I — Excellent Science"
    if any(x in fs or x in cid for x in ["RIA", "IA", "CSA", "JU-", "KDT",
                                           "CLEAN", "HEALTH", "DIGITAL",
                                           "FOOD", "CLIMATE", "CULTURE"]):
        return "Pillar II — Global Challenges"
    if any(x in fs or x in cid for x in ["EIC", "EIT", "INNOV", "EURATOM"]):
        return "Pillar III — Innovative Europe"
    return "Other / Unknown"


def derive_instrument(fs):
    fs = fs.upper()
    if "EIC-ACCELERATOR" in fs or "SMEINST" in fs: return "EIC Accelerator"
    if "EIC-PATHFINDER"  in fs:                    return "EIC Pathfinder"
    if "EIC-TRANSITION"  in fs:                    return "EIC Transition"
    if "ERC-ADG"         in fs:                    return "ERC Advanced Grant"
    if "ERC-COG"         in fs:                    return "ERC Consolidator Grant"
    if "ERC-STG"         in fs:                    return "ERC Starting Grant"
    if "ERC-POC"         in fs:                    return "ERC Proof of Concept"
    if "ERC"             in fs:                    return "ERC"
    if "MSCA-PF"         in fs:                    return "MSCA Postdoctoral Fellowship"
    if "MSCA-DN"         in fs:                    return "MSCA Doctoral Network"
    if "MSCA-SE"         in fs:                    return "MSCA Staff Exchange"
    if "MSCA"            in fs:                    return "MSCA"
    if fs == "RIA":                                return "Research & Innovation Action"
    if fs == "IA":                                 return "Innovation Action"
    if fs == "CSA":                                return "Coordination & Support"
    if "COFUND"          in fs:                    return "COFUND"
    return fs if fs else "Unknown"


STATUS_MAP = {
    "SIGNED":               "In Bearbeitung",
    "UNDER_IMPLEMENTATION": "In Bearbeitung",
    "CLOSED":               "Abgeschlossen",
    "TERMINATED":           "Abgeschlossen",
}


# ── Step 6: LLM enrichment ───────────────────────────────────────────────────
SATW_CATEGORIES = [
    "Quantum computing", "Internet of Things", "Industry 5.0",
    "PICs (photonic integrated circuits)", "URLLC",
    "Artificial photosynthesis", "Deep geothermal energy", "Hydrogen",
    "Negative emissions technologies", "Phosphorus recycling",
    "Plastics recycling", "Synfuels",
    "2D materials", "Bioplastics from waste", "CO2-based plastics",
    "CO2-reduced concrete", "Diamond-based photonics", "Fibre-optic sensors",
    "Flexible batteries", "Perovskite", "Plasma technologies",
    "Sustainable adhesives and sealants", "TIMs (thermal interface materials)",
    "Bacteriophages", "Biocatalysis", "Bioinspiration and biointegration",
    "Human augmentation", "mRNA", "Personalised nutrition", "Synthetic biology",
    "Earth observation",
]

SYSTEM_PROMPT = f"""You are a scientific research analyst for EU-funded projects.
Given a project title and objective (already in English), return a JSON object
with exactly two fields:
  "keywords" : A JSON array of 5-8 English keyword strings (lowercase, specific
               noun phrases). Cover the core technology, method, application
               domain, and any relevant material or organism.
  "satw_2025": A JSON array of matching SATW Technology Outlook 2025 categories.
               STRICT RULES — only assign if the project's PRIMARY technology
               directly matches. Do not assign based on tangential overlap.
               "Hydrogen" must relate to H2 as energy carrier (fuel cells,
               electrolysis), NOT hydrogen bonds or hydrogenation chemistry.
               "Plasma technologies" = plasma surface treatment/coating, NOT
               blood plasma or medical plasma.
               "Human augmentation" requires exoskeletons, BCIs, or
               neuroprosthetics as PRIMARY focus.
               "Industry 5.0" only for explicit smart manufacturing.
               When in doubt, return []. Precision over recall.
SATW 2025 categories:
{', '.join(SATW_CATEGORIES)}

Return ONLY the JSON object. No preamble, no markdown fences."""


def call_mistral(title, objective):
    user_msg = f"Title: {title}\n\nObjective: {objective[:1500]}"
    payload = {
        "model": MISTRAL_MDL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        "temperature": 0.1,
        "max_tokens": 300,
    }
    headers = {
        "Authorization": f"Bearer {MISTRAL_KEY}",
        "Content-Type": "application/json",
    }
    for attempt in range(3):
        try:
            r = requests.post(f"{MISTRAL_URL}/chat/completions",
                              json=payload, headers=headers, timeout=30)
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            parsed = json.loads(text)
            kws  = parsed.get("keywords", [])
            satw = [s for s in parsed.get("satw_2025", []) if s in SATW_CATEGORIES]
            return ("; ".join(kws), "; ".join(satw), "ok")
        except requests.HTTPError:
            if r.status_code == 429:
                time.sleep(60 * (attempt + 1))
            elif r.status_code in (500, 502, 503):
                time.sleep(5 * (attempt + 1))
            else:
                return ("", "", f"failed:http{r.status_code}")
        except json.JSONDecodeError:
            return ("", "", "failed:json")
        except Exception as e:
            if attempt == 2:
                return ("", "", f"failed:{type(e).__name__}")
            time.sleep(3)
    return ("", "", "failed:retries")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    # ── Step 1: Ensure ZIP ──────────────────────────────────────────────────
    zip_path = ensure_zip(args.zip)

    # ── Step 2: Read ZIP ────────────────────────────────────────────────────
    print("\nReading ZIP contents…")
    with zipfile.ZipFile(zip_path) as zf:
        print("  Loading project.csv…", end=" ", flush=True)
        projects = read_csv_from_zip(zf, "project.csv")
        print(f"{len(projects):,} rows")

        print("  Loading organization.csv…", end=" ", flush=True)
        orgs = read_csv_from_zip(zf, "organization.csv")
        print(f"{len(orgs):,} rows")

    # ── Step 3: Swiss filter ────────────────────────────────────────────────
    print("\nFiltering Swiss projects…")
    swiss_ids, swiss_meta = build_swiss_data(orgs)
    print(f"  {len(swiss_ids):,} Horizon Europe projects with ≥1 Swiss participant")

    # Keep only Swiss projects
    swiss_projects = [p for p in projects if p.get("id", "").strip() in swiss_ids]
    print(f"  {len(swiss_projects):,} matched projects found in project.csv")

    if args.sample > 0:
        swiss_projects = swiss_projects[:args.sample]
        print(f"  Sampling {len(swiss_projects)} projects")

    # ── Step 4: Resume ──────────────────────────────────────────────────────
    done_ids = set()
    if args.resume and os.path.exists(CHECKPOINT):
        with open(CHECKPOINT, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done_ids.add(row.get("project_id", ""))
        print(f"  Resuming: {len(done_ids):,} already done")

    todo = [p for p in swiss_projects if p.get("id", "").strip() not in done_ids]
    print(f"  {len(todo):,} to process")

    if not todo:
        print("Nothing to do.")
        _merge_checkpoint(swiss_projects, swiss_meta)
        return

    # ── Step 5: Open checkpoint for writing ─────────────────────────────────
    write_header = not os.path.exists(CHECKPOINT) or len(done_ids) == 0
    ckpt_f = open(CHECKPOINT, "a", encoding="utf-8", newline="")
    writer = csv.DictWriter(ckpt_f, fieldnames=OUTPUT_COLS, extrasaction="ignore")
    if write_header:
        writer.writeheader()

    # ── Step 6: Fetch HTML + enrich ─────────────────────────────────────────
    print(f"\nFetching project details + enriching ({len(todo):,} projects, {MAX_WORKERS} workers)…")
    n_done = 0
    start_t = time.time()

    def process_one(proj):
        pid = proj.get("id", "").strip()
        rcn = proj.get("rcn", "").strip()
        title = proj.get("title", "").strip()
        fs    = proj.get("fundingScheme", "").strip()
        call  = proj.get("call", "").strip()

        # Fetch HTML for full objective
        html = fetch_project_html(rcn) if rcn else None
        parsed = parse_project_html(html)
        objective = parsed["objective"]

        # LLM enrichment
        if not args.no_llm and (title or objective):
            kws, satw, status = call_mistral(title, objective)
        else:
            kws, satw, status = "", "", "skipped"

        conf = 1.0 if len(objective) > 200 else (0.7 if objective else 0.3)

        meta = swiss_meta.get(pid, {})

        row = {
            "source":                "CORDIS HE",
            "project_id":            pid,
            "rcn":                   rcn,
            "acronym":               proj.get("acronym", ""),
            "title_en":              title,
            "objective":             objective,
            "keywords":              kws,
            "satw_2025":             satw,
            "confidence":            str(conf),
            "enrich_status":         status,
            "programme":             "Horizon Europe",
            "pillar":                derive_pillar(fs, call),
            "instrument":            derive_instrument(fs),
            "funding_scheme":        fs,
            "call_id":               call,
            "topics":                proj.get("topics", ""),
            "total_cost":            proj.get("totalCost", ""),
            "ec_contribution":       proj.get("ecMaxContribution", ""),
            "currency":              "EUR",
            "start_date":            proj.get("startDate", ""),
            "end_date":              proj.get("endDate", ""),
            "status":                STATUS_MAP.get(proj.get("status", "").upper(), proj.get("status", "")),
            "swiss_organisations":   meta.get("swiss_organisations", ""),
            "swiss_is_coordinator":  meta.get("swiss_is_coordinator", ""),
            "swiss_ec_contribution": meta.get("swiss_ec_contribution", ""),
            "participant_countries": meta.get("participant_countries", ""),
            "cordis_url":            f"{BASE_URL}/project/rcn/{rcn}" if rcn else "",
            # ARAMIS-compatible aliases
            # NB: "Forschungsstelle" maps to the ARAMIS "Funder" badge/filter —
            # for CORDIS that's the EU programme, NOT the Swiss beneficiary
            # (which lives in swiss_organisations and is shown as the institution).
            "Forschungsstelle":            "European Commission (Horizon Europe)",
            "Projektnummer":               pid,
            "Projekttitel englisch":       title,
            "Kurzbeschreibung":            objective[:500],
            "Projektstatus":               STATUS_MAP.get(proj.get("status", "").upper(), proj.get("status", "")),
            "Projektstart":                proj.get("startDate", ""),
            "Projektende":                 proj.get("endDate", ""),
            "Gesamtkosten bewilligt":      proj.get("ecMaxContribution", ""),
        }
        return row

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_one, p): p for p in todo}
        for fut in as_completed(futures):
            try:
                row = fut.result()
            except Exception as e:
                proj = futures[fut]
                row = {k: "" for k in OUTPUT_COLS}
                row["project_id"] = proj.get("id", "")
                row["enrich_status"] = f"error:{e}"

            writer.writerow(row)
            ckpt_f.flush()
            n_done += 1

            if n_done % 20 == 0 or n_done <= 5:
                elapsed = time.time() - start_t
                rate = n_done / elapsed if elapsed > 0 else 0
                eta_h = (len(todo) - n_done) / rate / 3600 if rate > 0 else 0
                print(f"  [{n_done:>5,}/{len(todo):,}] {rate:.2f}/s  ~{eta_h:.1f}h left"
                      f"  last: {row.get('acronym') or row.get('project_id')}", flush=True)

    ckpt_f.close()
    _merge_checkpoint(swiss_projects, swiss_meta)


def _merge_checkpoint(swiss_projects, swiss_meta):
    print(f"\nMerging checkpoint → {OUTPUT_CSV}…")
    if not os.path.exists(CHECKPOINT):
        print("  No checkpoint found.")
        return

    ckpt = {}
    with open(CHECKPOINT, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ckpt[row.get("project_id", "")] = row

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLS, extrasaction="ignore")
        w.writeheader()
        for proj in swiss_projects:
            pid = proj.get("id", "").strip()
            if pid in ckpt:
                w.writerow(ckpt[pid])

    satw_n = sum(1 for r in ckpt.values() if r.get("satw_2025"))
    obj_n  = sum(1 for r in ckpt.values() if r.get("objective"))
    print(f"  Output: {len(ckpt):,} projects")
    print(f"  With SATW tags:  {satw_n:,}")
    print(f"  With objective:  {obj_n:,}")
    print(f"\nDone → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
