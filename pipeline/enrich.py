"""
ARAMIS Enrichment Pipeline
==========================
Translates abstracts to English, extracts keywords, assigns SATW 2025 categories,
and computes a confidence score for every project in the ARAMIS CSV.

Provider: Mistral Small (mistral-small-latest) via La Plateforme
Switch:   Change BASE_URL + MODEL below to use Infomaniak or any OpenAI-compatible API

Usage:
    python enrich.py --input full_export.csv --output aramis_enriched.csv
    python enrich.py --input full_export.csv --output aramis_enriched.csv --sample 500
    python enrich.py --input full_export.csv --output aramis_enriched.csv --resume
"""

import os, sys, json, time, argparse, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

# ── PROVIDER CONFIG ────────────────────────────────────────────────────────────
# Mistral Small (default)
BASE_URL = "https://api.mistral.ai/v1"
MODEL    = "mistral-small-latest"
API_KEY  = os.environ.get("MISTRAL_API_KEY", "")

# ── To switch to Infomaniak, comment out the three lines above and use these: ──
# BASE_URL = "https://api.infomaniak.com/1/ai/{PRODUCT_ID}/openai/v1"  # replace {PRODUCT_ID}
# MODEL    = "mistralai/Ministral-3-14B-Instruct-2512"
# API_KEY  = os.environ.get("INFOMANIAK_API_KEY", "")

# ── To switch to OpenAI GPT-4.1 Nano: ─────────────────────────────────────────
# BASE_URL = "https://api.openai.com/v1"
# MODEL    = "gpt-4.1-nano"
# API_KEY  = os.environ.get("OPENAI_API_KEY", "")
# ──────────────────────────────────────────────────────────────────────────────

RATE_LIMIT_RPM = 60       # Mistral Small free tier; raise to 500+ on paid
BATCH_SIZE     = 20       # rows processed before saving checkpoint
MAX_RETRIES    = 4        # per-row retry attempts on transient errors
RETRY_WAIT     = 8        # seconds between retries

SATW_2025 = [
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
    "Human augmentation", "mRNA", "Personalised nutrition",
    "Synthetic biology", "Earth observation",
]
SATW_LIST_STR = "\n".join(f"- {t}" for t in SATW_2025)

SYSTEM_PROMPT = f"""You are a scientific research analyst specialising in Swiss and European
R&D funding. You receive project metadata in multiple languages (German, French, Italian,
English) and return a JSON object with exactly these four fields:

  "abstract_en"  : A concise English abstract (2–4 sentences, max 80 words).
                   Translate faithfully — do NOT invent content not present in the source.
                   If the source is already in English, clean and condense it.
                   If no abstract is available, write: "No abstract available."

  "keywords"     : A JSON array of 5–8 English keyword strings (single concepts or short
                   noun phrases). Cover the core technology, method, application domain,
                   and any relevant material or organism. All lowercase.

  "satw_2025"    : A JSON array of matching SATW Technology Outlook 2025 category names
                   from the list below.

                   STRICT RULES for assignment:
                   - Only assign a category if the project's PRIMARY technology or research
                     focus directly matches. Do NOT assign based on tangential, incidental,
                     or superficial keyword overlap.
                   - A project about pest control, animal disease, food safety, or ecology
                     is NOT "Synthetic biology" or "Biocatalysis" unless it explicitly
                     develops or applies those technologies as its core method.
                   - A project using genomic sequencing or PCR is NOT "mRNA" unless it
                     specifically develops or applies mRNA-based tools or vaccines.
                   - "Hydrogen" must relate to hydrogen as an energy carrier (fuel cells,
                     electrolysis, H2 storage/production) — NOT to hydrogen bonds,
                     hydrogenation reactions, or hydrogen peroxide.
                   - "Plasma technologies" must relate to plasma surface treatment,
                     coating, or deposition — NOT blood plasma or medical plasma.
                   - "Human augmentation" requires exoskeletons, BCIs, or neuroprosthetics
                     as a primary focus — NOT general medical devices or wearables.
                   - When in doubt, return an empty array. Precision matters more than recall.
                   - Return an empty array [] if no category clearly matches.

                   SATW 2025 categories:
{SATW_LIST_STR}

  "confidence"   : A float 0.0–1.0 indicating how confident you are in the enrichment.
                   Use this scale:
                   1.0 = rich abstract (>200 chars) in any language, clear topic
                   0.8 = short abstract (50–200 chars) or title-only but topic is clear
                   0.6 = title-only, moderate length (>50 chars), topic inferrable
                   0.4 = very short title (<50 chars) or highly ambiguous topic
                   0.2 = only a project number or uninformative stub

Return ONLY the JSON object. No preamble, no markdown fences, no explanation.
"""


def build_user_message(row: pd.Series) -> str:
    parts = []
    for col, label in [
        ("Projekttitel deutsch",     "Title (DE)"),
        ("Projekttitel englisch",    "Title (EN)"),
        ("Projekttitel französisch", "Title (FR)"),
        ("Projekttitel italienisch", "Title (IT)"),
        ("Kurzbeschreibung",         "Abstract"),
        ("Bereich",                  "Field"),
        ("NABS-Kategorie",           "NABS category"),
        ("Forschungsstelle",         "Funding body"),
    ]:
        val = str(row.get(col, "")).strip()
        if val:
            parts.append(f"{label}: {val}")
    return "\n".join(parts) if parts else "No content available."


def compute_confidence_deterministic(row: pd.Series) -> float:
    """Fast heuristic confidence — used as fallback if the model call fails."""
    abstract = str(row.get("Kurzbeschreibung", "")).strip()
    titles = [
        str(row.get("Projekttitel deutsch", "")).strip(),
        str(row.get("Projekttitel englisch", "")).strip(),
        str(row.get("Projekttitel französisch", "")).strip(),
        str(row.get("Projekttitel italienisch", "")).strip(),
    ]
    max_title_len = max((len(t) for t in titles if t), default=0)
    n_langs = sum(1 for t in titles if t)
    ab_len = len(abstract)

    if ab_len >= 200:
        base = 1.0
    elif ab_len >= 50:
        base = 0.8
    elif ab_len > 0:
        base = 0.65
    elif max_title_len >= 50:
        base = 0.6
    elif max_title_len >= 20:
        base = 0.4
    else:
        base = 0.2

    bonus = min(0.05 * (n_langs - 1), 0.1)
    return round(min(base + bonus, 1.0), 2)


def call_model(client: OpenAI, user_msg: str) -> dict:
    """Single API call with JSON parsing. Raises on unrecoverable failure."""
    response = client.chat.completions.create(
        model=MODEL,
        temperature=0.1,
        max_tokens=400,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
    )
    raw = response.choices[0].message.content.strip()
    # strip accidental markdown fences
    raw = re.sub(r"^```json?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def enrich_row(client: OpenAI, row: pd.Series, fallback_conf: float) -> dict:
    """Enrich one row with retries. Returns a safe dict always."""
    user_msg = build_user_message(row)
    for attempt in range(MAX_RETRIES):
        try:
            result = call_model(client, user_msg)
            # validate required keys
            abstract_en = str(result.get("abstract_en", "")).strip()
            keywords    = result.get("keywords", [])
            satw        = result.get("satw_2025", [])
            confidence  = float(result.get("confidence", fallback_conf))
            # ensure lists
            if isinstance(keywords, str):
                keywords = [k.strip() for k in keywords.split(",")]
            if isinstance(satw, str):
                satw = [s.strip() for s in satw.split(",") if s.strip()]
            # filter satw to valid categories only
            satw = [s for s in satw if s in SATW_2025]
            return {
                "abstract_en": abstract_en or "No abstract available.",
                "keywords":    "; ".join(keywords[:8]),
                "satw_2025":   "; ".join(satw),
                "confidence":  round(min(max(confidence, 0.0), 1.0), 2),
                "enrich_status": "ok",
            }
        except json.JSONDecodeError as e:
            print(f"    JSON parse error (attempt {attempt+1}): {e}")
        except Exception as e:
            code = getattr(getattr(e, "response", None), "status_code", None)
            if code == 429:
                wait = RETRY_WAIT * (attempt + 1)
                print(f"    Rate limit — waiting {wait}s …")
                time.sleep(wait)
            elif code and code >= 500:
                print(f"    Server error {code} (attempt {attempt+1}) — retrying …")
                time.sleep(RETRY_WAIT)
            else:
                print(f"    Unexpected error (attempt {attempt+1}): {e}")
                if attempt == MAX_RETRIES - 1:
                    break
                time.sleep(2)

    # all retries exhausted — return deterministic fallback
    return {
        "abstract_en":   "Enrichment failed.",
        "keywords":      "",
        "satw_2025":     "",
        "confidence":    fallback_conf,
        "enrich_status": "failed",
    }


def load_checkpoint(output_path: Path) -> pd.DataFrame | None:
    if output_path.exists():
        print(f"  Found checkpoint: {output_path} — resuming from last save.")
        return pd.read_csv(output_path, dtype=str, keep_default_na=False)
    return None


def main():
    parser = argparse.ArgumentParser(description="Enrich ARAMIS projects via LLM")
    parser.add_argument("--input",   required=True, help="Path to full_export.csv")
    parser.add_argument("--output",  required=True, help="Path for enriched output CSV")
    parser.add_argument("--sample",  type=int, default=0,
                        help="Only process N rows (0 = all). Use 500 for a test run.")
    parser.add_argument("--resume",  action="store_true",
                        help="Resume from existing output file (skip already-done rows)")
    args = parser.parse_args()

    if not API_KEY:
        print("ERROR: Set the MISTRAL_API_KEY environment variable before running.")
        print("  export MISTRAL_API_KEY=your_key_here")
        sys.exit(1)

    # ── Load data ───────────────────────────────────────────────────────────────
    print(f"\nLoading {args.input} …")
    df = pd.read_csv(args.input, dtype=str, keep_default_na=False)
    print(f"  {len(df):,} projects loaded.")

    if args.sample > 0:
        df = df.head(args.sample)
        print(f"  Sample mode: processing first {args.sample} rows only.")

    output_path = Path(args.output)

    # ── Resume logic ────────────────────────────────────────────────────────────
    done_ids: set = set()
    if args.resume:
        existing = load_checkpoint(output_path)
        if existing is not None:
            done_mask = existing["enrich_status"].isin(["ok", "failed"])
            done_ids  = set(existing.loc[done_mask, "Projektnummer"].tolist())
            print(f"  Already enriched: {len(done_ids):,} rows — skipping.")

    # add new columns with defaults
    for col in ["abstract_en", "keywords", "satw_2025", "confidence", "enrich_status"]:
        if col not in df.columns:
            df[col] = ""

    # mark previously done rows
    if done_ids:
        df.loc[df["Projektnummer"].isin(done_ids), "enrich_status"] = "skip"

    todo = df[df["enrich_status"] == ""].index.tolist()
    print(f"  Rows to enrich: {len(todo):,}")

    if not todo:
        print("Nothing to do — all rows already enriched.")
        return

    # ── Estimate cost ───────────────────────────────────────────────────────────
    avg_in_chars  = df.loc[todo, ["Projekttitel deutsch","Projekttitel englisch",
                                   "Projekttitel französisch","Kurzbeschreibung"]]\
                      .apply(lambda r: sum(len(v) for v in r), axis=1).mean()
    est_in_tok    = (avg_in_chars / 4 + 380) * len(todo) / 1e6   # M tokens
    est_out_tok   = 160 * len(todo) / 1e6
    est_cost_usd  = est_in_tok * 0.10 + est_out_tok * 0.30
    print(f"\n  Estimated cost: ~${est_cost_usd:.2f} USD ({est_in_tok:.1f}M in + {est_out_tok:.1f}M out)")
    print(f"  Model: {MODEL}  |  Endpoint: {BASE_URL}")
    confirm = input(f"\n  Proceed? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    # ── Enrich ──────────────────────────────────────────────────────────────────
    client = OpenAI(api_key=API_KEY, base_url=BASE_URL)
    min_interval = 60.0 / RATE_LIMIT_RPM
    ok_count = fail_count = 0
    t_start = time.time()

    for i, idx in enumerate(todo):
        row  = df.loc[idx]
        proj = row.get("Projektnummer", f"row-{idx}")
        fb   = compute_confidence_deterministic(row)
        t0   = time.time()

        result = enrich_row(client, row, fb)
        for k, v in result.items():
            df.at[idx, k] = str(v)

        if result["enrich_status"] == "ok":
            ok_count += 1
        else:
            fail_count += 1

        # progress
        done_n  = i + 1
        elapsed = time.time() - t_start
        rate    = done_n / elapsed * 60
        eta_min = (len(todo) - done_n) / (done_n / elapsed) / 60 if done_n > 0 else 0
        print(f"  [{done_n:>5}/{len(todo)}] {proj:<20} conf={result['confidence']:.2f} "
              f"| {rate:.0f} req/min | ETA {eta_min:.0f} min", end="\r")

        # checkpoint every BATCH_SIZE rows
        if done_n % BATCH_SIZE == 0:
            df.to_csv(output_path, index=False)
            print(f"\n  ✓ Checkpoint saved ({done_n} done, {ok_count} ok, {fail_count} failed)")

        # rate limiting
        elapsed_call = time.time() - t0
        sleep_for    = max(0, min_interval - elapsed_call)
        if sleep_for > 0:
            time.sleep(sleep_for)

    # final save
    df.to_csv(output_path, index=False)
    total_time = (time.time() - t_start) / 60
    print(f"\n\n✅ Done! {ok_count:,} enriched, {fail_count:,} failed, {total_time:.1f} min total")
    print(f"   Output: {output_path}")
    print(f"\n   Quick stats:")
    enriched = df[df["enrich_status"] == "ok"]
    print(f"   Confidence ≥ 0.8:  {(pd.to_numeric(enriched['confidence'], errors='coerce') >= 0.8).sum():,}")
    print(f"   Has SATW tag:       {(enriched['satw_2025'] != '').sum():,}")
    print(f"   Has keywords:       {(enriched['keywords'] != '').sum():,}")


if __name__ == "__main__":
    main()
