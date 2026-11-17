#!/usr/bin/env python3
"""
ARAMIS incremental refresh
==========================
Brings aramis_scraped.csv.gz up to date with the live ARAMIS database while only
paying (in API calls and requests) for what actually changed.

Steps
  1. Download the ARAMIS bulk export (or read --export PATH)
  2. Diff it against the current data file: new projects, changed projects, removed ones
  3. Scrape ARAMIS detail pages for new projects (scan of project IDs above the
     highest known one) and re-scrape changed projects whose ID is known
  4. Enrich new projects, and changed projects whose title or abstract changed,
     with Mistral (abstract_en, keywords, satw_2025, confidence)
  5. Parse and canonicalise industry partners (clean_partners.py logic +
     company_aliases.csv, so new rows get the same canonical names as old ones)
  6. Safety checks, then write aramis_scraped.csv.gz, data_meta.json,
     pipeline/company_aliases.csv and a Markdown summary

Run from the repository root:
  python3 pipeline/refresh.py --dry-run                 # diff only, writes nothing
  python3 pipeline/refresh.py --limit 20                # small real run
  python3 pipeline/refresh.py --summary summary.md      # full run (what CI does)

Environment: MISTRAL_API_KEY (needed unless --no-llm)
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

import enrich as enrich_mod                     # noqa: E402  (Mistral enrichment)
import clean_partners as cp                     # noqa: E402  (partner parsing / aliases)
from scrape_aramis_scan import parse_beteiligte, parse_texte  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────
EXPORT_URL   = "https://datenausgabe.aramis.admin.ch/full_export.csv"
ARAMIS_URL   = "https://www.aramis.admin.ch"
DATA_FILE    = REPO / "aramis_scraped.csv.gz"
META_FILE    = REPO / "data_meta.json"
ALIASES_FILE = HERE / "company_aliases.csv"

KEY_COLS  = ["Forschungsstelle", "Projektnummer"]
TEXT_COLS = ["Projekttitel deutsch", "Projekttitel englisch", "Projekttitel französisch",
             "Projekttitel italienisch", "Kurzbeschreibung"]
SCRAPE_COLS = ["project_id", "wirtschaftspartner", "wirt_uid_url", "wirt_email",
               "wissensch_leitung", "forschungspartner",
               "texte_abstract_de", "texte_abstract_fr", "texte_abstract_it",
               "texte_abstract_en", "texte_keywords_de", "texte_keywords_en"]
PARTNER_COLS = ["wirt_person_name", "wirt_company_raw", "wirt_company_canonical"]
ENRICH_COLS  = ["abstract_en", "keywords", "satw_2025", "confidence", "enrich_status"]

HEADERS        = {"User-Agent": "Mozilla/5.0 (compatible; ARAMIS-explorer-refresh/1.0; "
                                "+https://github.com/epdupont-oss/aramis-funding-explorer)"}
SCAN_WORKERS   = 8
SCAN_BATCH     = 200       # IDs fetched per round
SCAN_STOP_GAP  = 1000      # stop after this many consecutive empty IDs (largest known gap: 459)
SCAN_MAX_IDS   = 25000     # hard cap on IDs scanned in one run
ENRICH_WORKERS = 4

# Safety limits
MAX_EXPORT_SHRINK   = 1000      # abort if the export has this many fewer rows than our data
MAX_FAILED_RATIO    = 0.20      # abort if more enrichments fail than this
MAX_GZ_BYTES        = 24_000_000  # Cloudflare Pages per-file limit is 25 MB
DEFAULT_MAX_NEW     = 8000      # abort above this many new projects unless --allow-large


class Abort(Exception):
    """A safety check failed; nothing is written."""


def log(msg: str) -> None:
    print(msg, flush=True)


# ── Loading & normalising ─────────────────────────────────────────────────────

def norm(v) -> str:
    """Compare values the way a human would: ignore CRLF vs LF and outer whitespace."""
    s = "" if v is None else str(v)
    return s.replace("\r\n", "\n").replace("\r", "\n").strip()


_CODE_SPLIT = re.compile(r",\s*(?=[A-Z]\d{3}\b)")


def norm_col(col: str, v) -> str:
    """Column-aware normalisation. Forschungsart is a list of research codes whose order
    ARAMIS does not keep stable, so compare it as a sorted set of codes."""
    s = norm(v)
    if col == "Forschungsart" and s:
        return ", ".join(sorted(x.strip() for x in _CODE_SPLIT.split(s) if x.strip()))
    return s


def read_csv_str(path_or_buf) -> pd.DataFrame:
    return pd.read_csv(path_or_buf, dtype=str, keep_default_na=False)


def download_export(dest: Path) -> Path:
    log(f"Downloading ARAMIS export from {EXPORT_URL} …")
    with requests.get(EXPORT_URL, headers=HEADERS, timeout=300, stream=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    size = dest.stat().st_size
    log(f"  {size / 1e6:.1f} MB")
    if size < 5_000_000:
        raise Abort(f"Export is only {size:,} bytes; expected ~28 MB. ARAMIS may be down.")
    return dest


def add_keys(df: pd.DataFrame, base_cols: list[str]) -> pd.DataFrame:
    """Add _key (funder + number + occurrence) and _fp (content fingerprint).

    18 project numbers occur twice under the same funder, so the occurrence index is
    assigned after sorting by content, which pairs duplicates the same way every time.
    """
    df = df.copy()
    other = [c for c in base_cols if c not in KEY_COLS]
    df["_fp"] = df[other].apply(lambda r: "\x1f".join(norm_col(c, r[c]) for c in other), axis=1)
    df["_text"] = df[TEXT_COLS].apply(lambda r: "\x1f".join(norm(v) for v in r), axis=1)
    df["_base"] = df["Forschungsstelle"].map(norm) + "\x1f" + df["Projektnummer"].map(norm)
    df = df.sort_values(["_base", "_fp"], kind="stable")
    df["_key"] = df["_base"] + "\x1f" + df.groupby("_base").cumcount().astype(str)
    return df.sort_index()


# ── Scraping ARAMIS detail pages ──────────────────────────────────────────────

_session = requests.Session()
_session.headers.update(HEADERS)


def fetch(url: str) -> str | None:
    """HTML, or None for 404. Raises after retries on other errors."""
    for attempt in range(3):
        try:
            r = _session.get(url, timeout=35)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    return None


def scrape_id(pid: int, with_texte: bool = True) -> dict | None:
    """Detail fields for one ARAMIS ProjectID, or None if the ID is empty."""
    html = fetch(f"{ARAMIS_URL}/Beteiligte/?ProjectID={pid}")
    if html is None:
        return None
    projekt_nr, data = parse_beteiligte(html)
    if not projekt_nr:
        return None
    data["project_id"] = str(pid)
    data["Projektnummer"] = projekt_nr
    if with_texte:
        try:
            t_html = fetch(f"{ARAMIS_URL}/Texte/?ProjectID={pid}")
            if t_html:
                data.update(parse_texte(t_html))
        except requests.RequestException:
            pass  # abstracts are optional
    return data


def _page_text(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.DOTALL)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def match_number(page_nr: str, html: str, by_token: dict[str, list[str]]) -> str | None:
    """Map the number read from a detail page to one of the wanted project numbers.

    parse_beteiligte reads the number up to the first space, so Innosuisse numbers
    such as '120.192 IP-ICT' come back as '120.192'. Use that first token to find
    candidates, then confirm the full number appears in the page text.
    """
    cands = by_token.get(page_nr.split()[0]) if page_nr else None
    if not cands:
        return None
    if len(cands) == 1 and cands[0] == page_nr:
        return page_nr
    text = _page_text(html)
    hits = [c for c in cands if re.search(r"(?<!\S)" + re.escape(c) + r"(?!\S)", text)]
    return max(hits, key=len) if hits else None


def scan_new_ids(start_pid: int, wanted_nrs: set[str]) -> tuple[dict[str, dict], dict]:
    """Scan ProjectIDs upward from start_pid until a long run of empty IDs.

    Returns ({Projektnummer: scraped fields}, stats). Only projects whose number is in
    wanted_nrs are kept; every non-empty ID counts as a hit for the stop rule.
    """
    by_token: dict[str, list[str]] = {}
    for nr in wanted_nrs:
        by_token.setdefault(nr.split()[0], []).append(nr)
    found: dict[str, dict] = {}
    stats = {"scanned": 0, "hits": 0, "errors": 0, "last_hit": start_pid - 1}
    pid = start_pid

    def one(p):
        try:
            html = fetch(f"{ARAMIS_URL}/Beteiligte/?ProjectID={p}")
            if html is None:
                return p, None, False
            page_nr, data = parse_beteiligte(html)
            if not page_nr:
                return p, None, False
            nr = match_number(page_nr, html, by_token)
            data["project_id"] = str(p)
            if nr:  # fetch abstracts only for projects we need
                try:
                    t_html = fetch(f"{ARAMIS_URL}/Texte/?ProjectID={p}")
                    if t_html:
                        data.update(parse_texte(t_html))
                except requests.RequestException:
                    pass
            return p, (nr, data), False
        except requests.RequestException:
            return p, None, True

    with ThreadPoolExecutor(SCAN_WORKERS) as pool:
        while pid - start_pid < SCAN_MAX_IDS:
            batch = list(range(pid, pid + SCAN_BATCH))
            pid += SCAN_BATCH
            for p, hit, err in pool.map(one, batch):
                stats["scanned"] += 1
                if err:
                    stats["errors"] += 1
                    continue
                if hit:
                    stats["hits"] += 1
                    stats["last_hit"] = max(stats["last_hit"], p)
                    nr, data = hit
                    if nr and nr not in found:
                        found[nr] = data
            if stats["scanned"] >= SCAN_BATCH and stats["errors"] / stats["scanned"] > 0.5:
                raise Abort(f"ARAMIS detail pages failing ({stats['errors']} errors in "
                            f"{stats['scanned']} requests); the site may be blocking us.")
            log(f"  scanned up to ID {pid - 1:,}: {stats['hits']} projects seen, "
                f"{len(found)} of {len(wanted_nrs)} wanted found, last hit {stats['last_hit']:,}")
            if pid - 1 - stats["last_hit"] >= SCAN_STOP_GAP:
                break
    return found, stats


# ── Partner cleaning (reuses clean_partners.py) ───────────────────────────────

class _MistralShim:
    """Gives an OpenAI-compatible client the `client.chat.complete(...)` shape that
    clean_partners.mistral_parse expects, so no extra SDK is needed."""

    def __init__(self, client):
        self._c = client
        self.chat = self

    def complete(self, **kw):
        return self._c.chat.completions.create(**kw)


def load_aliases() -> tuple[dict[str, str], Counter]:
    aliases, freq = {}, Counter()
    if ALIASES_FILE.exists():
        with open(ALIASES_FILE, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                aliases[r["raw_name"]] = r["canonical_name"]
                freq[r["raw_name"]] = int(r.get("frequency") or 0)
    return aliases, freq


def save_aliases(aliases: dict[str, str], freq: Counter) -> None:
    rows = sorted(((raw, aliases[raw], freq.get(raw, 0)) for raw in aliases),
                  key=lambda x: x[0].lower())
    with open(ALIASES_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["raw_name", "canonical_name", "frequency"])
        w.writerows(rows)


def canonicalise(raw: str, aliases: dict[str, str], known_canon: list[str]) -> str:
    if not raw:
        return ""
    if raw in aliases:
        return aliases[raw]
    key = raw.lower().strip().rstrip(".").strip()
    if key in cp.CURATED:
        return cp.CURATED[key]
    if cp.HAS_FUZZY and known_canon:
        m = cp.rfprocess.extractOne(raw, known_canon, scorer=cp.fuzz.token_sort_ratio,
                                    score_cutoff=cp.FUZZY_THRESHOLD)
        if m:
            return m[0]
    return raw


def clean_partner_rows(df: pd.DataFrame, idx: list, llm_client, use_uid: bool) -> dict:
    """Fill wirt_person_name / wirt_company_raw / wirt_company_canonical for rows idx."""
    aliases, freq = load_aliases()
    known_canon = sorted(set(aliases.values()))
    before = set(known_canon)
    ambiguous, uid_cache = [], {}
    for i in idx:
        wirt = df.at[i, "wirtschaftspartner"] or ""
        blocks = [b.strip() for b in wirt.split(cp.BLOCK_SEP) if b.strip()]
        person, company = cp.parse_block(blocks[0]) if blocks else ("", "")
        df.at[i, "wirt_person_name"] = person
        df.at[i, "wirt_company_raw"] = company
        if llm_client is not None and blocks and "\n" not in blocks[0]:
            ambiguous.append((i, blocks[0]))

    for s in range(0, len(ambiguous), cp.MISTRAL_BATCH):
        batch = ambiguous[s:s + cp.MISTRAL_BATCH]
        results = cp.mistral_parse([t for _, t in batch], _MistralShim(llm_client))
        for (i, _), res in zip(batch, results):
            if res.get("company_name"):
                df.at[i, "wirt_person_name"] = (res.get("person_name") or "").strip()
                df.at[i, "wirt_company_raw"] = res["company_name"].strip()

    for i in idx:
        raw = df.at[i, "wirt_company_raw"]
        canon = ""
        uid_url = df.at[i, "wirt_uid_url"] if "wirt_uid_url" in df.columns else ""
        if use_uid and uid_url:
            m = cp.RE_UID.search(uid_url)
            if m:
                official = cp.lookup_zefix(m.group(), uid_cache)
                time.sleep(cp.UID_DELAY_S)
                if official:
                    canon = cp.CURATED.get(official.lower().strip().rstrip("."), official)
        if not canon:
            canon = canonicalise(raw, aliases, known_canon)
        df.at[i, "wirt_company_canonical"] = canon
        if raw:
            freq[raw] += 1
            aliases.setdefault(raw, canon)

    save_aliases(aliases, freq)
    new_companies = sorted({df.at[i, "wirt_company_canonical"] for i in idx}
                           - before - {""})
    return {"rows": len(idx), "llm_refined": len(ambiguous), "new_companies": new_companies}


# ── Enrichment (reuses enrich.py) ─────────────────────────────────────────────

def enrich_rows(df: pd.DataFrame, idx: list, client) -> dict:
    base_cols = [c for c in df.columns if not c.startswith("_")]

    def one(i):
        row = df.loc[i, base_cols]
        fb = enrich_mod.compute_confidence_deterministic(row)
        return i, enrich_mod.enrich_row(client, row, fb)

    done = failed = 0
    with ThreadPoolExecutor(ENRICH_WORKERS) as pool:
        for i, res in pool.map(one, idx):
            for k, v in res.items():
                df.at[i, k] = str(v)
            done += 1
            failed += res["enrich_status"] != "ok"
            if done % 50 == 0 or done == len(idx):
                log(f"  enriched {done}/{len(idx)} ({failed} failed)")
    return {"attempted": done, "failed": failed}


# ── Summary ───────────────────────────────────────────────────────────────────

def project_title(r) -> str:
    for c in ["Projekttitel englisch", "Projekttitel deutsch",
              "Projekttitel französisch", "Projekttitel italienisch"]:
        t = norm(r.get(c, ""))
        if t:
            return re.sub(r"\s+", " ", t)[:110]
    return "(no title)"


def build_summary(info: dict, new_df: pd.DataFrame) -> str:
    L = [f"## ARAMIS refresh — {info['date']}", ""]
    L += [
        "| | |", "|---|---|",
        f"| Projects in ARAMIS export | {info['export_rows']:,} |",
        f"| Projects before → after | {info['before_rows']:,} → {info['after_rows']:,} |",
        f"| **New projects** | **{info['new']:,}** |",
        f"| Updated projects | {info['changed']:,} (re-enriched: {info['reenriched']:,}) |",
        f"| No longer in export (kept) | {info['removed']:,} |",
        f"| Detail pages found for new projects | {info.get('scraped_new', 0):,} of {info['new']:,} |",
        f"| Mistral enrichments | {info.get('enrich_attempted', 0):,} "
        f"({info.get('enrich_failed', 0)} failed) |",
        f"| Data file size | {info['gz_mb']:.1f} MB (limit 25 MB) |",
        "",
    ]
    if info.get("changed_fields"):
        L += ["### What changed in updated projects", ""]
        L += [f"- {c}: {n:,}" for c, n in info["changed_fields"]] + [""]
    if info.get("warnings"):
        L += ["### ⚠️ Warnings", ""] + [f"- {w}" for w in info["warnings"]] + [""]
    if len(new_df):
        L += ["### New projects by funder", ""]
        for f, c in new_df["Forschungsstelle"].value_counts().head(10).items():
            L.append(f"- {f}: {c}")
        tags = Counter(t.strip() for s in new_df.get("satw_2025", []) for t in str(s).split(";")
                       if t.strip())
        if tags:
            L += ["", "### SATW technologies in new projects", ""]
            L += [f"- {t}: {c}" for t, c in tags.most_common(10)]
        if info.get("new_companies"):
            L += ["", f"### New industry partners ({len(info['new_companies'])})", ""]
            L += [f"- {c}" for c in info["new_companies"][:25]]
            if len(info["new_companies"]) > 25:
                L.append(f"- … and {len(info['new_companies']) - 25} more")
        L += ["", "### Sample of new projects", ""]
        for _, r in new_df.head(8).iterrows():
            satw = f" · SATW: {r['satw_2025']}" if norm(r.get("satw_2025", "")) else ""
            L.append(f"- **{project_title(r)}** — {r['Forschungsstelle']}, "
                     f"start {r['Projektstart'] or '?'}{satw}")
    L += ["", "_Generated by `pipeline/refresh.py`. Merge this pull request to publish "
              "the new data on the live site._"]
    return "\n".join(L)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--export", help="Use a local ARAMIS export CSV instead of downloading")
    ap.add_argument("--dry-run", action="store_true", help="Diff only; write nothing, no API calls")
    ap.add_argument("--limit", type=int, default=0, help="Process at most N new projects (testing)")
    ap.add_argument("--no-llm", action="store_true", help="Skip Mistral (new rows get fallback values)")
    ap.add_argument("--no-scrape", action="store_true", help="Skip ARAMIS detail pages")
    ap.add_argument("--no-uid", action="store_true", help="Skip zefix UID name lookups")
    ap.add_argument("--allow-large", action="store_true",
                    help=f"Allow more than {DEFAULT_MAX_NEW} new projects in one run")
    ap.add_argument("--summary", help="Write a Markdown summary (used as the PR description)")
    args = ap.parse_args()

    warnings: list[str] = []
    today = datetime.now(timezone.utc).date().isoformat()

    # 1 ── Load current data and the export
    log(f"Loading {DATA_FILE.name} …")
    cur = read_csv_str(DATA_FILE)
    out_cols = list(cur.columns)
    log(f"  {len(cur):,} rows, {len(out_cols)} columns")

    tmp_export = REPO / ".refresh_export.csv"
    export_path = Path(args.export) if args.export else download_export(tmp_export)
    exp = read_csv_str(export_path)
    if export_path == tmp_export:
        tmp_export.unlink(missing_ok=True)
    base_cols = list(exp.columns)
    missing = [c for c in out_cols[:16] if c not in base_cols]
    if missing:
        raise Abort(f"Export is missing expected columns: {missing}. ARAMIS changed its format.")
    log(f"  export: {len(exp):,} rows")
    if len(exp) < len(cur) - MAX_EXPORT_SHRINK:
        raise Abort(f"Export has {len(exp):,} rows vs {len(cur):,} in our data; "
                    "looks truncated, refusing to continue.")

    # 2 ── Diff
    cur = add_keys(cur, base_cols)
    exp = add_keys(exp, base_cols)
    cur_by_key = dict(zip(cur["_key"], cur.index))
    exp_keys = set(exp["_key"])
    new_mask = ~exp["_key"].isin(cur_by_key)
    new_rows = exp[new_mask]
    removed = len(set(cur["_key"]) - exp_keys)

    changed_idx, text_changed_idx = [], []
    changed_fields: Counter = Counter()
    examples: dict[str, tuple] = {}
    both = exp[~new_mask]
    for _, e in both.iterrows():
        ci = cur_by_key[e["_key"]]
        if e["_fp"] != cur.at[ci, "_fp"]:
            changed_idx.append(ci)
            for c in base_cols:
                if c not in KEY_COLS and norm_col(c, e[c]) != norm_col(c, cur.at[ci, c]):
                    changed_fields[c] += 1
                    examples.setdefault(c, (e["Projektnummer"], norm(cur.at[ci, c])[:60],
                                            norm(e[c])[:60]))
            if e["_text"] != cur.at[ci, "_text"]:
                text_changed_idx.append(ci)
            for c in base_cols:            # take the export's values for base columns
                cur.at[ci, c] = e[c]

    log(f"Diff: {len(new_rows):,} new, {len(changed_idx):,} changed "
        f"({len(text_changed_idx):,} with new text), {removed:,} no longer in export")

    if args.limit and len(new_rows) > args.limit:
        warnings.append(f"--limit {args.limit}: only {args.limit} of {len(new_rows):,} "
                        "new projects processed in this run.")
        new_rows = new_rows.head(args.limit)
    if len(new_rows) > DEFAULT_MAX_NEW and not args.allow_large:
        raise Abort(f"{len(new_rows):,} new projects is unusually many; "
                    "re-run with --allow-large if this is expected.")

    for c, n in changed_fields.most_common():
        old, new = examples[c][1], examples[c][2]
        log(f"  {c:<26} {n:>5} changed   e.g. {examples[c][0]}: {old!r} -> {new!r}")

    if not len(new_rows) and not changed_idx:
        log("Nothing new or changed in ARAMIS; data left as is.")
        return 0

    if args.dry_run:
        log("\nDry run: sample of new projects")
        for _, r in new_rows.head(10).iterrows():
            log(f"  {r['Forschungsstelle']:<14} {r['Projektnummer']:<22} {project_title(r)[:70]}")
        return 0

    # Append new rows with all output columns
    new_df = new_rows.reindex(columns=out_cols + [c for c in ["_key", "_fp", "_text", "_base"]],
                              fill_value="")
    new_df.index = range(cur.index.max() + 1, cur.index.max() + 1 + len(new_df))
    df = pd.concat([cur, new_df])
    new_idx = list(new_df.index)

    # 3 ── Scrape detail pages
    scraped_new = 0
    if not args.no_scrape:
        pids = pd.to_numeric(cur["project_id"], errors="coerce")
        start = int(pids.max()) + 1 if pids.notna().any() else 1
        wanted = {norm(df.at[i, "Projektnummer"]) for i in new_idx}
        if wanted:
            log(f"Scanning ARAMIS project IDs from {start:,} for {len(wanted):,} new projects …")
            found, stats = scan_new_ids(start, wanted)
            for i in new_idx:
                data = found.get(norm(df.at[i, "Projektnummer"]))
                if data:
                    for c in SCRAPE_COLS:
                        df.at[i, c] = data.get(c, "")
                    scraped_new += 1
            if scraped_new < len(new_idx):
                warnings.append(f"{len(new_idx) - scraped_new} new projects have no ARAMIS "
                                "detail page yet (no partner data); normal for very new entries.")
        rescrape = [i for i in changed_idx if norm(df.at[i, "project_id"])]
        if rescrape:
            log(f"Re-scraping {len(rescrape):,} changed projects …")
            with ThreadPoolExecutor(SCAN_WORKERS) as pool:
                results = pool.map(lambda i: (i, _safe_scrape(int(df.at[i, "project_id"]))),
                                   rescrape)
                for i, data in results:
                    if data:
                        for c in SCRAPE_COLS:
                            df.at[i, c] = data.get(c, df.at[i, c])
    else:
        warnings.append("Detail-page scraping skipped (--no-scrape).")

    # 4 ── Enrich
    to_enrich = new_idx + text_changed_idx
    client = None
    enrich_stats = {"attempted": 0, "failed": 0}
    if args.no_llm:
        warnings.append("Mistral enrichment skipped (--no-llm); new rows have fallback values.")
        for i in new_idx:
            row = df.loc[i, base_cols]
            df.at[i, "abstract_en"] = "No abstract available."
            df.at[i, "confidence"] = str(enrich_mod.compute_confidence_deterministic(row))
            df.at[i, "enrich_status"] = "skip"
    elif to_enrich:
        if not enrich_mod.API_KEY:
            raise Abort("MISTRAL_API_KEY is not set.")
        client = enrich_mod.OpenAI(api_key=enrich_mod.API_KEY, base_url=enrich_mod.BASE_URL)
        log(f"Enriching {len(to_enrich):,} projects with {enrich_mod.MODEL} …")
        enrich_stats = enrich_rows(df, to_enrich, client)
        if (enrich_stats["attempted"] >= 10 and
                enrich_stats["failed"] / enrich_stats["attempted"] > MAX_FAILED_RATIO):
            raise Abort(f"{enrich_stats['failed']} of {enrich_stats['attempted']} "
                        "enrichments failed; check the Mistral key and quota.")
        if enrich_stats["failed"]:
            warnings.append(f"{enrich_stats['failed']} enrichments failed and kept fallback values.")

    # 5 ── Partners
    partner_idx = [i for i in new_idx + changed_idx if norm(df.at[i, "wirtschaftspartner"])]
    partner_stats = {"new_companies": []}
    if partner_idx:
        log(f"Cleaning industry partners for {len(partner_idx):,} projects …")
        partner_stats = clean_partner_rows(df, partner_idx, client, use_uid=not args.no_uid)

    # 6 ── Safety checks and write
    out = df[out_cols]
    if len(out) < len(cur):
        raise Abort("Output has fewer rows than the input; refusing to write.")
    buf = io.StringIO()
    out.to_csv(buf, index=False)
    raw_bytes = buf.getvalue().encode("utf-8")
    gz_bytes = gzip.compress(raw_bytes, compresslevel=9, mtime=0)
    if len(gz_bytes) > MAX_GZ_BYTES:
        raise Abort(f"Compressed data would be {len(gz_bytes) / 1e6:.1f} MB, over the "
                    "Cloudflare Pages 25 MB per-file limit. Time to move the data to R2.")

    DATA_FILE.write_bytes(gz_bytes)
    meta = {
        "data_as_of": today,
        "source_export": EXPORT_URL if not args.export else "local file",
        "projects": len(out),
        "last_refresh": {"new": len(new_idx), "updated": len(changed_idx),
                         "reenriched": len(text_changed_idx), "removed_from_export": removed},
    }
    META_FILE.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    log(f"Wrote {DATA_FILE.name} ({len(gz_bytes) / 1e6:.1f} MB, {len(out):,} rows) "
        f"and {META_FILE.name}")

    if args.summary:
        info = {
            "date": today, "export_rows": len(exp), "before_rows": len(cur),
            "after_rows": len(out), "new": len(new_idx), "changed": len(changed_idx),
            "reenriched": len(text_changed_idx), "removed": removed,
            "scraped_new": scraped_new,
            "enrich_attempted": enrich_stats["attempted"],
            "enrich_failed": enrich_stats["failed"],
            "gz_mb": len(gz_bytes) / 1e6, "warnings": warnings,
            "new_companies": partner_stats.get("new_companies", []),
            "changed_fields": changed_fields.most_common(),
        }
        Path(args.summary).write_text(build_summary(info, df.loc[new_idx, out_cols]),
                                      encoding="utf-8")
        log(f"Wrote summary to {args.summary}")
    for w in warnings:
        log(f"WARNING: {w}")
    return 0


def _safe_scrape(pid: int) -> dict | None:
    try:
        return scrape_id(pid)
    except requests.RequestException:
        return None


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Abort as e:
        print(f"\nABORTED: {e}\nNothing was written.", file=sys.stderr)
        sys.exit(2)
