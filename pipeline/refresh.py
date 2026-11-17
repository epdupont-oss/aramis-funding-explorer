#!/usr/bin/env python3
"""
ARAMIS incremental refresh
==========================
Brings aramis_scraped.csv.gz up to date with the live ARAMIS database while only
paying (in API calls and requests) for what actually changed.

Steps
  1. Download the ARAMIS bulk export (or read --export PATH)
  2. Diff it against the current data file: new projects, changed projects, removed ones
  3. Look up ARAMIS detail pages for new projects via the search form, within a
     time budget; projects not reached are retried next month (scrape_pending.csv)
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
import threading
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
SCRAPE_WORKERS = 8
SCRAPE_BUDGET_MIN = 60     # stop starting new lookups after this; the rest wait for next month
MAX_ATTEMPTS   = 3         # give up on a project after this many "not found" searches
ENRICH_WORKERS = 4
PENDING_FILE   = HERE / "scrape_pending.csv"

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

_local = threading.local()


def _sess() -> requests.Session:
    """One session per thread: the ARAMIS search form is ASP.NET with a ViewState."""
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
        _local.s.headers.update(HEADERS)
        _local.vs = None
    return _local.s


def fetch(url: str) -> str | None:
    """HTML, or None for 404. Raises after retries on other errors."""
    for attempt in range(3):
        try:
            r = _sess().get(url, timeout=60)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    return None


def _viewstate(html: str) -> dict:
    d = {}
    for k in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        m = re.search(rf'name="{re.escape(k)}"[^>]*value="([^"]*)"', html)
        d[k] = m.group(1) if m else ""
    d["__LASTFOCUS"] = d["__EVENTTARGET"] = d["__EVENTARGUMENT"] = ""
    return d


def search_ids(projekt_nr: str) -> list[str]:
    """ProjectIDs the ARAMIS search form returns for a project number (same request
    as the June scrape_aramis.py). Several can come back; the caller verifies."""
    s = _sess()
    url = f"{ARAMIS_URL}/ProjektsucheAlt/"
    for attempt in range(3):
        try:
            if _local.vs is None:
                r = s.get(url, timeout=60)
                r.raise_for_status()
                _local.vs = _viewstate(r.text)
            data = {**_local.vs,
                    "ctl00$cphContent$txtProjektsuche": projekt_nr,
                    "ctl00$cphContent$ddlForschungsstelle": "",
                    "ctl00$cphContent$ddlResultate": "20",
                    "ctl00$cphContent$cmdSearch": "Suche starten"}
            r = s.post(url, data=data, timeout=60)
            r.raise_for_status()
            _local.vs = _viewstate(r.text)
            return list(dict.fromkeys(re.findall(r"ProjectID=(\d+)", r.text)))
        except requests.RequestException:
            _local.vs = None
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    return []


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


def lookup_project(projekt_nr: str) -> tuple[str, dict | None]:
    """('found', fields) / ('not_found', None) / ('error', None) for one project."""
    try:
        pids = search_ids(projekt_nr)
        by_token = {projekt_nr.split()[0]: [projekt_nr]}
        for pid in pids[:5]:
            html = fetch(f"{ARAMIS_URL}/Beteiligte/?ProjectID={pid}")
            if not html:
                continue
            page_nr, data = parse_beteiligte(html)
            # Accept a verified match, or the only search result: programme IDs hold
            # several sub-projects but show just one number on their page.
            if match_number(page_nr, html, by_token) != projekt_nr and len(pids) > 1:
                continue
            data["project_id"] = pid
            try:
                t_html = fetch(f"{ARAMIS_URL}/Texte/?ProjectID={pid}")
                if t_html:
                    data.update(parse_texte(t_html))
            except requests.RequestException:
                pass  # abstracts are optional
            return "found", data
        return "not_found", None
    except requests.RequestException:
        return "error", None


def load_pending() -> dict[tuple, int]:
    out = {}
    if PENDING_FILE.exists():
        with open(PENDING_FILE, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                out[(r["Forschungsstelle"], r["Projektnummer"])] = int(r["attempts"] or 0)
    return out


def save_pending(pending: dict[tuple, int]) -> None:
    with open(PENDING_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Forschungsstelle", "Projektnummer", "attempts"])
        for (fs, nr), a in sorted(pending.items()):
            w.writerow([fs, nr, a])


def scrape_rows(df: pd.DataFrame, todo: list, budget_s: float) -> dict:
    """Look up detail pages for rows `todo`, in order, until the time budget runs out.
    Returns per-row outcome so the caller can update the pending list."""
    deadline = time.monotonic() + budget_s
    outcome: dict = {}

    def one(i):
        if time.monotonic() > deadline:
            return i, "skipped", None
        return (i,) + lookup_project(norm(df.at[i, "Projektnummer"]))

    t0 = time.monotonic()
    with ThreadPoolExecutor(SCRAPE_WORKERS) as pool:
        for n, (i, status, data) in enumerate(pool.map(one, todo), 1):
            outcome[i] = status
            if data:
                for c in SCRAPE_COLS:
                    df.at[i, c] = data.get(c, "")
            if n % 25 == 0 or n == len(todo):
                c = Counter(outcome.values())
                log(f"  {n}/{len(todo)} looked up in {(time.monotonic() - t0) / 60:.0f} min: "
                    f"{c['found']} found, {c['not_found']} not found, "
                    f"{c['error']} errors, {c['skipped']} left for next run")
    errs = sum(v == "error" for v in outcome.values())
    tried = sum(v != "skipped" for v in outcome.values())
    if tried >= 20 and errs / tried > 0.5:
        raise Abort(f"ARAMIS detail lookups failing ({errs} errors in {tried}); "
                    "the site may be down or blocking us.")
    return outcome


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
    ap.add_argument("--scrape-minutes", type=float, default=SCRAPE_BUDGET_MIN,
                    help="Time budget for ARAMIS detail-page lookups (default %(default)s)")
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

    if not len(new_rows) and not changed_idx and (args.no_scrape or not load_pending()):
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

    # 3 ── Scrape detail pages (new projects + ones carried over from earlier runs)
    scraped_idx: list = []
    if not args.no_scrape:
        pending = load_pending()
        key_of = lambda i: (norm(df.at[i, "Forschungsstelle"]), norm(df.at[i, "Projektnummer"]))
        carry = [i for i in df.index[:len(cur)]
                 if key_of(i) in pending and pending[key_of(i)] < MAX_ATTEMPTS
                 and not norm(df.at[i, "project_id"])]
        # Newest first, so if the budget runs out it is old back-filled projects that wait
        by_start = lambda i: norm(df.at[i, "Projektstart"])
        todo = sorted(new_idx, key=by_start, reverse=True) + sorted(carry, key=by_start, reverse=True)
        if todo:
            log(f"Looking up ARAMIS detail pages for {len(new_idx):,} new and "
                f"{len(carry):,} carried-over projects (budget {args.scrape_minutes:.0f} min) …")
            outcome = scrape_rows(df, todo, args.scrape_minutes * 60)
            for i, st in outcome.items():
                k = key_of(i)
                if st == "found":
                    pending.pop(k, None)
                    scraped_idx.append(i)
                elif st == "not_found":
                    pending[k] = pending.get(k, 0) + 1
                else:                      # skipped or error: try again next run
                    pending.setdefault(k, pending.get(k, 0))
            pending = {k: a for k, a in pending.items() if a < MAX_ATTEMPTS}
            save_pending(pending)
            left = sum(st in ("skipped", "error") for st in outcome.values())
            if left:
                warnings.append(f"{left} projects not looked up yet (ARAMIS is slow from GitHub); "
                                "they will be retried next month.")
        scraped_new = sum(1 for i in scraped_idx if i in set(new_idx))
    else:
        scraped_new = 0
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
    partner_idx = [i for i in scraped_idx if norm(df.at[i, "wirtschaftspartner"])]
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


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Abort as e:
        print(f"\nABORTED: {e}\nNothing was written.", file=sys.stderr)
        sys.exit(2)
