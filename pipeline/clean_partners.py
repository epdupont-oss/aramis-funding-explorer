#!/usr/bin/env python3
"""
clean_partners.py

Cleans the wirtschaftspartner column in aramis_scraped.csv.

  Phase 1 — Parse: split each block into (person_name, company_raw)
            Multi-line blocks use line-structure heuristics (reliable).
            Single-line blocks use suffix-anchored regex + optional Mistral pass.

  Phase 2 — Fuzzy dedup: cluster raw company names by similarity, pick canonical.

  Phase 3 — Curated aliases: apply a hand-written map (SBB/CFF, Roche, etc.).

  Phase 4 — Output: aramis_cleaned.csv + company_aliases.csv (editable lookup).

Usage:
  pip install rapidfuzz mistralai
  export MISTRAL_API_KEY=...          # optional; skip with --no-mistral
  python3 clean_partners.py [--no-mistral] [--threshold 88]
"""

import csv
import re
import sys
import json
import time
import argparse
import urllib.request
import urllib.error
import html as _html_lib
from collections import Counter, defaultdict
from pathlib import Path

# ── Optional deps ──────────────────────────────────────────────────────────────
try:
    from rapidfuzz import fuzz, process as rfprocess
    HAS_FUZZY = True
except ImportError:
    HAS_FUZZY = False

try:
    from mistralai import Mistral
    import os
    _MISTRAL_KEY = os.environ.get('MISTRAL_API_KEY', '')
    HAS_MISTRAL = bool(_MISTRAL_KEY)
except ImportError:
    HAS_MISTRAL = False
    _MISTRAL_KEY = ''

# ── Config ─────────────────────────────────────────────────────────────────────
INPUT_CSV   = 'aramis_scraped.csv'
OUTPUT_CSV  = 'aramis_cleaned.csv'
ALIASES_CSV = 'company_aliases.csv'
BLOCK_SEP   = ' ||| '
FUZZY_THRESHOLD = 95          # % similarity to consider same company
MISTRAL_MODEL   = 'mistral-small-latest'
MISTRAL_BATCH   = 30          # entries per LLM call
UID_REGISTER_URL = 'https://www.uid.admin.ch/Detail.aspx?uid_id={uid}'
UID_DELAY_S      = 0.15       # gentle rate-limiting between UID lookups

# ── Regex ──────────────────────────────────────────────────────────────────────
RE_PLZ     = re.compile(r'\bCH-\d{4}\b')
RE_UID     = re.compile(r'\bCHE[-.]?\d{3}[.-]?\d{3}[.-]?\d{3}\b')  # CHE-100.866.483 or CHE108526832
RE_EMAIL   = re.compile(r'\b[\w.%+-]+@[\w.-]+\.[a-zA-Z]{2,}\b')
RE_URL     = re.compile(r'www\.\S+|https?://\S+')
RE_PHONE   = re.compile(r'\+41[\s\d/().-]{7,}')
RE_STREET  = re.compile(r'^\d+\b')      # line starts with a street number

COMPANY_SUFFIXES = [
    'AG', 'SA', 'GmbH', 'Sàrl', 'Sarl', 'SARL', 'Ltd', 'Ltd.',
    'S.A.', 'Corp', 'Inc', 'Inc.', 'srl', 'SRL', 'KG', 'OHG', 'SE',
    'LLC', 'BV', 'NV', 'Holding', 'Group', 'Co.', '& Cie.',
]
_SFX_PATTERN = r'\b(?:' + '|'.join(re.escape(s) for s in COMPANY_SUFFIXES) + r')(?=\s|,|$)'
RE_SUFFIX = re.compile(_SFX_PATTERN)

TITLE_WORDS = {
    'Doktor', 'Dr.', 'Dr', 'Prof.', 'Prof', 'Professor',
    'Herr', 'Frau', 'Mr.', 'Mr', 'Mrs.', 'Mrs', 'Ms.', 'Ms',
    'Dipl.', 'Ing.', 'lic.', 'phil.', 'sc.', 'med.', 'iur.',
    'M.Sc.', 'M.Sc', 'B.Sc.', 'B.Sc', 'MSc', 'BSc',
}

# ── Curated alias map (lowercase stripped → canonical display name) ─────────────
CURATED = {
    # Transport
    'sbb': 'SBB CFF FFS',
    'cff': 'SBB CFF FFS',
    'ffs': 'SBB CFF FFS',
    'sbb cff': 'SBB CFF FFS',
    'sbb ag': 'SBB CFF FFS',
    'sbb cff ffs ag': 'SBB CFF FFS',
    'postauto ag': 'PostAuto',
    'postauto schweiz ag': 'PostAuto',
    # Post
    'die schweizerische post ag': 'Schweizerische Post',
    'schweizerische post ag': 'Schweizerische Post',
    'schweizerische post': 'Schweizerische Post',
    # Pharma / Life
    'novartis ag': 'Novartis',
    'novartis pharma ag': 'Novartis',
    'novartis institutes for biomedical research': 'Novartis',
    'roche diagnostics international ltd': 'Roche Diagnostics',
    'roche diagnostics international ag': 'Roche Diagnostics',
    'roche diagnostics ag': 'Roche Diagnostics',
    'roche diagnostics gmbh': 'Roche Diagnostics',
    'f. hoffmann-la roche ag': 'Roche',
    'f. hoffmann-la roche ltd': 'Roche',
    'hoffmann-la roche ag': 'Roche',
    # Food
    'nestle ag': 'Nestlé',
    'nestlé ag': 'Nestlé',
    'nestec sa': 'Nestlé',
    'nestlé research': 'Nestlé Research',
    # Tech / Industry
    'abb ag': 'ABB',
    'abb ltd': 'ABB',
    'abb schweiz ag': 'ABB',
    'swisscom ag': 'Swisscom',
    'swisscom (schweiz) ag': 'Swisscom',
    'georg fischer ag': 'Georg Fischer',
    'stadler rail ag': 'Stadler Rail',
    'stadler bussnang ag': 'Stadler Rail',
    # Energy
    'bkw energie ag': 'BKW',
    'bkw ag': 'BKW',
    'axpo ag': 'Axpo',
    'alpiq ag': 'Alpiq',
    # Finance
    'ubs ag': 'UBS',
    'ubs switzerland ag': 'UBS',
    'credit suisse ag': 'Credit Suisse',
    # Other
    'empa': 'Empa',
    'eawag': 'Eawag',
}

# ── Swiss UID register lookup ─────────────────────────────────────────────────

_RE_UID_NAME = re.compile(
    r'<div style="padding:7px 12px">([^<]+)</div>\s*<span id="[^"]*lentxtName'
)
_UID_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'

def _uid_dotted(uid_str: str) -> str:
    """Normalise any CHE-UID variant to the dotted format: CHE-100.866.483."""
    digits = re.sub(r'[^0-9]', '', uid_str)
    if len(digits) == 9:
        return f'CHE-{digits[:3]}.{digits[3:6]}.{digits[6:]}'
    return uid_str  # unexpected format — return as-is

def lookup_zefix(uid_str: str, cache: dict) -> str:
    """Return the official registered name from uid.admin.ch for a CHE-UID, or ''."""
    dotted = _uid_dotted(uid_str)
    if dotted in cache:
        return cache[dotted]
    url = UID_REGISTER_URL.format(uid=dotted)
    try:
        req = urllib.request.Request(url, headers={'User-Agent': _UID_UA})
        with urllib.request.urlopen(req, timeout=15) as resp:
            page = resp.read().decode('utf-8', errors='replace')
        m = _RE_UID_NAME.search(page)
        name = _html_lib.unescape(m.group(1).strip()) if m else ''
        cache[dotted] = name
    except Exception:
        cache[dotted] = ''
    return cache[dotted]


# ── Parsing helpers ────────────────────────────────────────────────────────────

def _is_noise_line(line: str) -> bool:
    """True if line is clearly not person/company (address, email, URL, phone)."""
    return bool(
        RE_PLZ.search(line) or
        RE_UID.search(line) or
        RE_EMAIL.search(line) or
        RE_URL.search(line) or
        RE_PHONE.search(line) or
        RE_STREET.match(line)
    )

def _clean_company(name: str) -> str:
    """Strip trailing/embedded noise from a company name."""
    name = RE_URL.sub('', name)
    name = RE_UID.sub('', name)
    name = RE_PLZ.sub('', name)
    return name.strip().strip(',').strip()

def parse_multiline(lines: list[str]) -> tuple[str, str]:
    """Parse a multi-line wirtschaftspartner block → (person, company)."""
    lines = [l.strip() for l in lines if l.strip()]

    # Find first line with a company suffix (= company name line)
    co_idx = -1
    for i, line in enumerate(lines):
        cleaned = _clean_company(line)
        if RE_SUFFIX.search(cleaned):
            co_idx = i
            break

    if co_idx == -1:
        # No suffix found — return last non-noise line as company name
        candidates = [l for l in lines if not _is_noise_line(l)]
        company = _clean_company(candidates[-1]) if candidates else ''
        person_lines = [l for l in candidates[:-1]] if len(candidates) > 1 else []
        return ' '.join(person_lines), company

    company = _clean_company(lines[co_idx])

    # Person = lines before company that aren't noise
    person_parts = [l for l in lines[:co_idx] if not _is_noise_line(l)]
    person = ' '.join(person_parts)
    return person, company


def parse_singleline(text: str) -> tuple[str, str]:
    """Parse a single-line wirtschaftspartner entry → (person, company)."""
    # Step 1: strip trailing noise tokens
    had_url = bool(RE_URL.search(text))          # URL = company-only format signal
    # Remove URL, CHE-UID and everything that follows them
    text = RE_URL.sub('', text).strip()
    # Remove CHE-UID and trailing content
    m = RE_UID.search(text)
    if m:
        text = text[:m.start()].strip()
    # Remove trailing PLZ and content after it
    m = RE_PLZ.search(text)
    if m:
        text = text[:m.start()].strip()
    text = text.strip().strip(',').strip()

    if not text:
        return '', ''

    # If the entry originally had a UID URL and nothing else, it's company-only
    if had_url and not RE_EMAIL.search(text) and not RE_PHONE.search(text):
        return '', _clean_company(text)

    # Step 2: find last company suffix in cleaned text
    matches = list(RE_SUFFIX.finditer(text))
    if not matches:
        # No suffix — return entire text as company (no person name)
        return '', text

    last_m = matches[-1]
    before_suffix = text[:last_m.end()].strip()  # includes the suffix word

    # Step 3: figure out person/company boundary
    # Strategy: split into words, suffix is at index k.
    # Walk back from suffix: company name is 1-5 words ending at suffix.
    # Everything before that is the person name.
    # If text starts with a title word, person is longer (title + 2 name words).

    words = before_suffix.split()
    if not words:
        return '', before_suffix

    # Find suffix word index (last occurrence)
    suffix_word = last_m.group().strip()
    try:
        sfx_idx = max(i for i, w in enumerate(words) if w.rstrip(',') == suffix_word)
    except ValueError:
        return '', before_suffix

    # Determine max company word count (exclusive of suffix):
    # - If first word is a title → person = first 3 words, company = rest
    has_title = words[0] in TITLE_WORDS if words else False

    if has_title:
        person_end = min(3, sfx_idx)  # title + up to 2 name words
    else:
        # Default: person = first 2 words (first + last name)
        person_end = min(2, sfx_idx)

    # Sanity: if person_end == sfx_idx there's no room for a company before suffix
    if person_end >= sfx_idx and sfx_idx > 0:
        person_end = max(0, sfx_idx - 2)  # at minimum 2 company words

    person = ' '.join(words[:person_end]).strip()
    company = ' '.join(words[person_end:sfx_idx + 1]).strip()
    return person, company


def parse_block(block: str) -> tuple[str, str]:
    """Dispatch to multiline or singleline parser."""
    block = block.strip()
    if not block:
        return '', ''
    lines = block.split('\n')
    if len(lines) > 1:
        return parse_multiline(lines)
    return parse_singleline(block)


# ── Mistral pass (optional) ────────────────────────────────────────────────────

def mistral_parse(entries: list[str], client) -> list[dict]:
    """
    Ask Mistral to parse a batch of raw partner strings.
    Returns list of {person, company} dicts.
    """
    prompt = (
        "You are parsing Swiss research funding implementation partner records. "
        "Each entry contains a person's name (with optional title) and a company name, "
        "sometimes mixed with address and UID information. "
        "For each numbered entry, extract:\n"
        "  - person_name: full name with title (empty string if no person listed)\n"
        "  - company_name: the company/organisation name only, without address or UID\n\n"
        "Respond ONLY with a JSON array of objects, one per entry, in order.\n"
        "Example: [{\"person_name\": \"Dr. Anna Meier\", \"company_name\": \"Roche AG\"}, ...]\n\n"
        "Entries:\n"
    )
    for i, entry in enumerate(entries, 1):
        prompt += f"{i}. {entry}\n"

    try:
        resp = client.chat.complete(
            model=MISTRAL_MODEL,
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0,
            max_tokens=2000,
        )
        raw = resp.choices[0].message.content.strip()
        # Strip markdown code fences if present
        raw = re.sub(r'^```[a-z]*\n?', '', raw).rstrip('`').strip()
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except Exception as e:
        print(f"    Mistral error: {e}", file=sys.stderr)
    return [{'person_name': '', 'company_name': ''} for _ in entries]


# ── Fuzzy dedup ────────────────────────────────────────────────────────────────

def build_canonical_map(names: list[str], freq: Counter, threshold: int) -> dict[str, str]:
    """Cluster company names by fuzzy similarity; return raw→canonical map."""
    canonical_map: dict[str, str] = {}

    # First pass: apply curated aliases
    for name in names:
        key = name.lower().strip().rstrip('.').strip()
        if key in CURATED:
            canonical_map[name] = CURATED[key]

    unresolved = [n for n in names if n not in canonical_map]
    print(f"  {len(canonical_map):,} names resolved by curated aliases")
    print(f"  {len(unresolved):,} names going into fuzzy clustering")

    if not HAS_FUZZY or not unresolved:
        for n in unresolved:
            canonical_map[n] = n
        return canonical_map

    # Sort by frequency (most common first = becomes canonical)
    unresolved_sorted = sorted(unresolved, key=lambda x: -freq.get(x, 0))

    assigned: dict[str, str] = {}  # name → canonical

    for name in unresolved_sorted:
        if name in assigned:
            continue
        # This name becomes a cluster head
        matches = rfprocess.extract(
            name, unresolved_sorted,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
            limit=50,
        )
        for match_name, _score, _ in matches:
            if match_name not in assigned:
                assigned[match_name] = name

    canonical_map.update(assigned)
    return canonical_map


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-mistral', action='store_true',
                        help='Skip Mistral LLM pass for ambiguous single-line entries')
    parser.add_argument('--no-uid', action='store_true',
                        help='Skip zefix UID lookup (offline / faster run)')
    parser.add_argument('--threshold', type=int, default=FUZZY_THRESHOLD,
                        help=f'Fuzzy match threshold (default: {FUZZY_THRESHOLD})')
    args = parser.parse_args()

    use_mistral = HAS_MISTRAL and not args.no_mistral
    mistral_client = Mistral(api_key=_MISTRAL_KEY) if use_mistral else None

    print(f"Reading {INPUT_CSV}...")
    rows = []
    fieldnames = None
    with open(INPUT_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        for row in reader:
            rows.append(row)
    print(f"Loaded {len(rows):,} rows. Fields: {len(fieldnames)}")

    # ── Phase 1: Parse blocks ─────────────────────────────────────────────────
    print('\nPhase 1: Parsing wirtschaftspartner blocks...')

    # Collect ambiguous single-line entries for optional Mistral pass
    ambiguous: list[tuple[int, str]] = []   # (row_index, raw_block)
    low_confidence_threshold = 0            # any single-line entry is ambiguous

    for idx, row in enumerate(rows):
        wirt = row.get('wirtschaftspartner', '') or ''
        if not wirt:
            row['wirt_person_name'] = ''
            row['wirt_company_raw'] = ''
            continue

        blocks = [b.strip() for b in wirt.split(BLOCK_SEP) if b.strip()]
        person, company = parse_block(blocks[0]) if blocks else ('', '')

        row['wirt_person_name'] = person
        row['wirt_company_raw'] = company

        # Flag single-line for optional LLM refinement
        if use_mistral and blocks and '\n' not in blocks[0]:
            ambiguous.append((idx, blocks[0]))

    parsed_count = sum(1 for r in rows if r.get('wirt_company_raw'))
    print(f"  Extracted company from {parsed_count:,} rows")

    # ── Phase 1b: Mistral refinement for single-line entries ──────────────────
    if use_mistral and ambiguous:
        print(f'\nPhase 1b: Mistral refinement for {len(ambiguous):,} single-line entries'
              f' in batches of {MISTRAL_BATCH}...')
        for batch_start in range(0, len(ambiguous), MISTRAL_BATCH):
            batch = ambiguous[batch_start:batch_start + MISTRAL_BATCH]
            raw_texts = [text for _, text in batch]
            results = mistral_parse(raw_texts, mistral_client)
            for (row_idx, _), result in zip(batch, results):
                rows[row_idx]['wirt_person_name'] = result.get('person_name', '').strip()
                rows[row_idx]['wirt_company_raw'] = result.get('company_name', '').strip()
            done = min(batch_start + MISTRAL_BATCH, len(ambiguous))
            print(f'  {done:,}/{len(ambiguous):,}', end='\r')
            time.sleep(0.2)  # gentle rate limiting
        print(f'  Done.                          ')
    elif not use_mistral:
        print('  (Mistral pass skipped)')

    # ── Phase 1.5: UID → official name via zefix ─────────────────────────────
    use_uid = not args.no_uid
    if use_uid:
        uid_rows = [(i, r) for i, r in enumerate(rows) if r.get('wirt_uid_url', '').strip()]
        print(f'\nPhase 1.5: zefix UID lookup for {len(uid_rows):,} rows...')
        uid_cache: dict[str, str] = {}
        uid_found = uid_tried = 0
        for i, (row_idx, row) in enumerate(uid_rows):
            m = RE_UID.search(row['wirt_uid_url'])
            if not m:
                row['_uid_name'] = ''
                continue
            uid_tried += 1
            name = lookup_zefix(m.group(), uid_cache)
            row['_uid_name'] = name
            if name:
                uid_found += 1
            time.sleep(UID_DELAY_S)
            if (i + 1) % 100 == 0 or (i + 1) == len(uid_rows):
                print(f'  {i+1:,}/{len(uid_rows):,}  ({uid_found:,} names resolved)', end='\r')
        print(f'\n  {uid_found:,} official names from zefix  '
              f'({uid_tried - uid_found} failed, {len(uid_cache)} unique UIDs queried)')
    else:
        print('\nPhase 1.5: zefix lookup skipped (--no-uid)')
        for row in rows:
            row['_uid_name'] = ''

    # ── Phase 2: Fuzzy dedup ──────────────────────────────────────────────────
    print('\nPhase 2: Fuzzy dedup of company names...')
    all_raw = [r['wirt_company_raw'] for r in rows if r.get('wirt_company_raw')]
    freq = Counter(all_raw)
    unique_names = list(freq.keys())
    print(f'  {len(unique_names):,} unique raw company names')

    canonical_map = build_canonical_map(unique_names, freq, args.threshold)

    n_clusters = len(set(canonical_map.values()))
    print(f'  Reduced to {n_clusters:,} canonical names')

    # ── Phase 3: Apply canonical map ─────────────────────────────────────────
    print('\nPhase 3: Applying canonical names...')
    uid_anchored = 0
    for row in rows:
        raw = row.get('wirt_company_raw', '')
        uid_name = row.pop('_uid_name', '')  # consume temp field

        if uid_name:
            # Prefer zefix official name; still apply curated alias if one matches
            curated_key = uid_name.lower().strip().rstrip('.')
            row['wirt_company_canonical'] = CURATED.get(curated_key, uid_name)
            uid_anchored += 1
        else:
            row['wirt_company_canonical'] = canonical_map.get(raw, raw) if raw else ''
    print(f'  {uid_anchored:,} rows got UID-anchored canonical name')

    # ── Phase 4: Write outputs ────────────────────────────────────────────────
    print(f'\nPhase 4: Writing outputs...')

    # company_aliases.csv — human-editable lookup table
    aliases_rows = sorted(
        ((raw, canonical_map.get(raw, raw), freq.get(raw, 0))
         for raw in unique_names),
        key=lambda x: x[0].lower()
    )
    with open(ALIASES_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['raw_name', 'canonical_name', 'frequency'])
        w.writerows(aliases_rows)
    print(f'  Wrote {ALIASES_CSV} ({len(aliases_rows):,} entries)')

    # aramis_cleaned.csv — full dataset with new columns
    new_cols = ['wirt_person_name', 'wirt_company_raw', 'wirt_company_canonical']
    out_fields = fieldnames + [c for c in new_cols if c not in fieldnames]
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=out_fields, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)
    print(f'  Wrote {OUTPUT_CSV}')

    # ── Sample output ─────────────────────────────────────────────────────────
    print('\n── Sample results (first 20 with partners) ───────────────────────')
    sample = [(r['wirt_person_name'], r['wirt_company_raw'], r['wirt_company_canonical'])
              for r in rows if r.get('wirt_company_raw')][:20]
    print(f"{'Person':<35} {'Company (raw)':<38} Canonical")
    print('─' * 100)
    for person, raw, canonical in sample:
        changed = '*' if raw != canonical else ' '
        print(f"{person[:34]:<35} {raw[:37]:<38}{changed}{canonical[:30]}")

    print('\nDone.')


if __name__ == '__main__':
    main()
