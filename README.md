# ARAMIS (& CORDIS) Swiss Explorer

An interactive, public explorer for Swiss publicly funded research and innovation projects. It brings together **47,618 Swiss federal projects** from ARAMIS and **2,121 Horizon Europe projects with Swiss participation** from CORDIS, enriched with English abstracts, keywords and a technology-foresight layer based on the SATW Technology Outlook 2025.

The official ARAMIS search is hard to navigate and has no analytics. This tool turns the same open data into something you can search, filter, map and compare in a browser.

## What you can do

- **Search** in keyword mode (instant, client-side) or AI mode (Mistral expands the query into multilingual synonyms before filtering)
- **Filter** by funder, status, year range, confidence, SATW research field, canton and institution type
- **Results** — project cards with English abstract, keywords, SATW tags, budget and a detail view linking back to ARAMIS or CORDIS
- **Statistics** — funding over time, top funders, SATW distribution and top keywords, all recomputed for the active filters
- **SATW Foresight** — public funding mapped onto the 31 technologies of the SATW Technology Outlook 2025, with trends per technology
- **Map** — geographic view of projects by location
- **Shareable views** — the filter state is kept in the URL

## Data sources

| Source | Content | Licence / citation |
|---|---|---|
| [ARAMIS](https://www.aramis.admin.ch) | Research database of the Swiss Federal Administration: bulk export plus project detail pages (industry partners, scientific lead, multilingual abstracts) | Cite www.aramis.admin.ch as the source |
| [CORDIS](https://cordis.europa.eu) | EU research projects; Horizon Europe projects with at least one Swiss organisation | CC BY 4.0, © European Union |
| [SATW Technology Outlook 2025](https://technology-outlook.satw.ch) | Taxonomy of 31 emerging technologies in 5 research fields | Used as a classification scheme only |

## Enrichment

Each project was run through Mistral Small (`mistral-small-latest`) to add:

| Field | Description |
|---|---|
| `abstract_en` | Faithful English translation/condensation of the original abstract (ARAMIS only; CORDIS objectives are already in English) |
| `keywords` | 5–8 English keywords |
| `satw_2025` | Matching SATW 2025 technologies, empty if none clearly match |
| `confidence` | 0–1, reflects how rich the source text was (full abstract → high, short title only → low). It says nothing about research quality |
| `enrich_status` | `ok` / `failed` / `skip` |

Known limitations:
- About 39% of ARAMIS projects have no abstract at source; their enrichment relies on titles and has lower confidence.
- SATW tagging has an estimated false-positive rate of around 5%. **Industry 5.0 is likely over-represented**, and the dashboard flags this.
- Location (canton) is derived from postal codes in contact data and is available for about 40% of projects.
- ARAMIS budgets are in CHF, CORDIS budgets in EUR (`currency` column).
- Enriched text is AI-generated; check the original source for anything important.

## Repository layout

| File | Purpose |
|---|---|
| `index.html` | The whole dashboard: a single self-contained HTML/JS file. Served as the site entry point |
| `aramis_explorer.html` | Identical copy of `index.html` |
| `aramis_scraped.csv.gz` | ARAMIS bulk export + scraped detail-page fields + LLM enrichment (47,618 rows) |
| `cordis_enriched.csv.gz` | Swiss-involved Horizon Europe projects + enrichment (2,121 rows) |
| `cordis_pipeline.py` | Builds `cordis_enriched.csv` from the CORDIS bulk ZIP: Swiss filter, project-page fetch, Mistral enrichment |

The data files are gzipped and decompressed in the browser with `DecompressionStream`, so no build step is needed.

Not yet in the repository: the ARAMIS enrichment script (`enrich.py`) and the ARAMIS detail-page scraper.

## Running locally

```bash
python3 -m http.server 8000
# open http://localhost:8000
```

The page must be served over HTTP (not opened as `file://`) so it can fetch the CSV files.

Libraries are loaded from jsDelivr: PapaParse, Chart.js and Leaflet. There are no other dependencies.

## Rebuilding the CORDIS data

```bash
pip install requests
export MISTRAL_API_KEY=...            # see note below
python3 cordis_pipeline.py --no-llm --sample 20   # test the Swiss filter and join
python3 cordis_pipeline.py --sample 100           # test enrichment
python3 cordis_pipeline.py                        # full run
python3 cordis_pipeline.py --resume               # continue after an interruption
gzip -kf cordis_enriched.csv
```

## Deployment

Hosted on Cloudflare Pages, deployed from the `main` branch. Every push to `main` redeploys the site.

## AI search and API keys

AI search calls the Mistral API directly from the browser, so the key in `index.html` is public by design. It should be a **free-tier key used for nothing else**. Keys for the enrichment pipelines belong in environment variables, never in the repository.

## Credits

Built by Emile Dupont as a weekend project with Claude Code. Data © the Swiss Federal Administration (ARAMIS) and the European Union (CORDIS). SATW Technology Outlook © SATW.
