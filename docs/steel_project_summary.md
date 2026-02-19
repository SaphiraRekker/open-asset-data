# Steel APA Project Summary
## Carbon Budget Tracker - Steel Sector Analysis

*Last updated: February 2026*

> **Architecture note:** The Python pipeline and company-level data now live in the
> **open-asset-data** repository (sibling repo / git submodule). Paths below prefixed
> with `open-asset-data/` refer to that repo. R analysis scripts remain in CarbonBudgetTracker.

---

## 1. What We Built

### Multi-Source Steel Emissions Platform

A Python pipeline that estimates and cross-validates steel company CO2 emissions using four independent data sources:

| Source | Code | What it provides |
|--------|------|-----------------|
| **Annual Reports** | `annual_report` | PDF extraction of reported production & emissions |
| **Climate Trace** | `climate_trace` | Satellite/model-based facility-level emissions |
| **Kampmann ALD** | `kampmann_ald` | David Kampmann's original spreadsheet data |
| **APA Calculator** | `apa_gem` | Our implementation of David's APA method, extended |

### APA Calculator (Asset-level Production Approach)

Replicates and extends David Kampmann's methodology:

```
Emissions = Σ (Plant_Capacity × Utilization_Rate × Emission_Factor)
```

| | David's Original | This Pipeline |
|---|---|---|
| Companies | 10 | 18 |
| Years | 2020-2023 | 2015-2024 |
| Company-year calculations | 40 | 137 |
| Data sources | 1 (BAU) | 4 (BAU + annual reports + curated + ALD) |
| Cross-validation | Manual | Automated multi-source comparison |

**Key insight discovered:** Indian/Chinese DRI uses COAL (EF=3.10), not gas (EF=1.05). This was causing 50-70% underestimation for companies like JSW Steel.

### Validated Results

| Company | Production (Mt) | Calculated Emissions (Mt CO2) | Intensity (tCO2/t) |
|---------|-----------------|------------------------------|-------------------|
| JSW Steel | 24.14 | 74.3 | 3.07 |
| Tata Steel | 29.94 | 83.3 | 2.78 |
| ArcelorMittal | 68.89 | 122.4 | 1.78 |

These match David's results and make sense given company technology mixes.

---

## 2. Pipeline Architecture

```
DATA SOURCES                          PROCESSING                         OUTPUTS
───────────────────────────────────────────────────────────────────────────────

PDF Reports ──► Extractors ──► steel_all_extracted.csv ──┐
                                                          │
GEM GSPT Plants ──► APA Calculator ──────────────────────┤
                                                          ├──► integrate.py ──► steel_multi_source.csv
Climate Trace Data ──────────────────────────────────────┤                      steel_defaults.csv
                                                          │
Kampmann ALD CSV ────────────────────────────────────────┘
```

### Running the pipeline

```bash
# Step 1: Run Python pipeline (in open-asset-data repo)
cd open-asset-data

# Download reports and extract data from PDFs
python3 -m pipeline.orchestrator

# Integrate all sources into unified dataset
python3 -m pipeline.integrate

# Step 2: Run R analysis (in CarbonBudgetTracker)
cd ../CarbonBudgetTracker
Rscript 02.Scripts/03_analysis/05_company_sda_pcp.R
Rscript 02.Scripts/04_output/steel_comprehensive.R
```

---

## 3. Files

### Python Pipeline (`open-asset-data/pipeline/`)

| File | Purpose |
|------|---------|
| `config.py` | Paths, emission factors, constants |
| `models.py` | DataPoint, CompanyYearData, SourceInfo dataclasses |
| `downloader.py` | PDF download from company IR pages |
| `registry.py` | Company URL registry |
| `base_extractor.py` | Abstract base class for PDF extractors |
| `orchestrator.py` | Runs download + extraction pipeline |
| `apa_calculator.py` | APA emissions calculator (standalone module) |
| `integrate.py` | Multi-source integration pipeline |
| `extractors/` | 10 company-specific extractors + generic fallback |

### Output Data (`open-asset-data/`)

| File | Content |
|------|---------|
| `data/processed/steel/steel_ald_combined.csv` | Combined ALD for R pipeline input |
| `data/processed/steel/steel_production_from_reports.csv` | Curated production data (26 companies, 2014-2024) |
| `outputs/steel/steel_apa_emissions.csv` | APA results (229 rows, 26 companies) |
| `outputs/steel/climatetrace_steel_company_annual.csv` | ClimateTrace company-level aggregation |

### R Scripts (`02.Scripts/`)

| File | Purpose | Status |
|------|---------|--------|
| `02g_steel_apa_simple.R` | Original APA calculation (David's method) | Superseded by Python |
| `02d_import_tpi_steel.R` | Import TPI intensity data | Still used |
| `02g_steel_data_validation.R` | Compare calculated vs reported emissions | Still used |

### Documentation

| File | Location | Purpose |
|------|----------|---------|
| `apa_methodology.md` | `Docs/` | Core APA methodology |
| `steel_project_summary.md` | `Docs/` | This file — project overview |
| `steel_data_tracker.md` | `Docs/` | What's done, what's pending |
| `steel_data_dictionary.md` | `Docs/` | All variable definitions |
| `steel_workflow_simple.md` | `Docs/` | Day-to-day workflow |
| `steel_python_pipeline.txt` | `Docs/` | Technical Python pipeline reference |
| Python pipeline docs | `open-asset-data/docs/` | Detailed pipeline documentation |

---

## 4. Data Sources

### What We Have

| Source | Data | Location | Status |
|--------|------|----------|--------|
| **GEM/GSPT** | Plant capacities, EFs | `KampmannALD/Copy of 1_Output Sheet...xlsx` | Active - 18 companies matched |
| **Climate Trace** | Facility emissions 2021-2025 | `ClimateTrace/` | Active - 6,958 records, 696 facilities |
| **Kampmann ALD** | David's original calculations | `KampmannALD/` | Active - 120 records, 10 companies, 2020-2025 |
| **Curated production** | Manual compilation from WSA/reports | `steel_production_from_reports.csv` | Active - 156 rows, 18 companies, 2015-2023 |
| **Annual reports** | Extracted from downloaded PDFs | `01.Data/01.RawData/01. Company Data/*/` | Active - 50 records, 9 companies, 2015-2024 |
| **TPI** | Intensity 2013-2050 | `TPI/Latest_CP_Assessments.csv` | Available |

### Coverage by Company (18 companies)

| Company | APA Years | Production Source | Notes |
|---------|-----------|-------------------|-------|
| ArcelorMittal | 2020-2023 | BAU, press releases | Pre-2020: curated excluded (WorldSteel vs consolidated) |
| Nippon Steel | 2015-2023 | Curated, annual reports | |
| POSCO Holdings | 2015-2023 | Curated, BAU | |
| Tata Steel | 2015-2024 | Curated, BAU, annual reports | Indian FY format |
| JSW Steel | 2015-2024 | Curated, annual reports | Highest EF (Indian DRI coal) |
| JFE Holdings | 2015-2023 | Curated | |
| Nucor | 2015-2023 | Curated | EF underestimate: GSPT missing DRI plants |
| ThyssenKrupp | 2015-2024 | Curated, BAU, annual reports | |
| SSAB | 2015-2023 | Curated, BAU | |
| Gerdau | 2015-2023 | Curated, annual reports | |
| BlueScope Steel | 2015-2023 | Curated, BAU | |
| Severstal | 2015-2023 | Curated, BAU | |
| China Steel | 2015-2023 | Curated, BAU | Includes Dragon Steel subsidiary |
| Cleveland-Cliffs | 2021-2023 | Curated | Was iron ore company pre-2020 |
| Hyundai Steel | 2015-2023 | Curated | |
| voestalpine | 2019-2023 | Curated | Pre-2019 filtered (UR > 1.0) |
| Kobe Steel | 2020-2023 | Curated | Pre-2020 filtered (UR > 1.0) |
| Baoshan Iron & Steel | 2020-2022 | BAU | Only BAU production available |

---

## 5. Known Limitations

1. **Nucor EF underestimation**: All 17 GSPT plants are EAF (EF=0.04). GSPT doesn't include DRI plants, so emissions are ~10x too low.
2. **US Steel excluded**: Only 1 GSPT plant (0.7 Mt capacity) vs 11-17 Mt production. UR > 1.0 filter excludes all years.
3. **Static plant snapshot**: Current GSPT data used for all years (2015-2024). BF lifespans make this reasonable for recent history.
4. **ArcelorMittal pre-2020**: No APA calculations. Curated source excluded (WorldSteel vs consolidated mismatch), BAU only covers 2020-2023.

---

## 6. Next Steps

### Immediate

1. **Improve Tata Steel extractor** for multi-year table extraction (similar to ArcelorMittal)
2. **Re-run ArcelorMittal extraction** with updated multi-year extractor to populate per-year data points
3. **Add SSAB multi-year extraction** (reports contain emissions data from 2015-2024)

### Short-term

4. **Resolve ArcelorMittal pre-2020** by adding consolidated production figures to curated CSV
5. **Add US Steel plants** to GSPT coverage (or use manual capacity override)
6. **Integrate TPI intensity data** with the Python pipeline for Paris alignment assessment
7. **Add forward projection** (David Kampmann's method: BAU vs Stated Transition Plans vs SDA pathway)

### Medium-term

8. **Extend to more companies** from TPI's ~40 steel companies
9. **Add WSA PDF scraper** for automated production data updates
10. **Satellite validation** via Climate Trace facility-level comparison

---

## 7. Quick Reference

### Run APA Calculation (Python — in open-asset-data)

```bash
cd open-asset-data
python3 -m pipeline.integrate
```

### Expected Emission Factors

| Technology | Region | EF (tCO2/t) |
|------------|--------|-------------|
| BF-BOF | India | 3.72 |
| BF-BOF | China | 2.10 |
| BF-BOF | EU/Japan | 1.77-2.05 |
| BF-BOF | US | 1.30 |
| DRI (coal) | India/China | 3.10 |
| DRI (gas) | Iran/Americas | 1.05 |
| EAF | Global | 0.03-0.12 |
| H2-DRI | All | 0.04 |

### Full Documentation

See `open-asset-data/docs/` and `Docs/steel_python_pipeline.txt` for technical documentation including:
- Emission factor tables
- Company-to-GEM matching patterns
- Certainty scoring methodology
- PDF extractor architecture
- Maintenance procedures

---

*This document summarizes the steel sector analysis pipeline for the Carbon Budget Tracker project.*
