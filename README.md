# open-asset-data

Company-level emissions data pipeline for [open-asset.ai](https://open-asset.ai).

Collects, processes, and integrates emissions data from multiple sources for steel, cement, power, and fossil fuel companies.

## Data Sources

| Source | Type | Description |
|--------|------|-------------|
| GEM GIST | Asset-level | Plant capacity, technology, location (Global Iron & Steel Tracker) |
| Kampmann ALD | Asset-level | Baseline emissions and production (SteelALD, PowerALD) |
| Climate Trace | Satellite | Facility-level emissions estimates (2021-2025) |
| Annual Reports | Company | PDF extraction of reported emissions and production |
| Oxford NZT | Targets | Net-zero commitments and interim targets |
| TPI | Assessment | Transition Pathway Initiative carbon performance |
| SBTi | Targets | Science Based Targets initiative commitments |

## Pipeline Modules

```bash
cd open-asset-data

# Full pipeline: download reports, extract data, produce CSVs
python -m pipeline.orchestrator

# Multi-source integration (annual reports + Climate Trace + APA)
python -m pipeline.integrate

# Asset-based Planning Approach (GEM plant-level emissions)
python -m pipeline.apa_calculator

# Forward projections (BAU + transition plan scenarios)
python -m pipeline.steel_projections

# Cement/power NZT projections
python -m pipeline.cement_nzt
python -m pipeline.power_nzt
```

## Key Outputs

| File | Location | Description |
|------|----------|-------------|
| `steel_ald_combined.csv` | `data/processed/steel/` | ALD-format projections (consumed by CarbonBudgetTracker) |
| `steel_apa_emissions.csv` | `outputs/steel/` | Plant-level emissions estimates |
| `steel_gem_bau_annual.csv` | `outputs/steel/` | BAU emissions projections |
| `steel_gem_tp_annual.csv` | `outputs/steel/` | Transition plan projections |
| `extraction_quality_report.csv` | `outputs/cross_sector/` | Data quality assessment |

## Integration with CarbonBudgetTracker

This repo is consumed by [CarbonBudgetTracker](https://github.com/YOUR_ORG/CarbonBudgetTracker) as a git submodule. CBT reads the output CSVs to perform carbon budget analysis (SDA/PCP calculations against IAM pathways).

## Coverage

- **25 steel companies** (ArcelorMittal, Tata Steel, JSW Steel, Nippon Steel, POSCO, Nucor, SSAB, ThyssenKrupp, JFE, Gerdau, Salzgitter, Kobe Steel, voestalpine, BlueScope, US Steel, Cleveland-Cliffs, Steel Dynamics, Ternium, SAIL, China Steel, Severstal, Evraz, NLMK, Baoshan, Hyundai Steel)
- **229 company-year APA pairs** (2014-2024)
- **231 annual reports** with 10 specialized extractors

## Requirements

```
pandas
numpy
pdfplumber
requests
openpyxl
```
