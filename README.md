# open-asset-data

Company-level emissions data pipeline for [open-assets.ai](https://open-assets.ai).

Collects, processes, and integrates emissions data from multiple sources for steel, cement, power, and fossil fuel companies. This repo handles all **company-level data extraction and processing**; carbon budget analysis is done downstream by [CarbonBudgetTracker](https://github.com/SaphiraRekker/CarbonBudgetTracker).

## Data Sources

| Source | Sectors | Type | Description |
|--------|---------|------|-------------|
| [InfluenceMap / Heede](https://influencemap.org) | Fossil fuels | Historical emissions | Company-level production & emissions (1854-2023) |
| [GEM](https://globalenergymonitor.org) | Fossil fuels, Steel | Asset-level | Plant capacity, reserves, technology, location |
| Kampmann ALD | Steel, Power | Asset-level | Baseline emissions and production (SteelALD, PowerALD) |
| [Climate Trace](https://climatetrace.org) | All sectors | Satellite | Facility-level emissions estimates (2021-2025) |
| Annual Reports | Steel | Company | PDF extraction of reported emissions and production |
| [Oxford NZT](https://zerotracker.net) | Fossil fuels | Targets | Net-zero commitments and interim targets |
| [TPI](https://www.transitionpathwayinitiative.org) | Steel | Assessment | Transition Pathway Initiative carbon performance |
| [SBTi](https://sciencebasedtargets.org) | All sectors | Targets | Science Based Targets initiative commitments |
| [NGER](https://www.cleanenergyregulator.gov.au) | Power | Regulatory | Australian National Greenhouse & Energy Reporting |

## Pipelines

This repo has two pipelines:

### Python Pipeline (Steel)

Handles steel data extraction, multi-source integration, and APA calculations.

```bash
python -m pipeline.orchestrator        # Full pipeline: download reports, extract, produce CSVs
python -m pipeline.integrate           # Multi-source integration (annual reports + Climate Trace + APA)
python -m pipeline.apa_calculator      # Asset-based Planning Approach (GEM plant-level emissions)
python -m pipeline.steel_projections   # Forward projections (BAU + transition plan scenarios)
python -m pipeline.cement_nzt          # Cement NZT projections
python -m pipeline.power_nzt          # Power NZT projections
```

### R Pipeline (Fossil Fuels, Power, Cross-sector)

Handles InfluenceMap, GEM reserves, NGER, SBTi, Climate Trace, and EJ conversion.

```bash
Rscript r_pipeline/run_all.R           # Full pipeline (~1 min)
```

**Import scripts** (`r_pipeline/import/`):
| Script | Source | Output |
|--------|--------|--------|
| `nger_power.R` | NGER designated generation facility CSVs | `outputs/power/nger_*.csv` |
| `influencemap.R` | InfluenceMap emissions_high_granularity.csv | `outputs/fossil_fuels/influencemap_*.csv` |
| `gem_reserves.R` | GEM GOGET (Oil & Gas) + GCMT (Coal) trackers | `outputs/fossil_fuels/gem_*.csv` |
| `tpi_steel.R` | TPI steel assessment CSV | `outputs/steel/tpi_*.csv` |
| `sbti.R` | SBTi targets CSV + company name matching | `data/processed/sbti/sbti_matched_targets.csv` |
| `climatetrace/*.R` | Climate Trace v5.2 emissions + ownership | `outputs/*/climatetrace_*.csv` |

**Processing scripts** (`r_pipeline/process/`):
| Script | What it does | Output |
|--------|-------------|--------|
| `convert_to_EJ.R` | Converts InfluenceMap production to exajoules | `outputs/fossil_fuels/influencemap_production_EJ.csv` |
| `nger_scenarios.R` | Creates BAU + transition scenarios for NGER power | `outputs/power/nger_ald_scenarios_complete.csv` |
| `nger_forward.R` | Forward-projects NGER facilities to 2050 | `outputs/power/nger_company_scenarios_*.csv` |
| `fossil_fuel_nzt.R` | Matches NZT targets to InfluenceMap companies | `data/processed/fossil_fuels/nzt_projections.csv` |

## Key Outputs

### Steel (Python pipeline)

| File | Location | Description |
|------|----------|-------------|
| `steel_ald_combined.csv` | `data/processed/steel/` | ALD-format projections consumed by CBT |
| `steel_apa_emissions.csv` | `outputs/steel/` | Plant-level emissions estimates (APA) |
| `steel_company_info.csv` | `outputs/steel/` | Company metadata and data sources |
| `extraction_quality_report.csv` | `outputs/cross_sector/` | Data quality scoring |

### Fossil Fuels (R pipeline)

| File | Location | Description |
|------|----------|-------------|
| `influencemap_production_EJ.csv` | `outputs/fossil_fuels/` | Production in EJ by company, fuel, year (1854-2023) |
| `influencemap_emissions_clean.csv` | `outputs/fossil_fuels/` | Emissions in MtCO2 by company, fuel, year |
| `influencemap_reserves_final.csv` | `outputs/fossil_fuels/` | GEM reserves matched to InfluenceMap companies |
| `nzt_projections.csv` | `data/processed/fossil_fuels/` | Oxford NZT emission pathways for fossil fuel companies |

### Power (R pipeline)

| File | Location | Description |
|------|----------|-------------|
| `nger_facilities.csv` | `outputs/power/` | Australian facility-level emissions & generation |
| `nger_corporate.csv` | `outputs/power/` | Corporate totals (aggregated from facilities) |
| `nger_ald_scenarios_complete.csv` | `outputs/power/` | BAU + transition plan scenarios to 2050 |

### Cross-sector (R pipeline)

| File | Location | Description |
|------|----------|-------------|
| `sbti_matched_targets.csv` | `data/processed/sbti/` | SBTi targets matched to pipeline company names |
| `climatetrace_*_company_annual.csv` | `outputs/*/` | Climate Trace data for steel, cement, power, aluminum, petrochemical, chemicals, pulp & paper |

## Integration with CarbonBudgetTracker

This repo is symlinked into CBT at `open-asset-data/`. CBT reads the CSV outputs via an adapter layer (`load_company_data.R`) to perform carbon budget analysis (ACA/SDA/PCP calculations against IAM pathways).

**Workflow:**
1. Run `open-asset-data` pipelines to produce company CSVs
2. Run CBT's `00_RUN_ALL.R` which reads from `open-asset-data/outputs/`
3. CBT generates final budget allocations and website data

## Coverage

- **165 fossil fuel companies** (InfluenceMap/Heede historical emissions database)
- **75 fossil fuel companies** with GEM reserves data
- **36 fossil fuel companies** with Oxford NZT projections
- **26 steel companies** with multi-source emissions data
- **324 Australian power companies** (NGER reporting entities)
- **7 sectors** of Climate Trace data (steel, cement, power, aluminum, petrochemical, chemicals, pulp & paper)

## Documentation

| Document | Description |
|----------|-------------|
| [`docs/apa_methodology.md`](docs/apa_methodology.md) | Asset-based Planning Approach for plant-level emissions |
| [`docs/data_quality_methodology.md`](docs/data_quality_methodology.md) | Multi-source data quality scoring framework |
| [`docs/steel_data_dictionary.md`](docs/steel_data_dictionary.md) | Steel company dataset variable reference |
| [`docs/steel_data_tracker.md`](docs/steel_data_tracker.md) | Data collection status by source |
| [`docs/r_pipeline.md`](docs/r_pipeline.md) | R pipeline architecture and data flow |

## Requirements

**Python** (steel pipeline):
```
pandas, numpy, pdfplumber, requests, openpyxl
```

**R** (fossil fuel/power pipeline):
```
tidyverse, readr, dplyr, tidyr, readxl, here, janitor, zoo
```
