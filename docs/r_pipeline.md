# R Pipeline Architecture

## Overview

The R pipeline (`r_pipeline/`) handles company-level data for fossil fuels, Australian power, and cross-sector datasets. It was migrated from CarbonBudgetTracker to enable open-asset-data to be a self-contained data processing repo.

```
r_pipeline/
  00_config.R          # Path setup, directory creation
  00_utilities.R       # Shared utility functions
  run_all.R            # Master runner (executes all scripts in order)
  import/              # Data extraction from raw sources
    nger_power.R       # Australian NGER facility data
    influencemap.R     # InfluenceMap/Heede fossil fuel emissions
    gem_reserves.R     # GEM oil, gas, coal reserves
    tpi_steel.R        # TPI steel assessments
    sbti.R             # SBTi target matching
    climatetrace/      # Climate Trace by sector (7 scripts)
  process/             # Data transformation and projections
    convert_to_EJ.R    # Production units -> exajoules
    nger_scenarios.R   # NGER BAU + transition plan scenarios
    nger_forward.R     # NGER forward projections to 2050
    fossil_fuel_nzt.R  # Oxford NZT target matching
  archive/             # Superseded scripts (for reference)
```

## Data Flow

```
Raw Data Sources                    Import Scripts               Processing Scripts           Final Outputs
================                    ==============               ==================           =============

NGER facility CSVs           -->  nger_power.R           -->  nger_scenarios.R      -->  nger_ald_scenarios_complete.csv
(data/raw/AU_NGER/)                   |                       nger_forward.R             nger_company_scenarios_*.csv
                                      +-->  nger_facilities.csv
                                      +-->  nger_corporate.csv

InfluenceMap emissions CSV   -->  influencemap.R         -->  convert_to_EJ.R       -->  influencemap_production_EJ.csv
(data/raw/InfluenceMap/)              |                                                  influencemap_*_summary_EJ.csv
                                      +-->  influencemap_emissions_clean.csv

GEM GOGET + GCMT Excel       -->  gem_reserves.R         -->  (matched to InfluenceMap)   influencemap_reserves_final.csv
(data/raw/GEM/)                                                                          gem_company_summary.csv

Oxford NZT                   -->  (loaded in process)    -->  fossil_fuel_nzt.R     -->  nzt_projections.csv

TPI steel CSV                -->  tpi_steel.R                                       -->  tpi_steel_assessments.csv
(data/raw/TPI/)

SBTi targets CSV             -->  sbti.R                                            -->  sbti_matched_targets.csv
(data/raw/ForwardLooking/)

Climate Trace v5.2           -->  climatetrace/*.R                                  -->  climatetrace_*_company_annual.csv
(data/raw/ClimateTrace/)              (7 sectors)                                        climatetrace_*_facility_annual.csv
```

## Key Design Decisions

### CSV Interface (not RDS)

All outputs are language-agnostic CSV files, not R-specific `.rds` objects. This allows:
- Python scripts to read the same outputs
- The website build script (TypeScript) to process them
- Easy inspection and debugging

### Dependency on conversion_factors.csv

The `convert_to_EJ.R` script requires `data/processed/fossil_fuels/conversion_factors.csv`, which contains energy conversion factors (coal, oil, gas -> EJ). This small reference file is committed to the repo rather than regenerated each run.

### InfluenceMap Processing

The InfluenceMap pipeline has two stages:

1. **Import** (`influencemap.R`): Reads the raw Heede database, cleans entity names, aggregates subsidiaries to parent level. Outputs emissions in MtCO2 and production in original units.

2. **EJ Conversion** (`convert_to_EJ.R`): Converts production from physical units (Mt coal, Mbbl oil, bcf gas) to exajoules using EI Statistics conversion factors. This is needed for the production-share allocation in CBT.

### NGER Processing

NGER data goes through three stages:

1. **Import** (`nger_power.R`): Reads 11 years of Australian designated generation facility CSVs, cleans company names using a manual mapping file, produces facility-level and corporate-level totals.

2. **Scenarios** (`nger_scenarios.R`): Creates BAU (constant production) and transition plan (linear closure) scenarios for each facility based on the Kampmann ALD methodology.

3. **Forward projections** (`nger_forward.R`): Projects facility-level scenarios to 2050, aggregates to company level, creates the `nger_ald_scenarios_complete.csv` that CBT reads for power sector analysis.

### GEM Reserves Matching

`gem_reserves.R` reads two GEM trackers:
- **GOGET** (Global Oil & Gas Extraction Tracker): Oil and gas reserves by field
- **GCMT** (Global Coal Mine Tracker): Coal reserves by mine

It parses ownership strings (e.g., "Shell [40%], BP [30%]"), allocates reserves proportionally, and matches companies to the InfluenceMap entity list using fuzzy name matching. The output (`influencemap_reserves_final.csv`) gives reserves-based decline curves for carbon budget calculations.

### SBTi Target Matching

`sbti.R` reads SBTi's published target list and matches company names to the entities used across the pipeline. The matching handles:
- Exact matches
- Common abbreviations (e.g., "Royal Dutch Shell" -> "Shell")
- Manual override mapping for difficult cases

Only the import/matching step lives here. Trajectory generation (turning targets into emission pathways) stays in CBT because it depends on company-specific production data (`company_pcp.rds`).

## Running

```bash
cd open-asset-data
Rscript r_pipeline/run_all.R    # ~1 minute
```

The runner executes scripts in dependency order:
1. All import scripts (independent, could run in parallel)
2. Processing scripts (depend on import outputs)

Outputs go to `outputs/` and `data/processed/` directories.
