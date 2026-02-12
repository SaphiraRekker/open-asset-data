# APA Methodology - Asset-based Planning Approach

## What is APA?

**APA (Asset-based Planning Approach)** is David Kampmann's methodology from his 2024 paper for calculating company-level emissions projections based on physical asset data.

**Key insight:** Instead of relying on corporate targets or intensity benchmarks, APA models emissions from the bottom up using actual plant-level data: which plants exist, what technology they use, where they are located, and how much they produce.

---

## Core Calculation

```
Plant Emissions = Allocated Production × Emission Factor
                = (Plant Capacity / Total Company Capacity) × Company Production × EF(country, technology, year)

Company Emissions = Sum of all plant emissions under that ownership
Weighted EF = Company Emissions / Company Production
```

The utilisation rate (UR = production / total capacity) distributes reported production across plants proportionally to their capacity share.

---

## Steel APA Implementation (Current)

### Data Source: GEM GIST December 2025

Three standalone Excel files from the Global Energy Monitor's Global Iron and Steel Tracker:

| File | Contents | Key Fields |
|------|----------|------------|
| `Plant-level-data-Global-Iron-and-Steel-Plant-Tracker-December-2025.xlsx` | Plant data + Plant production | plant_id, plant_name, country, parent, owner, status, start_year |
| `Iron-unit-level-data-Global-Iron-and-Steel-Plant-Tracker-December-2025.xlsx` | Iron-making units (BF, DRI) | plant_id, unit_name, technology, capacity_ttpa, status |
| `Steel-unit-level-data-Global-Iron-and-Steel-Plant-Tracker-December-2025.xlsx` | Steel-making units (BOF, EAF) | plant_id, unit_name, technology, capacity_ttpa, status |

**Plant count:** ~800+ plant-status entries after aggregation (operating, retired, construction).

### 26 Companies Tracked

Original 14 from Kampmann + 12 expanded:

| Company | GEM Pattern | Key Technology | Typical wEF (2020) |
|---------|-------------|---------------|-------------------|
| ArcelorMittal | `ArcelorMittal` | Mixed BF-BOF/EAF | ~1.78 |
| Tata Steel | `Tata Steel` | BF-BOF (India/UK) | ~2.65 |
| POSCO Holdings | `Posco\|POSCO` | BF-BOF (Korea) | ~1.64 |
| Nippon Steel | `Nippon Steel` | BF-BOF (Japan) | ~1.97 |
| JSW Steel | `JSW Steel\|JSW Ispat` | BF-BOF (India) | ~3.05 |
| ThyssenKrupp | `thyssenkrupp\|ThyssenKrupp` | BF-BOF (EU) | ~1.63 |
| SSAB | `SSAB` | Mixed (Sweden/US) | ~1.09 |
| Severstal | `Severstal` | BF-BOF (Russia) | ~2.79 |
| Baoshan Iron & Steel | `Baoshan\|Baowu` | BF-BOF (China) | ~1.88 |
| BlueScope Steel | `BlueScope` | Mixed (Australia) | ~0.87 |
| China Steel | `China Steel\|Dragon Steel` | BF-BOF (Taiwan) | ~1.93 |
| Nucor | `Nucor` | 100% EAF (US) | 0.04 |
| Gerdau | `Gerdau` | Mixed EAF/BF (Brazil) | ~0.70 |
| JFE Holdings | `JFE` | BF-BOF (Japan) | ~1.55 |
| US Steel | `U.S. Steel\|United States Steel` | Mixed (US) | ~1.45 |
| Hyundai Steel | `Hyundai Steel` | Mixed (Korea) | ~1.34 |
| Cleveland-Cliffs | `Cleveland.Cliffs\|AK Steel` | BF-BOF (US) | ~1.63 |
| Kobe Steel | `Kobe Steel\|KOBELCO` | Mixed (Japan) | ~1.23 |
| voestalpine | `voestalpine` | BF-BOF (Austria) | ~1.41 |
| SAIL | `Steel Authority\|SAIL` | BF-BOF (India) | ~3.58 |
| Steel Dynamics | `Steel Dynamics\|SDI` | EAF (US) | ~0.31 |
| Salzgitter | `Salzgitter` | BF-BOF (Germany) | ~1.55 |
| Ternium | `Ternium` | Mixed (Mexico/Argentina) | ~1.73 |
| NLMK | `NLMK\|Novolipetsk` | BF-BOF (Russia) | ~2.62 |
| Evraz | `Evraz` | BF-BOF (Russia) | ~2.23 |
| Liberty Steel | `Liberty Steel\|GFG Alliance` | EAF (UK/Australia) | ~0.40 |

**Result:** 229 company-year APA pairs covering 2014-2024.

---

## Emission Factors

### Source
Koolen & Vidovic (2022) **JRC129297** — "Greenhouse gas intensities of the EU steel industry and its trading partners", Table A4.

### BF-BOF (Scope 1 + Upstream)
Country-specific, **time-varying** with 0.5%/year compound improvement from 2020 reference:

```
EF(country, year) = EF_base(country) × (1 - 0.005)^(year - 2020)
```

| Region | Base EF (2020) | 2014 EF | 2024 EF |
|--------|---------------|---------|---------|
| EU | 1.770 | 1.824 | 1.735 |
| China | 1.760 | 1.814 | 1.725 |
| India | 3.720 | 3.834 | 3.646 |
| Japan | 2.050 | 2.113 | 2.009 |
| United States | 1.940 | 1.999 | 1.902 |
| Brazil | 2.190 | 2.257 | 2.147 |
| South Korea | 2.000 | 2.061 | 1.960 |
| Russia | 2.790 | 2.875 | 2.735 |
| Global (fallback) | 2.314 | 2.385 | 2.268 |

**Rationale for 0.5%/yr:** Worldsteel Sustainability Indicators (2025) show global BF-BOF intensity is largely flat over 2014-2024 (<5% total change). The IEA Iron & Steel Technology Roadmap assumes 0.3-0.7% annual improvement. We use 0.5% as a middle estimate. EFs for years before 2020 are *higher* than the reference (less efficient historically).

### EAF (Scope 1 only) — Static
EAF scope 1 emissions are very small (0.02-0.12 tCO2/t) and changes are negligible. Kept constant across all years.

| Key Regions | EF |
|------------|-----|
| United States | 0.04 |
| EU | 0.04 |
| South Korea | 0.03 |
| India | 0.07 |
| South Africa | 0.12 |
| Global | 0.051 |

### DRI — Static (technology-specific)
| Type | EF | Countries |
|------|-----|-----------|
| Coal-based | 3.10 | India, China, South Africa |
| Gas-based | 1.05 | All others |
| Hydrogen | 0.04 | Future technology |

### Country-to-Region Mapping
92+ countries are mapped to the 18 JRC EF regions via `COUNTRY_TO_EF_REGION`. Proxy mappings include: Canada → US, EU members → EU, Indonesia → India, Vietnam → China, etc.

---

## Production Data Hierarchy

Five sources, prioritised (lower number = higher priority):

| Priority | Source | Label | Coverage |
|----------|--------|-------|----------|
| 0 | Kampmann BAU sheets | `bau_reported` | 14 companies, 2020-2023 |
| 1 | Annual report extraction | `annual_report` | Varies by company |
| 2 | Curated reports CSV | `curated_reports` | 26 companies, 2014-2024 |
| 3 | Kampmann ALD data | `ald` | 14 companies, 2020-2023 |
| 4 | GEM plant production | `gem_plant_level` | Variable (50% coverage floor) |

For each (company, year), the highest-priority available value is used.

### GEM Plant Production (Priority 4)
The GEM Plant Production sheet contains per-plant crude steel output (ttpa) for 2019-2024. Coverage is highly variable:
- 2019-2023: 226-267 plants reported
- 2024: Only 8 plants reported

**Coverage floor filter:** If <50% of a company's plants reported production in a given year (vs the company's best-covered year), that year is skipped. This prevents garbage aggregate values from sparse data (e.g., ArcelorMittal 2024 had only 8 of 47 plants → 3.6 Mt → filtered out).

### ArcelorMittal Production
The curated file uses **consolidated crude steel production** from Q4 earnings press releases (93.1 Mt in 2014), not WorldSteel figures that include joint ventures (98.1 Mt). This is important because WSA counts equity share of JVs like AMNS India and Calvert, while the consolidated figure reflects only majority-owned operations.

---

## Year-Specific Plant Filtering

Plants are filtered per year based on GEM status and start_year:

| Status | Rule |
|--------|------|
| Operating / Pre-retirement | Include if start_year ≤ year |
| Construction | Include if start_year ≤ year (came online) |
| Retired / Mothballed | Include for all historical years (no close date in GEM plant file) |
| Announced / Cancelled | Always exclude |

This means the plant set changes over time: new plants come online, and the technology mix shifts. This is the **first-order effect** on weighted EF — much larger than the 0.5%/yr EF trend.

---

## Ownership Transfers

Corporate acquisitions that change GEM's Parent field are handled with year-aware filtering:

| Transfer | Year | Mechanism |
|----------|------|-----------|
| US Steel → Nippon Steel | Dec 2024 | Before 2024: US Steel plants found via `plant_name` fallback. After 2024: included in Nippon Steel's parent match. US Steel plants excluded from Nippon Steel count pre-2024. |

**Implementation:** `COMPANY_PLANT_NAME_PATTERNS` for fallback matching + `OWNERSHIP_TRANSFERS` list for year-based exclusion from acquirer.

---

## Projection Scenarios

### BAU (Business as Usual)
- Current plants continue operating until end of standard asset lifetime
- No technology changes assumed
- Six variants: `bau_ald`, `bau_closure`, `bau_constant`, `bau_recent`, `bau_longterm`, `bau_reserves_decline`

### TP (Transition Pathway) — Based on Announced Closures
- Models specific announced changes from company reports
- Plant closures with dates, technology conversions, new capacity, CCS
- **Default display:** `tp_cumulative_closure`

### Comparison TP Sources
- **NZT (Net Zero Tracker):** Corporate net-zero targets (top-down)
- **SBTi:** Science Based Targets initiative validated targets
- **Company Targets:** Self-reported targets from sustainability reports

---

## Data Hierarchy (All Sectors)

Each sector has one **default** emissions source displayed in `actual_annual_emissions`, plus comparison sources in `comparison_emissions_*` columns:

| Sector | Default Source | Comparison Sources |
|--------|---------------|-------------------|
| **Steel** | Kampmann ALD | APA, ClimateTrace, Company Reports |
| **Power** | Kampmann ALD | ClimateTrace, Company Reports |
| **Cement** | Kampmann ALD | ClimateTrace, Company Reports |
| **Fossil Fuels** | InfluenceMap | GEM/Urgewald, Company Reports |

**TP hierarchy:** `tp_cumulative_closure` (default) > NZT > SBTi > Company Targets

### Comparison Columns (5 total, 422 columns across all CSVs)
- `comparison_emissions_apa` — APA plant-level calculation (steel only)
- `comparison_emissions_climatetrace` — ClimateTrace facility estimates
- `comparison_emissions_ald` — Kampmann ALD pipeline value
- `comparison_emissions_company_reports` — Self-reported Scope 1
- `comparison_emissions_influencemap` — InfluenceMap production data (fossil fuels)

---

## Key Files

### Steel APA Pipeline
| File | Purpose |
|------|---------|
| `02.Scripts/scripts_python/report_pipeline/apa_calculator.py` | Core APA: plant loading, EF assignment, emissions calculation |
| `02.Scripts/scripts_python/report_pipeline/ownership_mapping.py` | Ownership transparency: equity shares, Kampmann cross-check, mismatch flagging |
| `02.Scripts/scripts_python/report_pipeline/config.py` | BF-BOF EF dictionary, file paths |
| `02.Scripts/scripts_python/report_pipeline/integrate.py` | Company name harmonisation (COMPANY_CANONICAL) |
| `01.Data/02.Processed/steel/steel_production_from_reports.csv` | Curated production data (26 companies, 2014-2024) |
| `03.Outputs/01.CompanyData/steel/steel_apa_emissions.csv` | APA results: 229 rows, 26 companies |
| `03.Outputs/01.CompanyData/steel/steel_ownership_mapping.csv` | Per-company-year plant ownership with equity shares and cross-checks |
| `03.Outputs/01.CompanyData/steel/steel_ownership_mismatches.csv` | Flagged discrepancies for human review |

### Comprehensive Output Pipeline
| File | Purpose |
|------|---------|
| `02.Scripts/04_output/00_column_standard.R` | 422-column standard + data hierarchy docs |
| `02.Scripts/04_output/steel_comprehensive.R` | Steel metrics + comparison column merge |
| `02.Scripts/04_output/power_comprehensive.R` | Power sector metrics |
| `02.Scripts/04_output/cement_comprehensive.R` | Cement sector metrics |
| `02.Scripts/04_output/fossilfuel_comprehensive.R` | Fossil fuel sector metrics |
| `02.Scripts/04_output/data_quality_check.R` | Cross-sector validation (EXPECTED_COLS=422) |

### GEM GIST Data Files
| File | Contents |
|------|----------|
| `Plant-level-data-...-December-2025.xlsx` | Plant data + Plant production (crude steel ttpa) |
| `Iron-unit-level-data-...-December-2025.xlsx` | BF and DRI unit capacities |
| `Steel-unit-level-data-...-December-2025.xlsx` | BOF and EAF unit capacities |

---

## Ownership Mapping & Transparency

### Why This Matters
Kampmann's methodology requires cross-referencing GEM plant data with annual reports to ensure the correct consolidation boundary. A company's "production" figure depends critically on which plants are included and at what equity share. Differences in consolidation (full vs equity-share vs proportionate) can cause 10-20% production discrepancies.

### How It Works
The `ownership_mapping.py` module generates two CSVs every pipeline run:

**`steel_ownership_mapping.csv`** — Every plant matched to every company for every year:
- `equity_share`: Parsed from GEM Parent field `[XX.X%]` notation
- `in_kampmann`: Whether this plant appears in Kampmann's Mar 2023 plant list
- `kampmann_ownership_share`: Kampmann's recorded equity share (for comparison)
- `flags`: Mismatch indicators for human review

**`steel_ownership_mismatches.csv`** — Flagged discrepancies:

| Flag Type | Meaning | Action |
|-----------|---------|--------|
| `NOT_IN_KAMPMANN` | Plant in GEM Dec 2025 but not in Kampmann Mar 2023 | Check if new plant or GEM vintage difference |
| `IN_KAMPMANN_NOT_GEM` | Plant in Kampmann but not matched in our GEM | Check name changes or pattern updates needed |
| `EQUITY_MISMATCH` | GEM and Kampmann disagree on equity share | Verify from annual report |
| `EQUITY_UNKNOWN` | GEM Parent field has no [XX.X%] for this company | Look up in annual report |
| `MINORITY_STAKE` | Equity below 50% | Check if consolidated or equity-method accounting |

### Cross-referencing Approach
GEM Plant IDs changed completely between vintages (Mar 2023: `SFI00001` → Dec 2025: `P100000120468`). Cross-referencing uses **plant name + country fuzzy matching** with 3-tier fallback:
1. Exact normalised name + same country
2. Substring containment + same country
3. Key location word overlap + same country

### Current Statistics (2020 base year)
- 228 plant-entries across 26 companies
- 98 unique mismatch flags requiring review
- 23 equity mismatches between GEM versions
- 21 plants in Kampmann but not matched in GEM (vintage differences)
- 16 minority stakes flagged for consolidation check

---

## What APA is NOT

- **NOT** TPI intensity benchmarks (those are sector-wide, not asset-level)
- **NOT** corporate target tracking (that's NZT — useful for comparison only)
- **NOT** satellite emissions estimates (that's ClimateTrace — useful for validation)
- **NOT** scenario pathway alignment (that's the PCP/SDA/ACA analysis that *uses* APA as input)

APA is purely: **Assets → Emission Factors → Baseline Emissions, driven by physical plant data and reported production.**

---

*Last updated: February 2025 (Session 19)*
