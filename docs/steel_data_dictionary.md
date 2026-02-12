# Steel Company Data Dictionary

## Overview

This document defines all variables in the steel company master dataset. Use this as a reference when collecting data or interpreting results.

**Last Updated:** January 2025  
**Maintained By:** Carbon Budget Tracker Team

---

## Variable Groups

| Group | Purpose | Source Priority |
|-------|---------|-----------------|
| **Identifiers** | Company identification | Internal |
| **Production** | Steel output for David's APA method | WSA → Annual Reports → Quarterly |
| **Reported Emissions** | What companies claim | CDP → Sustainability Reports → Annual Reports |
| **Climate Trace** | Satellite-based independent estimate | Climate Trace API |
| **Calculated (APA)** | Our calculated emissions | David's methodology |
| **Metadata** | Data quality tracking | Internal |

---

## Variable Definitions

### IDENTIFIERS

| Variable | Type | Description | Example |
|----------|------|-------------|---------|
| `company` | string | Company name (standardized) | "Tata Steel" |
| `company_id` | string | Short identifier for joins | "TATA" |
| `year` | integer | Calendar year (or FY end year for Indian companies) | 2023 |
| `country_hq` | string | Headquarters country | "India" |

**Notes on company naming:**
- Use WSA naming convention where possible
- For Indian companies, year = FY end year (FY2023-24 → 2024)
- Joint ventures: Use operating company name

---

### PRODUCTION DATA

| Variable | Type | Unit | Description |
|----------|------|------|-------------|
| `production_mt` | numeric | Million tonnes | Crude steel production |
| `production_source` | string | - | Data source |
| `production_notes` | string | - | Additional context |

**Valid sources (in priority order):**
1. `WSA 2024` - World Steel in Figures publication
2. `Annual Report` - Company annual/integrated report
3. `20-F` / `10-K` - SEC filings (for US-listed)
4. `Quarterly` - Quarterly production releases
5. `CDP` - CDP climate disclosure
6. `Estimate` - Our estimate (flag for review)

**What counts as "crude steel":**
- Liquid steel cast into semi-finished products
- Includes: slabs, blooms, billets
- Excludes: pig iron, DRI (unless converted to steel)

---

### REPORTED EMISSIONS

| Variable | Type | Unit | Description |
|----------|------|------|-------------|
| `reported_scope1_mt` | numeric | Mt CO2e | Direct emissions (company-reported) |
| `reported_scope2_mt` | numeric | Mt CO2e | Indirect from electricity (company-reported) |
| `reported_scope12_mt` | numeric | Mt CO2e | Scope 1 + Scope 2 combined |
| `reported_intensity_scope12` | numeric | tCO2e/t steel | Emissions intensity |
| `emissions_source` | string | - | Data source for emissions |
| `emissions_boundary` | string | - | What's included in the boundary |
| `emissions_notes` | string | - | Additional context |

**Valid sources:**
1. `CDP 2024` - CDP Climate Change questionnaire
2. `Sustainability Report 2023` - Annual sustainability report
3. `Integrated Report 2023-24` - Combined annual/sustainability report
4. `Climate Action Report` - Dedicated climate disclosure
5. `TPI` - Transition Pathway Initiative assessment

**Boundary definitions (critical!):**
- `Steel operations only` - Just steelmaking, excludes mining
- `Steel + mining` - Includes captive mines
- `Consolidated` - All subsidiaries
- `Domestic` - Home country only
- `Global` - Worldwide operations

**Common issues:**
- Some companies report CO2 only, others report CO2e (includes CH4, N2O)
- Market-based vs location-based Scope 2
- Equity share vs operational control

---

### CLIMATE TRACE DATA

| Variable | Type | Unit | Description |
|----------|------|------|-------------|
| `climatetrace_mt` | numeric | Mt CO2 | Satellite-derived emissions |
| `climatetrace_facilities` | integer | count | Number of facilities matched |
| `climatetrace_notes` | string | - | Matching notes |

**Notes:**
- Climate Trace provides facility-level estimates
- Need to match facilities to companies
- Data available 2021-2024 (varies by facility)
- See separate Climate Trace integration chat for methodology

---

### TPI DATA (Transition Pathway Initiative)

| Variable | Type | Unit | Description |
|----------|------|------|-------------|
| `tpi_intensity_historical` | numeric | tCO2/t steel | Latest reported emissions intensity |
| `tpi_intensity_2030` | numeric | tCO2/t steel | Projected intensity for 2030 |
| `tpi_intensity_2050` | numeric | tCO2/t steel | Projected intensity for 2050 |
| `tpi_alignment_2030` | string | - | Alignment status at 2030 |
| `tpi_alignment_2050` | string | - | Alignment status at 2050 |
| `tpi_mq_level` | integer | 0-5 | Management Quality level |
| `tpi_assessment_date` | date | - | Date of TPI assessment |
| `tpi_production_mt` | numeric | Mt steel | Production data (if available from TPI) |

**Data availability status:**
- ✅ **Have:** Intensity data, alignment scores, MQ levels (from public download)
- 🔄 **Requested:** Activity/production data (call scheduled with Nina & Carmen, Jan 2025)

**Why we need production data:**
TPI provides intensity (tCO2/t steel) but for carbon budget calculations we need absolute emissions:
```
Absolute emissions (Mt CO2) = Intensity (tCO2/t) × Production (Mt steel)
```
Without production, we can only compare intensities, not calculate cumulative budgets.

**Workaround if TPI can't share:**
Combine TPI intensity with WSA production data:
```r
absolute_emissions = tpi_intensity * wsa_production
```

**Data source:** 
- Download from: https://www.transitionpathwayinitiative.org/sectors/steel
- Click download icon → CP_Assessments Excel file
- Updated annually (typically Q3)

**TPI Alignment categories:**
- `1.5 Degrees` - Aligned with 1.5°C pathway
- `Below 2 Degrees` - Aligned with <2°C pathway  
- `National Pledges` - Aligned with national pledges only
- `Not Aligned` - Not aligned with any benchmark

**TPI Management Quality levels:**
- Level 0: Unaware of climate change as business issue
- Level 1: Acknowledges climate change
- Level 2: Building capacity
- Level 3: Integrating into operations
- Level 4: Strategic assessment
- Level 5: Climate transition leadership

**What TPI provides:**
- Emissions intensity (Scope 1+2 per tonne steel)
- Historical pathway + company targets
- Comparison against IEA scenarios
- Uses CDP data + company reports as sources

**Why TPI is valuable:**
- Independent third-party assessment
- Consistent methodology across companies
- Already uses SDA approach (same as our methodology)
- Covers 40 steel companies globally
- Updates annually

---

### COMPANY TRANSITION PLANS

| Variable | Type | Unit | Description |
|----------|------|------|-------------|
| `target_netzero_year` | integer | year | Net zero target year (if any) |
| `target_interim_year` | integer | year | Interim target year |
| `target_interim_reduction_pct` | numeric | % | Reduction % for interim target |
| `target_baseline_year` | integer | year | Baseline year for target |
| `target_baseline_emissions_mt` | numeric | Mt CO2 | Baseline emissions |
| `target_type` | string | - | "absolute" or "intensity" |
| `target_scope` | string | - | "Scope 1", "Scope 1+2", "Scope 1+2+3" |
| `target_source` | string | - | Where target was announced |
| `technology_plans` | string | - | Summary of announced tech changes |
| `capex_decarbonization` | numeric | USD millions | Committed decarbonization investment |
| `plant_closures_announced` | string | - | Announced BF/fossil plant closures |
| `green_projects_announced` | string | - | H2-DRI, CCS, EAF projects |
| `plan_credibility_notes` | string | - | Assessment of plan credibility |

**Why this matters:**
The CBT assesses companies against Paris-aligned budgets. To do this properly, we need to know:
1. What companies **say** they'll do (transition plans)
2. What they **need** to do (SDA/carbon budget)
3. The **gap** between plan and requirement

**Sources for transition plan data:**
- CDP Climate Change responses (Section C4 - Targets)
- Company sustainability/climate reports
- Investor presentations
- Press releases (technology announcements)
- Net Zero Tracker (netzerotracker.net)
- SBTi target database

**Key questions to answer:**
- Does the company have a net zero target? By when?
- What interim milestones have they set?
- What technology changes are planned (BF→EAF, H2-DRI, CCS)?
- Have they committed capital to decarbonization?
- Are announced plant closures consistent with their targets?

| Variable | Type | Unit | Description |
|----------|------|------|-------------|
| `calculated_apa_mt` | numeric | Mt CO2 | Emissions from David's method |
| `calculated_apa_ef` | numeric | tCO2/t | Weighted average emission factor |
| `calculated_apa_notes` | string | - | Calculation notes |

**Methodology:**
1. Production × Emission Factor = Emissions
2. EF assigned by country × process (see emission factor tables)
3. Plants allocated using uniform utilization rate
4. See `02g_steel_apa_simple.R` for implementation

**When calculated ≠ reported:**
- Boundary differences (we calculate steel only)
- Grid carbon intensity differences
- Process classification errors
- Actual vs nameplate capacity

---

### METADATA

| Variable | Type | Description |
|----------|------|-------------|
| `data_quality_flag` | string | Quality indicator |
| `last_updated` | date | When record was last updated |
| `updated_by` | string | Who updated the record |

**Quality flags:**
- `complete` - All key fields populated
- `partial` - Missing some emissions data
- `production_only` - Only production data available
- `estimated` - Contains estimated values
- `stale` - Data > 2 years old
- `review` - Needs manual review (discrepancies)

---

## Validation Rules

### Cross-checks to perform:

1. **Intensity sanity check:**
   - BF-BOF heavy: 1.5 - 2.5 tCO2/t
   - EAF heavy: 0.3 - 1.0 tCO2/t
   - Mixed: 1.0 - 2.0 tCO2/t

2. **Calculated vs Reported:**
   - Flag if difference > 30%
   - Investigate boundary differences

3. **Year-over-year:**
   - Production change > 20% → verify
   - Emissions change > 25% → verify

4. **Climate Trace vs Reported:**
   - Should be in same ballpark
   - CT may miss some facilities

---

## Data Collection Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: Production Data                                         │
│ ─────────────────────────                                       │
│ • Download WSA World Steel in Figures (annual, June)            │
│ • Fill in production_mt and production_source                   │
│ • For non-top-50, check company annual reports                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: Reported Emissions                                      │
│ ────────────────────────────                                    │
│ • Check CDP (if disclosed) - most standardized                  │
│ • Check Sustainability Report - usually in "Environment" section│
│ • Note the BOUNDARY carefully                                   │
│ • Fill in reported_scope1_mt, reported_scope12_mt, etc.         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 3: Climate Trace                                           │
│ ───────────────────────                                         │
│ • Run Climate Trace extraction script (separate chat)           │
│ • Match facilities to companies                                 │
│ • Fill in climatetrace_mt                                       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 4: Calculate APA Emissions                                 │
│ ────────────────────────────────                                │
│ • Run 02g_steel_apa_simple.R                                    │
│ • Uses production_mt + GEM plants + emission factors            │
│ • Fills in calculated_apa_mt, calculated_apa_ef                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 5: Validation                                              │
│ ──────────────────                                              │
│ • Compare calculated vs reported                                │
│ • Compare Climate Trace vs reported                             │
│ • Flag discrepancies > 30% for review                           │
│ • Update data_quality_flag                                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Source URLs

| Source | URL | Update Frequency |
|--------|-----|------------------|
| WSA Top Producers | https://worldsteel.org/data/top-steel-producers/ | Annual (June) |
| WSA Steel in Figures | https://worldsteel.org/data/world-steel-in-figures/ | Annual (June) |
| CDP | https://www.cdp.net/en/responses | Annual (varies) |
| Climate Trace | https://climatetrace.org/inventory | Annual |
| TPI | https://www.transitionpathwayinitiative.org/ | Periodic |

### Company-specific:

| Company | IR/Sustainability URL |
|---------|----------------------|
| Tata Steel | https://www.tatasteel.com/investors/ |
| ArcelorMittal | https://corporate.arcelormittal.com/investors |
| JSW Steel | https://www.jsw.in/investors/steel |
| POSCO | https://www.posco.co.kr/homepage/docs/eng6/jsp/ir/ |
| Nippon Steel | https://www.nipponsteel.com/en/ir/ |
| SAIL | https://sail.co.in/en/investor |
| ThyssenKrupp | https://www.thyssenkrupp.com/en/investors |
| SSAB | https://www.ssab.com/en/company/investors |
| Cleveland-Cliffs | https://www.clevelandcliffs.com/investors |

---

## Revision History

| Date | Change | By |
|------|--------|-----|
| 2025-01 | Initial version | CBT |

