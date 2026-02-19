# Steel Data Collection - Master Tracker

## Overview

This tracks all data workstreams for steel company analysis in the Carbon Budget Tracker.

**Goal:** Comprehensive steel company emissions data from multiple sources for validation and analysis.

---

## Data Sources Status

| Source | Data Type | Status | Chat/Script | Notes |
|--------|-----------|--------|-------------|-------|
| **GEM Plant Tracker** | Plant capacities, processes | ✅ Complete | In Kampmann Excel | 2023 data |
| **WSA Production** | Company production (Mt) | ✅ Template ready | This chat | Manual entry from PDFs |
| **Company Reports** | Production + reported emissions | 🔄 In progress | This chat | Need to scrape |
| **CDP** | Reported emissions (standardized) | 📋 Planned | - | Need API access |
| **Climate Trace** | Satellite emissions | 🔄 Separate chat | See CT chat | Facility matching needed |
| **TPI** | Intensity + alignment + MQ scores | 🔄 In contact | This chat | Have intensity; awaiting activity data (call scheduled with Nina/Carmen) |
| **David's APA** | Calculated emissions | ✅ Complete | `open-asset-data/pipeline/apa_calculator.py` | Validated methodology (26 companies) |
| **Net Zero Tracker** | Company targets | 🔄 Have data | Separate chat | Need to integrate |
| **Company Transition Plans** | Technology roadmaps, capex, closures | 📋 To scrape | This chat | Scrape alongside emissions |

---

## TPI Data Collection

**URL:** https://www.transitionpathwayinitiative.org/sectors/steel

**Current status:** 
- ✅ Downloaded CP_Assessments (intensity data back to 2013)
- ✅ Downloaded MQ_Assessments (Management Quality scores)
- 🔄 **Requested activity/production data** - email sent to Nina & Carmen (Jan 2025)
- 📅 **Call scheduled** to discuss data sharing

**The gap:** TPI provides intensity (tCO2/t steel) but NOT the underlying production data. To calculate absolute emissions for carbon budgets, we need:
```
Absolute emissions = Intensity × Production
```
Without production data, we can't convert TPI intensity to absolute Mt CO2.

**How to download (what's publicly available):**
1. Go to TPI steel sector page
2. Click the download icon (top right)
3. Download `CP_Assessments_[date].xlsx`
4. Also download `MQ_Assessments_[date].xlsx` for Management Quality

**What you get:**
- 40 steel companies
- Historical emissions intensity (tCO2/t)
- Projected intensity to 2050
- Alignment with 1.5°C / Below 2°C / National Pledges
- Management Quality scores (Level 0-5)

**Steel companies covered by TPI (as of 2024):**
- ArcelorMittal, Tata Steel, POSCO, Nippon Steel, JFE Holdings
- JSW Steel, ThyssenKrupp, SSAB, Voestalpine, Salzgitter
- BlueScope, Nucor, US Steel, Cleveland-Cliffs, Steel Dynamics
- Gerdau, CSN, Ternium
- Hyundai Steel, China Steel Corp, Kobe Steel
- Plus ~20 more

---

## Key Files

### Data Files

| File | Location | Purpose |
|------|----------|---------|
| `steel_ald_combined.csv` | `open-asset-data/data/processed/steel/` | Combined ALD → R pipeline input |
| `steel_apa_emissions.csv` | `open-asset-data/outputs/steel/` | APA results (229 rows, 26 companies) |
| `steel_production_from_reports.csv` | `open-asset-data/data/processed/steel/` | Curated production data |
| GEM GIST Excel files | `open-asset-data/data/raw/GEM/` | Plant database (Dec 2025) |

### Scripts

| Script | Location | Purpose |
|--------|----------|---------|
| `pipeline/integrate.py` | `open-asset-data/` | Python: multi-source integration |
| `pipeline/apa_calculator.py` | `open-asset-data/` | Python: APA emissions calculation |
| `05_company_sda_pcp.R` | `02.Scripts/03_analysis/` | R: PCP calculation (SDA + ACA) |
| `steel_comprehensive.R` | `02.Scripts/04_output/` | R: final output with comparison columns |

### Documentation

| File | Purpose |
|------|---------|
| `apa_methodology.md` | Core APA methodology |
| `steel_data_dictionary.md` | All variable definitions |
| `steel_python_pipeline.txt` | Technical Python pipeline reference |

---

## Company Coverage

### Tier 1: Full Data (Production + Reported + Calculated)

| Company | Production | Reported Emissions | Climate Trace | APA Calculated |
|---------|------------|-------------------|---------------|----------------|
| Tata Steel | ✅ 2020-2023 | ✅ 2020-2023 | 🔄 | 🔄 |
| ArcelorMittal | ✅ 2020-2023 | ✅ 2020-2023 | 🔄 | 🔄 |
| POSCO | ✅ 2020-2023 | ✅ 2020-2023 | 🔄 | 🔄 |
| Nippon Steel | ✅ 2020-2023 | ✅ 2020-2023 | 🔄 | 🔄 |
| JSW Steel | ✅ 2020-2023 | ⚠️ Partial | 🔄 | 🔄 |
| ThyssenKrupp | ✅ 2020-2023 | ✅ 2020-2023 | 🔄 | 🔄 |
| SSAB | ✅ 2020-2023 | ✅ 2020-2023 | 🔄 | 🔄 |

### Tier 2: Production + Some Emissions

| Company | Production | Reported Emissions | Notes |
|---------|------------|-------------------|-------|
| SAIL | ✅ | ⚠️ Limited | Indian govt company |
| Cleveland-Cliffs | ✅ | ✅ | Post-2021 acquisition |
| JFE Holdings | ✅ | ✅ | |
| Hyundai Steel | ✅ | ✅ | |
| Nucor | ✅ | ✅ | EAF-based |
| Gerdau | ✅ | ✅ | EAF-based |
| BlueScope | ✅ | ✅ | |

### Tier 3: Production Only (Need Emissions)

| Company | Production | Status |
|---------|------------|--------|
| NLMK | ✅ | ❌ Sanctions - limited disclosure |
| MMK | ✅ | ❌ Sanctions - limited disclosure |
| Severstal | ✅ | ❌ Sanctions - limited disclosure |
| Jindal Steel & Power | ✅ | 🔍 Need to find |
| China Steel Corp | ✅ | 🔍 Need to find |
| Kobe Steel | ✅ | 🔍 Need to find |

---

## Validation Checks

### Calculated vs Reported Emissions

When APA calculated emissions differ significantly from reported:

| Discrepancy | Likely Cause | Action |
|-------------|--------------|--------|
| Calculated >> Reported | Boundary: we include more | Check what's in company boundary |
| Calculated << Reported | Boundary: they include mining | Note in emissions_boundary |
| Large variance year-to-year | Acquisition/divestiture | Check for M&A activity |
| Indian companies high | DRI correctly using 3.10? | Verify coal-based DRI |

### Expected Intensity Ranges

| Company Type | Expected tCO2/t | If Outside Range |
|--------------|-----------------|------------------|
| BF-BOF dominant (India) | 2.5 - 3.5 | Check EF assignments |
| BF-BOF dominant (EU/Japan) | 1.7 - 2.3 | Check EF assignments |
| EAF dominant | 0.3 - 1.0 | Verify process classification |
| Mixed | 1.0 - 2.0 | Check plant breakdown |

---

## Next Steps

### Immediate (This Week)
- [ ] Run APA calculation for all companies with production data
- [ ] Fill in reported emissions for Tier 1 companies
- [ ] Integrate Climate Trace data (from other chat)

### Short-term (This Month)
- [ ] Complete Tier 2 company emissions
- [ ] Build validation dashboard (calculated vs reported vs CT)
- [ ] Document all boundary differences

### Medium-term
- [ ] Automate WSA PDF extraction
- [ ] Set up CDP data pipeline
- [ ] Extend to 2024 data when available

---

## Related Chats

| Topic | Key Findings | Status |
|-------|--------------|--------|
| **Historical steel emissions** | Manual data collection approach | Active |
| **David's APA methodology** | DRI coal/gas critical distinction | ✅ Documented |
| **Climate Trace integration** | Facility-level satellite data | Active |
| **Power sector NGER** | Australian power company projections | ✅ Complete |
| **TPI data integration** | Requested activity data; have intensity back to 2013 | 🔄 Call scheduled |
| **Net Zero Tracker** | Company target metadata | 🔄 Have data |

---

## Transition Plan Assessment (David Kampmann's APA Method)

**What we're building:**

The CBT doesn't just track historical emissions - it assesses whether company plans are Paris-aligned using **David Kampmann's APA (Asset-based Physical Assessment)** methodology from his 2024 paper.

### Three Projections to Compare

| Projection | What It Models | Data Needed |
|------------|----------------|-------------|
| **BAU** | No action - continue current operations, reinvest in same tech | Current assets, lifetimes |
| **Stated Transition Plan** | What companies SAY they'll do - announced tech changes | Asset-level TP details |
| **Paris-Aligned** | What science REQUIRES | SDA pathway, carbon budget |

```
Emissions
    │
    │    BAU (no change)
    │    ╭─────────────────────────────────────────────
    │   ╱
    │  ╱   Stated TP (what they claim)
    │ ╱    ╭───────────────────────────────
    │╱    ╱
    ●────╱─────────────────────────────────────────
    │   ╱        Paris-Aligned (what's needed)
    │  ╱
    │ ╱
    │╱
    └────────────────────────────────────────────────► Time
   2024                2030                      2050

   Kampmann finding: Only 3/20 companies' stated TPs were Paris-aligned!
```

### Stated Transition Plan Projection (Kampmann APA Method)

**NOT just targets** - models specific announced technology changes at asset level:

| Change Type | Example | Data Source | How to Model |
|-------------|---------|-------------|--------------|
| Phase-outs | "Close BF2 by 2027" | Annual reports, press releases | Remove capacity & emissions from date |
| Tech switches | "Convert to H2-DRI-EAF" | Climate reports, CDP | Change EF from 2.1→0.05 at switch date |
| Capacity changes | "Expand Dolvi by 5 Mt" | Investor presentations | Add capacity with appropriate EF |
| CCS additions | "Add 2 Mt CCS by 2028" | Project announcements | Reduce net emissions by capture rate |

**Asset-level data to scrape:**

```
Company: Tata Steel UK
Plant: Port Talbot

CURRENT STATE (2024):
├─ Technology: BF-BOF
├─ Capacity: 5 Mt/year
├─ EF: 2.05 tCO2/t
└─ Annual emissions: 10.25 Mt CO2

ANNOUNCED CHANGE:
├─ Action: Close BFs → EAF conversion  
├─ Date: 2027
└─ Source: Tata Steel UK Green Steel Plan (2023)

POST-CHANGE (2027+):
├─ Technology: EAF (scrap-based)
├─ Capacity: 3 Mt/year  
├─ EF: 0.04 tCO2/t
└─ Annual emissions: 0.12 Mt CO2

IMPACT: 10.13 Mt CO2/year reduction (99%)
```

**This is what makes David's APA approach different from just tracking "net zero by 2050" targets!**

The method models the actual plant-level changes, not just the headline target.

### Data to Scrape for TP Assessment

When scraping company reports, collect:

**For BAU baseline:**
- Current plant capacities (have from GEM)
- Asset ages / retirement dates
- Standard asset lifetimes (BF ~40yr, EAF ~30yr)

**For Stated Transition Plan:**
- Announced plant closures (with dates)
- Technology conversion plans (BF→EAF, BF→H2-DRI)
- New green capacity announcements
- CCS project commitments
- Capex allocated to decarbonization

**Sources:**
- Company climate/sustainability reports
- CDP responses (Section C4 - Targets, C-CE4a - Steel)
- Investor presentations
- Press releases
- Green Steel Tracker (industrytransition.org)
- GEM project pipeline

---

## Questions to Resolve

1. **Boundary standardization:** Should we adjust reported emissions to match our boundary, or keep as-reported?

2. **FY vs CY:** Indian companies report FY (April-March). How to align with CY production from WSA?

3. **Joint ventures:** AM/NS India is 60% ArcelorMittal, 40% Nippon Steel. Who gets the emissions?

4. **Climate Trace gaps:** Some facilities not in CT. How to handle missing coverage?

5. **TPI activity data:** Awaiting response - if they can't share, need to source production separately from WSA/company reports and combine with TPI intensity.

---

## Pending Contacts & Data Requests

| Contact | Organisation | Request | Status | Next Step |
|---------|--------------|---------|--------|-----------|
| Nina & Carmen | TPI (LSE) | Activity/production data behind intensity figures | Email sent Jan 2025 | Call scheduled |

---

*Last updated: February 2026*
