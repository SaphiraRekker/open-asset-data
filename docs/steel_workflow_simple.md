# Steel Data - Simple Workflow

## The One File That Matters

**`steel_master_data.csv`** - Everything goes here. One row per company-year.

---

## Annual Update Checklist (June each year)

When WSA releases new "World Steel in Figures":

### Week 1: Production Data
- [ ] Download WSA PDF from worldsteel.org/data/world-steel-in-figures/
- [ ] Add new year's production to `steel_master_data.csv`
- [ ] Source = "WSA 2025" (or whatever year)

### Week 2: Reported Emissions  
- [ ] Check CDP responses (if available)
- [ ] Check company sustainability reports (Google: "[company] sustainability report 2024")
- [ ] Add to reported_scope1_mt, reported_scope12_mt columns
- [ ] Note the boundary in emissions_boundary column

### Week 3: TPI Data
- [ ] Go to transitionpathwayinitiative.org/sectors/steel
- [ ] Download CP_Assessments Excel file
- [ ] Download MQ_Assessments Excel file
- [ ] Add TPI intensity, alignment, and MQ level to master data

### Week 4: Transition Plans (NEW)
- [ ] Check Net Zero Tracker for updated targets
- [ ] Review company climate reports for technology commitments
- [ ] Note any announced plant closures or green projects
- [ ] Update target_* and technology_plans columns

### Week 5: Calculate & Validate
- [ ] Re-run Python pipeline: `cd open-asset-data && python -m pipeline.integrate`
- [ ] Re-run R pipeline: `05_company_sda_pcp.R` then `steel_comprehensive.R`
- [ ] Run `data_quality_check.R` to validate output
- [ ] Review any records with >30% discrepancy

### Week 6: Climate Trace
- [ ] Pull latest Climate Trace data (see CT chat)
- [ ] Match facilities to companies
- [ ] Add to climatetrace_mt column

---

## Adding a New Company

1. **Find production:** Search WSA top producers or company annual report
2. **Add row to CSV:** 
   ```
   New Company,NEWCO,2023,Country,15.0,WSA 2024,,...
   ```
3. **Add to company_patterns in R script** (if not matching automatically):
   ```r
   "New Company", "GEM Parent Name Pattern",
   ```
4. **Run calculation:** `calculate_company_emissions(plants, "New Company", 15.0)`
5. **Find reported emissions:** Check sustainability report
6. **Validate:** Compare calculated vs reported

---

## Quick Reference

### Where to find production data:
| Source | URL | When |
|--------|-----|------|
| WSA Top 50 | worldsteel.org/data/top-steel-producers/ | June annually |
| WSA PDF | worldsteel.org/data/world-steel-in-figures/ | June annually |

### Where to find reported emissions:
| Source | Best for |
|--------|----------|
| CDP | Standardized disclosure (if company responds) |
| Sustainability Report | Most companies publish annually |
| Annual/Integrated Report | Often has emissions in ESG section |

### Where to find TPI data:
| Data | How to get | Status |
|------|------------|--------|
| Intensity + Alignment | transitionpathwayinitiative.org/sectors/steel → Download | ✅ Have it |
| Management Quality | Same page → MQ_Assessments file | ✅ Have it |
| **Activity/Production** | Requested from Nina/Carmen | 🔄 Call scheduled |

**Note:** TPI intensity alone isn't enough for carbon budgets. We need activity data to convert to absolute emissions. If TPI can't share, use WSA production + TPI intensity.

**TPI covers ~40 steel companies including:**
ArcelorMittal, Tata, POSCO, Nippon Steel, JSW, ThyssenKrupp, SSAB, 
Voestalpine, BlueScope, Nucor, US Steel, Cleveland-Cliffs, Gerdau, etc.

### Where to find transition plan data:
| Data | Source | URL |
|------|--------|-----|
| Net zero targets | Net Zero Tracker | netzerotracker.net |
| SBTi validated targets | SBTi | sciencebasedtargets.org/companies-taking-action |
| Technology roadmaps | Company climate reports | (varies) |
| Plant closures | GEM Steel Tracker | globalenergymonitor.org |
| Green steel projects | Press releases, LeadIT | industrytransition.org |

### Expected emission intensities:
| Type | tCO2/t steel |
|------|--------------|
| India BF-BOF | 2.5 - 3.5 |
| EU/Japan BF-BOF | 1.7 - 2.3 |
| EAF-based | 0.3 - 1.0 |
| Mixed | 1.0 - 2.0 |

---

## File Locations

```
open-asset-data/                          ← Python pipeline repo (git submodule)
  pipeline/integrate.py                   ← Run: python -m pipeline.integrate
  data/processed/steel/
    steel_ald_combined.csv                ← Combined ALD (input for R)
    steel_production_from_reports.csv     ← Curated production data
  outputs/steel/
    steel_apa_emissions.csv               ← APA results (26 companies)

CarbonBudgetTracker/
  02.Scripts/03_analysis/
    05_company_sda_pcp.R                  ← Calculate PCP (SDA + ACA)
  02.Scripts/04_output/
    steel_comprehensive.R                 ← Generate final output CSVs
    data_quality_check.R                  ← Cross-sector validation
  03.Outputs/02.Analysis/steel/
    steel_2014_base.csv                   ← Final output (2014 base year)
    steel_2020_base.csv                   ← Final output (2020 base year)
  Docs/
    steel_data_dictionary.md              ← What columns mean
```

---

## When Things Go Wrong

| Problem | Solution |
|---------|----------|
| Company not matching plants | Add pattern to `company_patterns` in R script |
| Calculated >> Reported | Check emissions_boundary - company may exclude some ops |
| Calculated << Reported | Company includes mining - note in emissions_notes |
| EF looks wrong | Check if Indian DRI using 3.10 (coal) not 1.05 (gas) |
| Missing from GEM | Add plant manually or use company-reported capacity |

---

## Related Chats Reference

| Topic | What it covers |
|-------|----------------|
| David's APA methodology | How calculation works, DRI coal/gas distinction |
| Historical steel emissions | Manual data collection for reported emissions |
| Climate Trace integration | Satellite data extraction |

---

*Keep it simple: One CSV, update annually, validate with R script.*
