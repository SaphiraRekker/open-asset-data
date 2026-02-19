# Data Quality Methodology for Steel Emissions

## Overview

This document describes the data quality scoring framework used in the multi-source
steel emissions pipeline. The methodology is based on established standards:

- **ISO 14064-1**: GHG verification data quality indicators
- **GHG Protocol**: Uncertainty assessment guidance
- **IPCC 2006 Guidelines**: Pedigree matrix approach for emissions inventory data
- **Weidema & Wesnaes (1996)**: Original pedigree matrix for LCA data quality

## Pedigree Matrix Approach

We adapt the standard 5-level pedigree matrix (where 1 = highest quality, 5 = lowest)
to a 0-1 certainty score (where 1 = highest quality). Our scoring considers five
dimensions from ISO 14044 / IPCC guidelines:

1. **Reliability** - Source credibility and verification status
2. **Completeness** - Coverage of all relevant emissions sources
3. **Temporal Representativeness** - How current the data is
4. **Geographical Representativeness** - Match to actual locations
5. **Technological Representativeness** - Match to actual processes

## Source Type Scoring

### Annual Reports (Base Score: 0.50)

| Indicator | Score | Justification |
|-----------|-------|---------------|
| Reliability | Very Good | Company self-reported, typically third-party verified |
| Completeness | Very Good | Covers full organizational boundary |
| Temporal | Very Good | Published annually for that reporting year |
| Geographical | Very Good | Company-specific locations |
| Technological | Very Good | Reflects actual technology mix |

**Rationale**: Annual reports undergo internal review and often third-party
assurance (e.g., PwC, KPMG verification). They represent the most authoritative
source for a company's own emissions.

### Climate Trace (Base Score: 0.35)

| Indicator | Score | Justification |
|-----------|-------|---------------|
| Reliability | Good | Satellite + facility model, independently verified |
| Completeness | Very Good | Global satellite coverage |
| Temporal | Good | May lag 1-2 years behind actual |
| Geographical | Very Good | Facility-level resolution |
| Technological | Fair | Modeled based on facility type, not actual |

**Rationale**: Climate Trace provides independent verification through satellite
observations and facility-level modeling. However, it relies on estimation models
that may not capture actual operational parameters.

### APA - Asset-based Planning Approach (Base Score: 0.30)

| Indicator | Score | Justification |
|-----------|-------|---------------|
| Reliability | Good | Physics-based calculation with documented method |
| Completeness | Fair | Limited to matched plant data |
| Temporal | Fair | Uses static plant snapshot across all years |
| Geographical | Very Good | Plant-level data with country assignment |
| Technological | Good | Uses plant-specific process type (BF-BOF, EAF, DRI) |

**Rationale**: The APA approach (David Kampmann's methodology) calculates emissions
from plant capacity, utilization rate, and emission factors. It provides a
physics-based estimate but relies on:
- Static plant data (assumes plant configurations unchanged)
- Production estimates from external sources
- Average emission factors by country×technology

## Extraction Quality Modifiers

| Quality Level | Score Bonus | Definition |
|--------------|-------------|------------|
| High | +0.30 | Explicit table extraction with unit validation |
| Medium | +0.20 | Context-based extraction, verified plausible |
| Modeled | +0.15 | Calculated from models (Climate Trace, APA) |
| Low | +0.05 | Inferred or uncertain extraction |

## Recency Bonus

Data quality degrades over time as:
- Technologies change
- Companies restructure
- Production patterns shift

| Age | Bonus | Justification |
|-----|-------|---------------|
| ≤2 years | +0.10 | Current operational data |
| 3-5 years | +0.05 | Recently representative |
| >5 years | +0.00 | May not reflect current state |

## Cross-Validation Bonus

When multiple independent sources agree, confidence increases:

| Agreement | Bonus | Definition |
|-----------|-------|------------|
| Within 15% | +0.10 | Strong agreement across sources |
| Within 30% | +0.05 | Moderate agreement |
| >30% divergence | +0.00 | Significant disagreement |

## Total Certainty Score

```
Certainty = Source_Base + Quality_Modifier + Recency_Bonus + CrossVal_Bonus
```

Maximum possible: 1.0
Minimum possible: 0.05 (low quality, old, single source)

## Score Interpretation

| Range | Interpretation | Typical Use |
|-------|----------------|-------------|
| 0.80-1.00 | Very High | Primary reference, suitable for reporting |
| 0.60-0.80 | High | Good for analysis, cross-check recommended |
| 0.40-0.60 | Medium | Useful for estimates, flag uncertainty |
| 0.20-0.40 | Low | Use with caution, seek better sources |
| <0.20 | Very Low | Indicative only, not for formal reporting |

## References

1. **ISO 14064-1:2018** - Greenhouse gases — Part 1: Specification with guidance at
   the organization level for quantification and reporting of greenhouse gas
   emissions and removals

2. **GHG Protocol** - Technical Guidance for Calculating Scope 3 Emissions (2013)
   Chapter 7: Uncertainty Assessment

3. **IPCC 2006 Guidelines for National Greenhouse Gas Inventories** - Volume 1:
   General Guidance and Reporting, Chapter 3: Uncertainties

4. **Weidema, B.P. & Wesnaes, M.S. (1996)** - Data quality management for life
   cycle inventories—an example of using data quality indicators.
   Journal of Cleaner Production, 4(3-4), 167-174.

5. **Koolen, D. & Vidovic, D. (2022)** - Greenhouse gas intensities of the EU
   steel industry and its trading partners. JRC129297. European Commission.

---

*Document version: 1.0*
*Last updated: January 2026*
