"""
Multi-source data integration for steel company emissions and production.

Combines three data sources into a unified dataset:
  1. Annual Reports  - PDF-extracted (company self-reported, audited)
  2. Climate Trace   - Satellite + facility model estimates (independent)
  3. APA             - Asset-based Planning Approach (David Kampmann's method)

Note: APA replicates and extends David Kampmann's methodology from his
"Asset-based Planning Approach" paper. It uses GEM plant data, production
figures, and country×technology emission factors to estimate emissions.

Each observation carries:
  - source: which dataset it came from
  - certainty: 0-1 score reflecting data quality and provenance
  - is_default: whether this is the recommended value for platform users

Usage:
    cd open-asset-data
    python -m pipeline.integrate
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import numpy as np

from .config import (
    PROCESSED_DATA_DIR,
    OUTPUTS_DIR,
    CLIMATE_TRACE_FILE,
    EF_BF_BOF,
    EF_TECHNOLOGY,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ============================================================================
# Company name harmonization
# ============================================================================
# Maps variant names (lowercase) to canonical company names.
# Climate Trace uses subsidiary names; ALD and reports use parent names.

COMPANY_CANONICAL = {
    # ArcelorMittal
    "arcelormittal": "ArcelorMittal",
    "arcelor mittal": "ArcelorMittal",
    "arcelormittal atlantique et lorraine sas": "ArcelorMittal",
    "arcelormittal brasil sa": "ArcelorMittal",
    "arcelormittal bremen gmbh": "ArcelorMittal",
    "arcelormittal dofasco gp": "ArcelorMittal",
    "arcelormittal duisburg gmbh": "ArcelorMittal",
    "arcelormittal eisenhüttenstadt gmbh": "ArcelorMittal",
    "arcelormittal españa sa": "ArcelorMittal",
    "arcelormittal hamburg gmbh": "ArcelorMittal",
    "arcelormittal hunedoara sa": "ArcelorMittal",
    "arcelormittal kryvyi rih pjsc": "ArcelorMittal",
    "arcelormittal long products canada gp": "ArcelorMittal",
    "arcelormittal méditerranée sas": "ArcelorMittal",
    "arcelormittal méxico sa de cv": "ArcelorMittal",
    "arcelormittal nippon steel india ltd": "ArcelorMittal",
    "arcelormittal poland sa": "ArcelorMittal",
    "arcelormittal sa": "ArcelorMittal",
    "arcelormittal south africa ltd": "ArcelorMittal",
    "arcelormittal tubarão comercial sa": "ArcelorMittal",
    "arcelormittal zenica doo": "ArcelorMittal",
    "arcelormittal olaberria-bergara sl": "ArcelorMittal",
    "arcelormittal sestao sl": "ArcelorMittal",
    "arcelormittal warszawa sp zoo": "ArcelorMittal",
    # Tata Steel
    "tata steel": "Tata Steel",
    "tata steel (thailand) pcl": "Tata Steel",
    "tata steel ijmuiden bv": "Tata Steel",
    "tata steel long products ltd": "Tata Steel",
    "tata steel ltd": "Tata Steel",
    "tata steel uk ltd": "Tata Steel",
    # JSW Steel
    "jsw steel": "JSW Steel",
    "jsw steel ltd": "JSW Steel",
    "jsw ispat special products ltd": "JSW Steel",
    "jsw steel usa ohio inc": "JSW Steel",
    # Nippon Steel
    "nippon steel": "Nippon Steel",
    "nippon steel corp": "Nippon Steel",
    # POSCO
    "posco": "POSCO Holdings",
    "posco holdings": "POSCO Holdings",
    "posco holdings inc": "POSCO Holdings",
    "pt krakatau posco": "POSCO Holdings",
    "posco (zhangjiagang) stainless steel co ltd": "POSCO Holdings",
    "posco vietnam co ltd": "POSCO Holdings",
    # SSAB
    "ssab": "SSAB",
    "ssab ab": "SSAB",
    "ssab americas holding ab": "SSAB",
    # ThyssenKrupp
    "thyssenkrupp": "ThyssenKrupp",
    "thyssenkrupp steel europe ag": "ThyssenKrupp",
    # Nucor
    "nucor": "Nucor",
    "nucor corp": "Nucor",
    "nucor steel decatur llc": "Nucor",
    "nucor steel kankakee inc": "Nucor",
    "nucor steel memphis inc": "Nucor",
    "nucor steel seattle inc": "Nucor",
    "nucor steel tuscaloosa inc": "Nucor",
    "nucor yamato steel co": "Nucor",
    # JFE
    "jfe holdings": "JFE Holdings",
    "jfe steel corp": "JFE Holdings",
    "jfe bars & shapes corp": "JFE Holdings",
    # Gerdau
    "gerdau": "Gerdau",
    "gerdau acominas sa": "Gerdau",
    "gerdau acos longos sa": "Gerdau",
    "gerdau ameristeel corp": "Gerdau",
    "gerdau corsa sapi de cv": "Gerdau",
    "gerdau sa": "Gerdau",
    # Baoshan / Baowu (in ALD)
    "baoshan iron & steel": "Baoshan Iron & Steel",
    # BlueScope
    "bluescope steel": "BlueScope Steel",
    # China Steel
    "china steel": "China Steel",
    # Severstal
    "severstal": "Severstal",
    "pao severstal": "Severstal",
    # US Steel
    "us steel": "US Steel",
    "united states steel": "US Steel",
    "united states steel corp": "US Steel",
    # Hyundai Steel
    "hyundai steel": "Hyundai Steel",
    "hyundai steel co": "Hyundai Steel",
    "hyundai steel co ltd": "Hyundai Steel",
    # Cleveland-Cliffs
    "cleveland-cliffs": "Cleveland-Cliffs",
    "cleveland cliffs": "Cleveland-Cliffs",
    "cleveland-cliffs inc": "Cleveland-Cliffs",
    "ak steel": "Cleveland-Cliffs",
    # Kobe Steel
    "kobe steel": "Kobe Steel",
    "kobe steel ltd": "Kobe Steel",
    "kobelco": "Kobe Steel",
    # voestalpine
    "voestalpine": "voestalpine",
    "voestalpine ag": "voestalpine",
    "voestalpine stahl gmbh": "voestalpine",
    # SAIL
    "sail": "SAIL",
    "steel authority of india": "SAIL",
    "steel authority of india ltd": "SAIL",
    # Steel Dynamics
    "steel dynamics": "Steel Dynamics",
    "steel dynamics inc": "Steel Dynamics",
    "sdi": "Steel Dynamics",
    # Salzgitter
    "salzgitter": "Salzgitter",
    "salzgitter ag": "Salzgitter",
    "salzgitter flachstahl gmbh": "Salzgitter",
    # Ternium
    "ternium": "Ternium",
    "ternium sa": "Ternium",
    "techint": "Ternium",
    # NLMK
    "nlmk": "NLMK",
    "novolipetsk": "NLMK",
    "novolipetsk steel": "NLMK",
    "nlmk pao": "NLMK",
    # Evraz
    "evraz": "Evraz",
    "evraz plc": "Evraz",
    "evraz group": "Evraz",
    # Liberty Steel
    "liberty steel": "Liberty Steel",
    "gfg alliance": "Liberty Steel",
    "liberty steel group": "Liberty Steel",
}


def harmonize_company(name: str) -> str:
    """Map a company name variant to its canonical form."""
    if pd.isna(name):
        return name
    key = name.strip().lower()
    if key in COMPANY_CANONICAL:
        return COMPANY_CANONICAL[key]
    # Fuzzy fallback: check if any canonical key is a prefix
    for pattern, canonical in COMPANY_CANONICAL.items():
        if key.startswith(pattern) or pattern.startswith(key):
            return canonical
    return name  # return as-is if no match


# ============================================================================
# Source loaders
# ============================================================================

def load_annual_reports() -> pd.DataFrame:
    """Load PDF-extracted annual report data."""
    path = PROCESSED_DATA_DIR / "steel_all_extracted.csv"
    if not path.exists():
        logger.warning(f"Annual report data not found: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    rows = []
    for _, r in df.iterrows():
        company = harmonize_company(r["company"])
        year = int(r["year"])
        confidence = r.get("confidence", "medium")

        if r["metric"] == "production_mt":
            rows.append({
                "company": company,
                "year": year,
                "metric": "production_mt",
                "value": r["value"],
                "unit": "Mt",
                "source": "annual_report",
                "source_detail": r.get("source_pdf", ""),
                "extraction_method": r.get("extraction_method", ""),
                "confidence_raw": confidence,
                "source_page": r.get("source_page", ""),
                "notes": r.get("notes", ""),
            })
        elif r["metric"].startswith("emissions"):
            rows.append({
                "company": company,
                "year": year,
                "metric": "emissions_mt_co2",
                "value": r["value"],
                "unit": "Mt CO2",
                "source": "annual_report",
                "source_detail": r.get("source_pdf", ""),
                "extraction_method": r.get("extraction_method", ""),
                "confidence_raw": confidence,
                "source_page": r.get("source_page", ""),
                "notes": r.get("notes", ""),
            })

    result = pd.DataFrame(rows)
    logger.info(f"Annual reports: {len(result)} records, "
                f"{result['company'].nunique()} companies")
    return result


def load_climate_trace() -> pd.DataFrame:
    """Load Climate Trace facility-aggregated data."""
    if not CLIMATE_TRACE_FILE.exists():
        logger.warning(f"Climate Trace data not found: {CLIMATE_TRACE_FILE}")
        return pd.DataFrame()

    df = pd.read_csv(CLIMATE_TRACE_FILE)

    # Map company names to canonical forms
    df["company_canonical"] = df["company"].apply(harmonize_company)

    # Aggregate subsidiaries to parent company level
    agg = (
        df.groupby(["company_canonical", "year"])
        .agg(
            activity_mt=("activity", "sum"),
            emissions_mt=("emissions", "sum"),
            n_facilities=("n_facilities", "sum"),
            n_subsidiaries=("company", "nunique"),
            facility_types=("facility_types", lambda x: ", ".join(sorted(set(
                ft.strip()
                for fts in x.dropna()
                for ft in fts.split(",")
            )))),
        )
        .reset_index()
    )

    # Compute group-level intensity
    agg["intensity"] = agg["emissions_mt"] / agg["activity_mt"].replace(0, np.nan)

    rows = []
    for _, r in agg.iterrows():
        company = r["company_canonical"]
        year = int(r["year"])

        # Production
        if r["activity_mt"] > 0:
            rows.append({
                "company": company,
                "year": year,
                "metric": "production_mt",
                "value": round(r["activity_mt"], 3),
                "unit": "Mt",
                "source": "climate_trace",
                "source_detail": f"{int(r['n_facilities'])} facilities, "
                                 f"{int(r['n_subsidiaries'])} entities",
                "extraction_method": "satellite_model",
                "confidence_raw": "modeled",
                "source_page": "",
                "notes": f"Facility types: {r['facility_types']}",
            })

        # Emissions
        if r["emissions_mt"] > 0:
            rows.append({
                "company": company,
                "year": year,
                "metric": "emissions_mt_co2",
                "value": round(r["emissions_mt"], 3),
                "unit": "Mt CO2e",
                "source": "climate_trace",
                "source_detail": f"{int(r['n_facilities'])} facilities, "
                                 f"intensity={r['intensity']:.2f}",
                "extraction_method": "satellite_model",
                "confidence_raw": "modeled",
                "source_page": "",
                "notes": f"Facility types: {r['facility_types']}",
            })

    result = pd.DataFrame(rows)
    logger.info(f"Climate Trace: {len(result)} records, "
                f"{result['company'].nunique()} companies")
    return result


# ============================================================================
# Data Quality Scoring
# ============================================================================
#
# We use TWO separate quality dimensions:
#
# 1. RELIABILITY - Is the number accurate for what it claims to measure?
#    - annual_report: HIGH (audited, verified by third party)
#    - climate_trace: MEDIUM (modeled from satellite + facility data)
#    - apa: MEDIUM (physics-based calculation from plant data)
#
# 2. COMPARABILITY - Can this number be compared across companies?
#    - annual_report: LOW (different scopes, boundaries, methodologies)
#    - climate_trace: HIGH (consistent methodology, but limited years 2021-2025)
#    - apa: HIGH (consistent methodology, full year coverage)
#
# For platform use cases (comparing companies against pathways), we prefer
# COMPARABILITY over RELIABILITY, so APA is the default source.
# ============================================================================

def compute_reliability(row: pd.Series) -> float:
    """Compute reliability score (0-1): Is the number accurate?

    Higher = more confidence in the accuracy of this specific measurement.

    Factors:
      - Source verification (audited > modeled > calculated)
      - Extraction confidence (high > medium > low)
      - Recency (newer data more reliable)
    """
    score = 0.0

    # Source verification level
    source = row.get("source", "")
    if source == "annual_report":
        score += 0.50  # Third-party audited
    elif source == "climate_trace":
        score += 0.35  # Satellite + model verified
    elif source == "apa":
        score += 0.35  # Physics-based, uses verified plant data

    # Extraction quality
    conf = str(row.get("confidence_raw", "")).lower()
    if conf == "high":
        score += 0.30
    elif conf == "medium":
        score += 0.20
    elif conf in ("modeled", "satellite_model", "asset_level_model"):
        score += 0.15
    else:
        score += 0.05

    # Recency
    year = row.get("year", 2020)
    current_year = 2025
    age = current_year - year
    if age <= 2:
        score += 0.10
    elif age <= 5:
        score += 0.05

    return round(min(score, 1.0), 3)


def compute_comparability(row: pd.Series) -> float:
    """Compute comparability score (0-1): Can this be compared across companies?

    Higher = more suitable for cross-company benchmarking.

    Factors:
      - Methodology consistency (same scope/boundary for all companies)
      - Coverage (available for all companies and years)
    """
    source = row.get("source", "")

    if source == "apa":
        # APA: Highest comparability
        # - Same methodology for all companies
        # - Same scope boundaries (Scope 1 steel production)
        # - Full year coverage (2015-2050)
        return 0.95

    elif source == "climate_trace":
        # Climate Trace: High comparability but limited coverage
        # - Consistent satellite/model methodology
        # - Same scope for all facilities
        # - BUT: Limited to 2021-2025
        return 0.80

    elif source == "annual_report":
        # Annual reports: Low comparability
        # - Different scope definitions (Scope 1 only vs 1+2, etc.)
        # - Different consolidation methods (equity vs operational)
        # - Different boundary definitions
        # - Some include downstream, others don't
        return 0.40

    return 0.30


def compute_certainty(row: pd.Series) -> float:
    """Legacy function: Compute combined certainty score.

    DEPRECATED: Use compute_reliability() and compute_comparability() instead.
    Kept for backward compatibility.

    This combines reliability and comparability into a single score,
    which conflates two different quality dimensions.
    """
    # Weight comparability higher since platform use case is cross-company
    reliability = compute_reliability(row)
    comparability = compute_comparability(row)
    return round(0.4 * reliability + 0.6 * comparability, 3)


def add_cross_validation_bonus(df: pd.DataFrame) -> pd.DataFrame:
    """Add a cross-validation bonus when multiple sources agree within 15%."""
    df = df.copy()
    df["certainty_cross_val"] = 0.0

    for (company, year, metric), group in df.groupby(
        ["company", "year", "metric"]
    ):
        if len(group) < 2:
            continue

        values = group["value"].values
        median_val = np.median(values)
        if median_val == 0:
            continue

        for idx in group.index:
            val = df.loc[idx, "value"]
            pct_diff = abs(val - median_val) / median_val
            if pct_diff <= 0.15:
                df.loc[idx, "certainty_cross_val"] = 0.10
            elif pct_diff <= 0.30:
                df.loc[idx, "certainty_cross_val"] = 0.05

    df["certainty"] = (df["certainty_base"] + df["certainty_cross_val"]).clip(
        upper=1.0
    )
    return df


# ============================================================================
# Default source selection
# ============================================================================

def select_defaults(df: pd.DataFrame) -> pd.DataFrame:
    """Mark the recommended default value for each (company, year, metric).

    Priority order (optimized for COMPARABILITY - cross-company benchmarking):
    1. APA (Asset-based Planning Approach) — consistent methodology, full coverage
    2. Climate Trace (satellite-based) — consistent methodology, limited years
    3. Annual report (company self-reported) — reliable but not comparable

    This prioritizes comparability over reliability because the platform's
    main use case is comparing companies against each other and pathways.

    Users who want company-specific tracking can toggle to annual_report view.

    Within same source, prefer higher reliability score.
    """
    df = df.copy()
    df["is_default"] = False

    # Priority order: APA > Climate Trace > Annual Report (for comparability)
    source_priority = {
        "apa": 0, "climate_trace": 1, "annual_report": 2,
    }
    df["_source_rank"] = df["source"].map(source_priority).fillna(99)

    for (company, year, metric), group in df.groupby(
        ["company", "year", "metric"]
    ):
        # Sort: comparability priority first, then reliability descending
        sorted_group = group.sort_values(
            ["_source_rank", "reliability"], ascending=[True, False]
        )
        best_idx = sorted_group.index[0]
        df.loc[best_idx, "is_default"] = True

    df.drop(columns=["_source_rank"], inplace=True)
    return df


# ============================================================================
# Data quality flags
# ============================================================================

# Known bad extractions to exclude
EXCLUDE_RULES = [
    # --- JFE Holdings: generic extractor misidentifies production ---
    # 2015-2016: "surpassed 100 million tons" is cumulative milestone, not annual
    {"company": "JFE Holdings", "year": 2015, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "JFE Holdings", "year": 2016, "metric": "production_mt",
     "source": "annual_report"},
    # 2017: 3.04 Mt from 10,000-ton unit table (should be ~30 Mt)
    {"company": "JFE Holdings", "year": 2017, "metric": "production_mt",
     "source": "annual_report"},
    # 2018-2020: "30 million tons" is capacity, not production (actual ~24-27 Mt)
    {"company": "JFE Holdings", "year": 2018, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "JFE Holdings", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "JFE Holdings", "year": 2020, "metric": "production_mt",
     "source": "annual_report"},
    # 2021: 22.76 Mt extracted from non-production context
    {"company": "JFE Holdings", "year": 2021, "metric": "production_mt",
     "source": "annual_report"},

    # --- US Steel: all "production capability" not actual production ---
    # Actual production ~15-16 Mt; "capability" is nameplate capacity
    {"company": "US Steel", "year": 2014, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2015, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2016, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2017, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2018, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2020, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2021, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2022, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "US Steel", "year": 2023, "metric": "production_mt",
     "source": "annual_report"},

    # --- Salzgitter: all "capacity" not actual production ---
    # Actual production ~5-6 Mt; extracted "crude steel capacity of 7 million tons"
    {"company": "Salzgitter", "year": 2014, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2015, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2016, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2017, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2018, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2020, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2021, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2022, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2023, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Salzgitter", "year": 2024, "metric": "production_mt",
     "source": "annual_report"},

    # --- Ternium: 12.4 Mt repeated is capacity, not production ---
    # 2016: 3.5 Mt could be actual for one division; keeping it
    {"company": "Ternium", "year": 2017, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Ternium", "year": 2018, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Ternium", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Ternium", "year": 2020, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Ternium", "year": 2021, "metric": "production_mt",
     "source": "annual_report"},

    # --- Steel Dynamics: suspicious values ---
    # 2021: 27.0 Mt is total system capacity across all sites (actual ~11 Mt)
    {"company": "Steel Dynamics", "year": 2021, "metric": "production_mt",
     "source": "annual_report"},
    # 2018-2019: 2.0 Mt is single-division capacity, not total company
    {"company": "Steel Dynamics", "year": 2018, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "Steel Dynamics", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},

    # --- SAIL: 106.6 Mt in 2019 is clearly wrong (actual ~16 Mt) ---
    {"company": "SAIL", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},

    # --- NLMK ---
    # 2018: 17.5 Mt is total steel capacity, not NLMK production (actual ~17 Mt crude)
    # Actually ~17.5 might be correct for NLMK 2018 - keep it
    # 2021: "167 million tons" is US industry figure, not NLMK
    {"company": "NLMK", "year": 2021, "metric": "production_mt",
     "source": "annual_report"},

    # --- Hyundai Steel ---
    # 2017: 60.0 Mt is total Hyundai Motor Group capacity, not Hyundai Steel alone
    {"company": "Hyundai Steel", "year": 2017, "metric": "production_mt",
     "source": "annual_report"},
    # 2022: 2.6 Mt is EAF scrap-based capacity only (total actual ~21 Mt)
    {"company": "Hyundai Steel", "year": 2022, "metric": "production_mt",
     "source": "annual_report"},
    # 2024 emissions: 2.023 Mt CO2 is partial scope (actual ~30 Mt)
    {"company": "Hyundai Steel", "year": 2024, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- Gerdau ---
    # 2019 emissions: 85 Mt CO2 is clearly wrong (actual ~12 Mt CO2)
    {"company": "Gerdau", "year": 2019, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- Kobe Steel ---
    # 2019 emissions: 2.274 Mt is DRI plant capacity figure, not emissions
    {"company": "Kobe Steel", "year": 2019, "metric": "emissions_mt_co2",
     "source": "annual_report"},
    # 2024 emissions: 4.464 Mt is DRI-related number, not total emissions
    {"company": "Kobe Steel", "year": 2024, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- Salzgitter emissions ---
    # 2022: 8.0 Mt CO2 extracted from context about "95% of Scope 1-3" — low confidence
    {"company": "Salzgitter", "year": 2022, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- SSAB production ---
    # 2021: 8.8 Mt is "production capacity" not actual production (actual ~7 Mt)
    {"company": "SSAB", "year": 2021, "metric": "production_mt",
     "source": "annual_report"},

    # --- Nippon Steel ---
    # 2024: capacity not production
    {"company": "Nippon Steel", "year": 2024, "metric": "production_mt",
     "source": "annual_report"},

    # --- BlueScope Steel ---
    # 2015: 1.33 Mt is single-segment despatch, not group total (~3-5 Mt)
    {"company": "BlueScope Steel", "year": 2015, "metric": "production_mt",
     "source": "annual_report"},
    # 2017: 7.4 Mt CO2 is historical Australian reduction figure, not Scope 1+2
    {"company": "BlueScope Steel", "year": 2017, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- Tata Steel ---
    # 2016: 9.97 Mt is India-only production, not consolidated (~26 Mt with Europe)
    {"company": "Tata Steel", "year": 2016, "metric": "production_mt",
     "source": "annual_report"},
    # 2019: 1735 Mt is obviously wrong (probably revenue or kt misparse)
    {"company": "Tata Steel", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},
    # 2023: 55.2 Mt CO2 is too high — likely includes Scope 3 or wrong context
    {"company": "Tata Steel", "year": 2023, "metric": "emissions_mt_co2",
     "source": "annual_report"},
    # 2024: 84.0 Mt CO2 is far too high — likely Scope 1+2+3 or misparse
    {"company": "Tata Steel", "year": 2024, "metric": "emissions_mt_co2",
     "source": "annual_report"},
    # 2025: 62.62 Mt CO2 is too high — likely includes broader scope
    {"company": "Tata Steel", "year": 2025, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- China Steel ---
    # All extracted production values ~1 Mt are a single product line, not total (~10 Mt)
    {"company": "China Steel", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "China Steel", "year": 2020, "metric": "production_mt",
     "source": "annual_report"},
    {"company": "China Steel", "year": 2021, "metric": "production_mt",
     "source": "annual_report"},
    # 2020: 2.02 Mt CO2 is partial scope (actual ~20+ Mt CO2)
    {"company": "China Steel", "year": 2020, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- Steel Dynamics: partial division data ---
    # 2015: 1.2 Mt is single plant, not total company (~8-9 Mt in 2015)
    {"company": "Steel Dynamics", "year": 2015, "metric": "production_mt",
     "source": "annual_report"},

    # --- Gerdau ---
    # 2019 production: 5.6 Mt is Brazil-only, not consolidated (~12 Mt)
    {"company": "Gerdau", "year": 2019, "metric": "production_mt",
     "source": "annual_report"},
    # 2021/2023 emissions: 2.02 Mt CO2 is partial scope (actual ~10-15 Mt CO2)
    {"company": "Gerdau", "year": 2021, "metric": "emissions_mt_co2",
     "source": "annual_report"},
    {"company": "Gerdau", "year": 2023, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- NLMK ---
    # 2020 emissions: 2.0 Mt CO2 is partial scope (actual ~25 Mt CO2)
    {"company": "NLMK", "year": 2020, "metric": "emissions_mt_co2",
     "source": "annual_report"},
    # 2021 emissions: 33.58 Mt CO2 is too high (actual ~25 Mt CO2)
    {"company": "NLMK", "year": 2021, "metric": "emissions_mt_co2",
     "source": "annual_report"},

    # --- Hyundai Steel ---
    # 2017 emissions: 18.8 Mt CO2 is plausible — keep it
    # 2023 emissions: 30.69 Mt CO2 is plausible — keep it

    # --- SAIL emissions ---
    # 2022-2024 emissions values 45-52 Mt CO2 are plausible for SAIL (~15-19 Mt production at ~2.8 EF)
    # Keep them
]


def apply_quality_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove known bad extractions and flag suspicious values."""
    df = df.copy()
    df["quality_flag"] = ""

    # Remove known bad rows
    for rule in EXCLUDE_RULES:
        mask = pd.Series(True, index=df.index)
        for col, val in rule.items():
            mask &= df[col] == val
        if mask.any():
            df.loc[mask, "quality_flag"] = "excluded: known extraction error"
            logger.info(f"Excluding {mask.sum()} rows: {rule}")

    # Flag suspicious values
    # Production > 100 Mt for any single company is suspicious
    mask = (df["metric"] == "production_mt") & (df["value"] > 100)
    df.loc[mask, "quality_flag"] = df.loc[mask, "quality_flag"].where(
        df.loc[mask, "quality_flag"] != "", "suspicious: production > 100 Mt"
    )

    # Emissions > 200 Mt for single company is suspicious
    mask = (df["metric"] == "emissions_mt_co2") & (df["value"] > 200)
    df.loc[mask, "quality_flag"] = df.loc[mask, "quality_flag"].where(
        df.loc[mask, "quality_flag"] != "", "suspicious: emissions > 200 Mt"
    )

    return df


# ============================================================================
# Cross-source comparison
# ============================================================================

def build_comparison_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Build a wide-format comparison table: one row per (company, year, metric)
    with columns for each source's value and the default."""
    active = df[df["quality_flag"] == ""].copy()

    # Pivot to wide format
    wide = active.pivot_table(
        index=["company", "year", "metric"],
        columns="source",
        values="value",
        aggfunc="first",
    ).reset_index()

    # Flatten column names
    wide.columns = [
        f"value_{c}" if c not in ("company", "year", "metric") else c
        for c in wide.columns
    ]

    # Add default value
    defaults = active[active["is_default"]][
        ["company", "year", "metric", "value", "source", "certainty"]
    ].rename(columns={
        "value": "default_value",
        "source": "default_source",
        "certainty": "default_certainty",
    })
    wide = wide.merge(defaults, on=["company", "year", "metric"], how="left")

    # Add cross-source agreement metrics
    source_cols = [c for c in wide.columns if c.startswith("value_")]
    for _, row in wide.iterrows():
        vals = [row[c] for c in source_cols if pd.notna(row[c])]
        if len(vals) >= 2:
            median = np.median(vals)
            spread = (max(vals) - min(vals)) / median if median > 0 else np.nan
            wide.loc[_, "n_sources"] = len(vals)
            wide.loc[_, "source_spread_pct"] = round(spread * 100, 1)
        else:
            wide.loc[_, "n_sources"] = len(vals)
            wide.loc[_, "source_spread_pct"] = np.nan

    wide = wide.sort_values(["company", "year", "metric"]).reset_index(drop=True)
    return wide


# ============================================================================
# Main pipeline
# ============================================================================

def run_integration():
    """Run full multi-source integration pipeline."""
    logger.info("=" * 60)
    logger.info("MULTI-SOURCE DATA INTEGRATION")
    logger.info("=" * 60)

    # Step 1: Load all sources
    logger.info("\n--- Loading data sources ---")
    ar_df = load_annual_reports()
    ct_df = load_climate_trace()

    # Load APA (Asset-based Planning Approach) - replicates Kampmann methodology
    from .apa_calculator import load_apa_source
    apa_df = load_apa_source()

    # Step 2: Combine
    logger.info("\n--- Combining sources ---")
    combined = pd.concat([ar_df, ct_df, apa_df], ignore_index=True)
    logger.info(f"Combined: {len(combined)} total records")

    # Step 3: Quality filters
    logger.info("\n--- Applying quality filters ---")
    combined = apply_quality_filters(combined)
    n_excluded = (combined["quality_flag"] != "").sum()
    logger.info(f"Excluded/flagged: {n_excluded} records")

    # Step 4: Quality scoring (reliability + comparability)
    logger.info("\n--- Computing quality scores ---")
    combined["reliability"] = combined.apply(compute_reliability, axis=1)
    combined["comparability"] = combined.apply(compute_comparability, axis=1)
    combined["certainty_base"] = combined.apply(compute_certainty, axis=1)
    combined = add_cross_validation_bonus(combined)

    # Step 5: Select defaults
    logger.info("\n--- Selecting default sources ---")
    active = combined[combined["quality_flag"] == ""].copy()
    active = select_defaults(active)
    # Merge is_default back
    combined["is_default"] = False
    combined.loc[active.index, "is_default"] = active["is_default"]

    # Step 6: Save outputs
    logger.info("\n--- Saving outputs ---")
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    # Long format (all records, all sources)
    out_long = PROCESSED_DATA_DIR / "steel_multi_source.csv"
    output_cols = [
        "company", "year", "metric", "value", "unit", "source",
        "source_detail", "extraction_method",
        "reliability", "comparability", "certainty",  # Quality scores
        "is_default", "quality_flag", "notes",
    ]
    combined[output_cols].to_csv(out_long, index=False)
    logger.info(f"Long format saved: {out_long} ({len(combined)} rows)")

    # Wide format comparison
    wide = build_comparison_wide(combined)
    out_wide = PROCESSED_DATA_DIR / "steel_multi_source_comparison.csv"
    wide.to_csv(out_wide, index=False)
    logger.info(f"Wide comparison saved: {out_wide} ({len(wide)} rows)")

    # Default-only view (what the platform shows by default)
    defaults_only = combined[
        (combined["is_default"]) & (combined["quality_flag"] == "")
    ].copy()
    out_defaults = PROCESSED_DATA_DIR / "steel_defaults.csv"
    defaults_only[output_cols].to_csv(out_defaults, index=False)
    logger.info(f"Defaults saved: {out_defaults} ({len(defaults_only)} rows)")

    # Step 7: Print summary
    _print_summary(combined, wide)

    return combined, wide


def _print_summary(combined: pd.DataFrame, wide: pd.DataFrame):
    """Print integration summary."""
    active = combined[combined["quality_flag"] == ""]

    logger.info("\n" + "=" * 60)
    logger.info("INTEGRATION SUMMARY")
    logger.info("=" * 60)

    # Source coverage
    logger.info("\n--- Source coverage ---")
    for source in ["annual_report", "climate_trace", "apa"]:
        subset = active[active["source"] == source]
        companies = subset["company"].nunique()
        years = f"{subset['year'].min()}-{subset['year'].max()}" if len(subset) > 0 else "N/A"
        logger.info(f"  {source}: {len(subset)} records, {companies} companies, years {years}")

    # Company coverage
    logger.info("\n--- Company coverage (active records) ---")
    for company in sorted(active["company"].unique()):
        comp_data = active[active["company"] == company]
        sources = comp_data["source"].unique()
        years = sorted(comp_data["year"].unique())
        year_range = f"{min(years)}-{max(years)}"
        prod_count = len(comp_data[comp_data["metric"] == "production_mt"])
        emis_count = len(comp_data[comp_data["metric"] == "emissions_mt_co2"])
        logger.info(
            f"  {company}: sources={list(sources)}, "
            f"years={year_range}, production={prod_count}, emissions={emis_count}"
        )

    # Cross-source agreement
    logger.info("\n--- Cross-source agreement ---")
    multi = wide[wide["n_sources"] >= 2]
    if len(multi) > 0:
        logger.info(f"  Observations with 2+ sources: {len(multi)}")
        logger.info(
            f"  Median source spread: {multi['source_spread_pct'].median():.1f}%"
        )
        high_spread = multi[multi["source_spread_pct"] > 30]
        if len(high_spread) > 0:
            logger.info(f"  High disagreement (>30%): {len(high_spread)} cases")
            for _, row in high_spread.iterrows():
                logger.info(
                    f"    {row['company']} {int(row['year'])} {row['metric']}: "
                    f"spread={row['source_spread_pct']:.0f}%"
                )

    # Default source distribution
    logger.info("\n--- Default source selection ---")
    defaults = active[active["is_default"]]
    source_counts = defaults["source"].value_counts()
    for source, count in source_counts.items():
        logger.info(f"  {source}: {count} defaults selected")

    # Certainty distribution
    logger.info("\n--- Certainty score distribution ---")
    for source in ["annual_report", "climate_trace", "apa"]:
        subset = active[active["source"] == source]
        if len(subset) > 0:
            logger.info(
                f"  {source}: mean={subset['certainty'].mean():.3f}, "
                f"min={subset['certainty'].min():.3f}, max={subset['certainty'].max():.3f}"
            )

    logger.info("=" * 60)


if __name__ == "__main__":
    run_integration()
