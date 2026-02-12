"""
Steel emissions forward projections module.

This module provides forward projection capabilities for steel company emissions,
integrating multiple data sources:

1. TPI (Transition Pathway Initiative) - Intensity pathways to 2050
2. Kampmann ALD - BAU and Transition Pathway scenarios
3. Company commitments - Oxford Net Zero Tracker, SBTi targets
4. BAU trend projection - CAGR-based extrapolation

Usage:
    cd open-asset-data
    python -m pipeline.steel_projections
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .config import (
    PROCESSED_DATA_DIR,
    PROCESSED_STEEL_DIR,
    OUTPUTS_DIR,
    OUTPUTS_COMPANY_STEEL,
    KAMPMANN_ALD_FILE,
    TPI_ASSESSMENTS_FILE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ============================================================================
# Paths
# ============================================================================

# TPI data (from config)
TPI_FILE = TPI_ASSESSMENTS_FILE

# Oxford Net Zero Tracker - scraped steel targets
OXFORD_NZT_FILE = OUTPUTS_COMPANY_STEEL / "scraped_data" / "oxford_nz_steel_targets.csv"

# ============================================================================
# Company name mapping (TPI → canonical)
# ============================================================================

TPI_COMPANY_MAP = {
    "ArcelorMittal": "ArcelorMittal",
    "BlueScope Steel": "BlueScope Steel",
    "China Steel": "China Steel",
    "Gerdau": "Gerdau",
    "Hyundai Steel": "Hyundai Steel",
    "JFE Holdings": "JFE Holdings",
    "JSW Steel": "JSW Steel",
    "Kobe Steel": "Kobe Steel",
    "Nippon Steel": "Nippon Steel",
    "Nucor": "Nucor",
    "POSCO": "POSCO Holdings",
    "POSCO Holdings": "POSCO Holdings",
    "Severstal": "Severstal",
    "SSAB": "SSAB",
    "Tata Steel": "Tata Steel",
    "ThyssenKrupp": "ThyssenKrupp",
    "thyssenkrupp": "ThyssenKrupp",
    "United States Steel": "US Steel",
    "US Steel": "US Steel",
    "voestalpine": "voestalpine",
}

# Oxford NZT company name mapping (NZT → canonical)
NZT_COMPANY_MAP = {
    "Nippon Steel Corporation": "Nippon Steel",
    "ArcelorMittal": "ArcelorMittal",
    "ThyssenKrupp": "ThyssenKrupp",
    "POSCO": "POSCO Holdings",
    "JFE Holdings": "JFE Holdings",
    "Nucor": "Nucor",
    "Tata Steel": "Tata Steel",
    "Severstal": "Severstal",
    "JSW Steel": "JSW Steel",
    "China Steel": "China Steel",
    "Hyundai Steel": "Hyundai Steel",
    "Kobe Steel": "Kobe Steel",
    "Steel Dynamics": "Steel Dynamics",
    "voestalpine AG": "voestalpine",
    "Beijing Shougang": "Beijing Shougang",
    "US Steel": "US Steel",
}


def harmonize_company(name: str) -> str:
    """Map company name to canonical form."""
    if pd.isna(name):
        return name
    clean = name.strip()
    return TPI_COMPANY_MAP.get(clean, clean)


# ============================================================================
# TPI Data Loading
# ============================================================================

def load_tpi_steel() -> pd.DataFrame:
    """Load TPI intensity data for steel companies (2013-2050).

    TPI provides INTENSITY (tCO2/t steel), not absolute emissions.
    To get emissions: Emissions = Intensity × Production

    Returns:
        DataFrame with columns: company, year, intensity_tco2_per_t,
                               is_historical, alignment_2050
    """
    if not TPI_FILE.exists():
        logger.warning(f"TPI file not found: {TPI_FILE}")
        return pd.DataFrame()

    df = pd.read_csv(TPI_FILE)

    # Filter to Steel sector
    steel = df[df["Sector"] == "Steel"].copy()
    if len(steel) == 0:
        logger.warning("No steel companies found in TPI data")
        return pd.DataFrame()

    logger.info(f"TPI: Found {len(steel)} steel companies")

    # Year columns are 2013-2050
    year_cols = [str(y) for y in range(2013, 2051)]
    available_years = [c for c in year_cols if c in steel.columns]

    # Reshape to long format
    rows = []
    for _, r in steel.iterrows():
        company = harmonize_company(r["Company Name"])
        cutoff_year = int(r["History to Projection Cutoff Year"]) if pd.notna(r["History to Projection Cutoff Year"]) else 2023
        alignment = r.get("Carbon Performance Alignment 2050", "")

        for year_str in available_years:
            val = r[year_str]
            if pd.notna(val):
                year = int(year_str)
                rows.append({
                    "company": company,
                    "year": year,
                    "intensity_tco2_per_t": float(val),
                    "is_historical": year <= cutoff_year,
                    "alignment_2050": alignment,
                    "source": "tpi",
                })

    result = pd.DataFrame(rows)
    logger.info(f"TPI steel: {len(result)} records, {result['company'].nunique()} companies, "
                f"years {result['year'].min()}-{result['year'].max()}")
    return result


def calculate_tpi_emissions(tpi_df: pd.DataFrame,
                            production_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate absolute emissions from TPI intensity × production.

    Args:
        tpi_df: TPI intensity data (from load_tpi_steel)
        production_df: Production data with columns: company, year, production_mt

    Returns:
        DataFrame with emissions projections
    """
    if tpi_df.empty or production_df.empty:
        return pd.DataFrame()

    # Merge on company and year
    merged = tpi_df.merge(
        production_df[["company", "year", "production_mt"]],
        on=["company", "year"],
        how="left"
    )

    # Calculate emissions
    merged["emissions_mt_co2"] = merged["intensity_tco2_per_t"] * merged["production_mt"]

    # For future years without production data, estimate production
    # using latest available production (constant production assumption)
    for company in merged["company"].unique():
        mask = merged["company"] == company
        company_data = merged[mask].copy()

        # Find latest year with production data
        with_prod = company_data[company_data["production_mt"].notna()]
        if len(with_prod) > 0:
            latest_year = with_prod["year"].max()
            latest_prod = with_prod[with_prod["year"] == latest_year]["production_mt"].values[0]

            # Fill forward for future years
            future_mask = mask & (merged["year"] > latest_year) & merged["production_mt"].isna()
            merged.loc[future_mask, "production_mt"] = latest_prod
            merged.loc[future_mask, "emissions_mt_co2"] = (
                merged.loc[future_mask, "intensity_tco2_per_t"] * latest_prod
            )

    result = merged[merged["emissions_mt_co2"].notna()].copy()
    result["scenario"] = "tpi_pathway"

    logger.info(f"TPI emissions: {len(result)} projections calculated")
    return result


# ============================================================================
# Kampmann ALD Projections
# ============================================================================

def load_kampmann_projections() -> pd.DataFrame:
    """Load Kampmann ALD BAU and Transition Pathway scenarios.

    Returns:
        DataFrame with columns: company, year, scenario, production_mt, emissions_mt_co2
    """
    if not KAMPMANN_ALD_FILE.exists():
        logger.warning(f"Kampmann ALD not found: {KAMPMANN_ALD_FILE}")
        return pd.DataFrame()

    df = pd.read_csv(KAMPMANN_ALD_FILE)

    rows = []
    for _, r in df.iterrows():
        company = harmonize_company(r["Company Name"])
        year = int(r["Year"])
        variable = r["Variable"]
        value = r["Value"]

        # Determine scenario and metric
        if "BAU" in variable:
            scenario = "bau"
        elif "TP" in variable:
            scenario = "transition_pathway"
        else:
            continue

        if "Production" in variable:
            metric = "production_mt"
            # Values are in kt, convert to Mt
            value = value / 1000
        elif "Emission" in variable:
            metric = "emissions_mt_co2"
            # Already in Mt CO2
        else:
            continue

        rows.append({
            "company": company,
            "year": year,
            "scenario": scenario,
            "metric": metric,
            "value": round(value, 3),
        })

    result = pd.DataFrame(rows)

    # Pivot to wide format (production and emissions as columns)
    if len(result) > 0:
        pivot = result.pivot_table(
            index=["company", "year", "scenario"],
            columns="metric",
            values="value",
            aggfunc="sum"
        ).reset_index()
        pivot.columns.name = None

        logger.info(f"Kampmann projections: {len(pivot)} records, "
                    f"{pivot['company'].nunique()} companies, "
                    f"years {pivot['year'].min()}-{pivot['year'].max()}")
        return pivot

    return pd.DataFrame()


# ============================================================================
# BAU Trend Projection
# ============================================================================

def project_bau(historical_df: pd.DataFrame,
                base_year: int = 2023,
                end_year: int = 2050,
                method: str = "recent_trend") -> pd.DataFrame:
    """Project emissions forward using BAU trend extrapolation.

    Similar to fossil fuel script 14_reformat_fossilfuel_with_projections.R

    Args:
        historical_df: Historical emissions with columns: company, year, emissions_mt_co2
        base_year: Year to start projections from
        end_year: Year to project to
        method: "recent_trend" (2021-2023) or "long_trend" (2015-2023)

    Returns:
        DataFrame with BAU projections
    """
    if historical_df.empty:
        return pd.DataFrame()

    results = []

    for company in historical_df["company"].unique():
        company_data = historical_df[historical_df["company"] == company].copy()
        company_data = company_data.sort_values("year")

        # Determine trend period
        if method == "recent_trend":
            trend_data = company_data[company_data["year"].between(2021, 2023)]
        else:  # long_trend
            trend_data = company_data[company_data["year"].between(2015, 2023)]

        if len(trend_data) < 2:
            logger.debug(f"Insufficient data for {company} BAU projection")
            continue

        # Calculate CAGR
        first_year = trend_data["year"].min()
        last_year = trend_data["year"].max()
        first_val = trend_data[trend_data["year"] == first_year]["emissions_mt_co2"].values[0]
        last_val = trend_data[trend_data["year"] == last_year]["emissions_mt_co2"].values[0]

        if first_val <= 0 or last_val <= 0:
            continue

        n_years = last_year - first_year
        if n_years == 0:
            continue

        cagr = (last_val / first_val) ** (1 / n_years) - 1

        # Cap extreme growth rates (matching fossil fuel script)
        cagr = max(min(cagr, 0.05), -0.10)  # -10% to +5%

        # Get base value
        base_data = company_data[company_data["year"] == base_year]
        if len(base_data) == 0:
            base_data = company_data[company_data["year"] == company_data["year"].max()]
        base_val = base_data["emissions_mt_co2"].values[0]
        actual_base_year = int(base_data["year"].values[0])

        # Project forward
        for year in range(actual_base_year + 1, end_year + 1):
            years_forward = year - actual_base_year
            projected_val = base_val * ((1 + cagr) ** years_forward)

            results.append({
                "company": company,
                "year": year,
                "emissions_mt_co2": round(projected_val, 3),
                "scenario": "bau",
                "projection_method": method,
                "cagr_applied": round(cagr, 4),
            })

    result = pd.DataFrame(results)
    if len(result) > 0:
        logger.info(f"BAU projections: {len(result)} records, "
                    f"{result['company'].nunique()} companies")
    return result


# ============================================================================
# Company Commitments (Oxford Net Zero Tracker)
# ============================================================================

def load_oxford_nzt() -> pd.DataFrame:
    """Load Oxford Net Zero Tracker company commitments.

    Returns:
        DataFrame with columns:
          - company: Canonical company name
          - end_target_type: Type of 2050 target (Net zero, Carbon neutral, etc.)
          - end_target_year: Target year (2045, 2050, 2055)
          - end_target_status: Status (In corporate strategy, Declaration/pledge)
          - interim_target_year: Interim target year (usually 2030)
          - interim_target_pct: Interim reduction percentage
          - interim_baseline_year: Baseline year for interim target
          - scope1_coverage, scope2_coverage, scope3_coverage: Yes/No/Partial
    """
    if not OXFORD_NZT_FILE.exists():
        logger.warning(f"Oxford NZT file not found: {OXFORD_NZT_FILE}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(OXFORD_NZT_FILE)
        logger.info(f"Oxford NZT: Loaded {len(df)} companies")

        # Map company names to canonical form
        df["company"] = df["company"].apply(
            lambda x: NZT_COMPANY_MAP.get(x, x) if pd.notna(x) else x
        )

        # Log available targets
        has_interim = df["interim_target_pct"].notna().sum()
        has_nz = df["end_target_year"].notna().sum()
        logger.info(f"  Companies with interim targets: {has_interim}")
        logger.info(f"  Companies with NZ/neutrality targets: {has_nz}")

        return df

    except Exception as e:
        logger.error(f"Error loading Oxford NZT: {e}")
        return pd.DataFrame()


def project_nzt_commitment(
    company: str,
    base_emissions: float,
    base_year: int,
    nzt_row: pd.Series,
    end_year: int = 2055
) -> pd.DataFrame:
    """Generate emissions pathway based on company's NZT commitment.

    Uses linear interpolation between:
      - Base year emissions
      - Interim target (e.g., 2030: -25% from baseline)
      - End target (e.g., 2050: Net zero = 0 or near-zero)

    Args:
        company: Company name
        base_emissions: Latest available emissions (Mt CO2)
        base_year: Year of base emissions
        nzt_row: Row from NZT data with target information
        end_year: Final year to project to

    Returns:
        DataFrame with commitment-based projections
    """
    rows = []

    # Extract target details
    interim_year = nzt_row.get("interim_target_year")
    interim_pct = nzt_row.get("interim_target_pct")
    interim_baseline = nzt_row.get("interim_baseline_year")
    end_target_year = nzt_row.get("end_target_year")
    end_target_type = nzt_row.get("end_target_type", "")

    # Skip if no usable targets
    if pd.isna(end_target_year) and pd.isna(interim_year):
        return pd.DataFrame()

    # Build trajectory points
    trajectory = [(base_year, base_emissions)]

    # Add interim target point if available
    if pd.notna(interim_year) and pd.notna(interim_pct):
        interim_year = int(interim_year)
        # Calculate target emissions
        # interim_pct is reduction % from baseline (e.g., 25 = 25% reduction)
        # We use base_emissions as proxy if we don't have baseline year data
        interim_emissions = base_emissions * (1 - interim_pct / 100)
        if interim_year > base_year:
            trajectory.append((interim_year, max(0, interim_emissions)))

    # Add end target point
    if pd.notna(end_target_year):
        end_target_year = int(end_target_year)
        # Determine end target emissions
        if "net zero" in str(end_target_type).lower():
            # Net zero = residual emissions only (assume 5% residual)
            end_emissions = base_emissions * 0.05
        elif "carbon neutral" in str(end_target_type).lower() or "climate neutral" in str(end_target_type).lower():
            # Carbon neutrality may include offsets, assume 10% residual
            end_emissions = base_emissions * 0.10
        elif "reduction" in str(end_target_type).lower():
            # Emissions reduction target (use the % if available)
            end_pct = nzt_row.get("end_target_pct", 40)
            end_emissions = base_emissions * (1 - end_pct / 100) if pd.notna(end_pct) else base_emissions * 0.6
        else:
            # Unknown target type, assume 80% reduction
            end_emissions = base_emissions * 0.20

        if end_target_year > base_year:
            trajectory.append((end_target_year, max(0, end_emissions)))

    # Sort trajectory by year
    trajectory = sorted(trajectory, key=lambda x: x[0])

    # Generate annual projections through linear interpolation
    for year in range(base_year, end_year + 1):
        # Find bracketing points
        before = [(y, e) for y, e in trajectory if y <= year]
        after = [(y, e) for y, e in trajectory if y > year]

        if not before:
            # Before first point, use first value
            emissions = trajectory[0][1]
        elif not after:
            # After last point, use last value (maintain end target)
            emissions = trajectory[-1][1]
        else:
            # Interpolate between bracketing points
            y1, e1 = before[-1]
            y2, e2 = after[0]
            if y2 == y1:
                emissions = e1
            else:
                # Linear interpolation
                emissions = e1 + (e2 - e1) * (year - y1) / (y2 - y1)

        rows.append({
            "company": company,
            "year": year,
            "emissions_mt_co2": round(max(0, emissions), 3),
            "scenario": "nzt_commitment",
            "source": "oxford_nzt",
        })

    return pd.DataFrame(rows)


def generate_nzt_projections(
    historical_df: pd.DataFrame,
    nzt_df: pd.DataFrame
) -> pd.DataFrame:
    """Generate commitment-based projections for all companies with NZT data.

    Args:
        historical_df: Historical emissions with columns: company, year, emissions_mt_co2
        nzt_df: Oxford NZT data from load_oxford_nzt()

    Returns:
        DataFrame with commitment pathways for all matching companies
    """
    if historical_df.empty or nzt_df.empty:
        return pd.DataFrame()

    all_projections = []

    for _, nzt_row in nzt_df.iterrows():
        company = nzt_row["company"]

        # Find latest historical emissions for this company
        company_hist = historical_df[historical_df["company"] == company]
        if company_hist.empty:
            continue

        # Get most recent year's emissions
        latest = company_hist.sort_values("year").iloc[-1]
        base_year = int(latest["year"])
        base_emissions = float(latest["emissions_mt_co2"])

        # Generate projection
        proj = project_nzt_commitment(
            company=company,
            base_emissions=base_emissions,
            base_year=base_year,
            nzt_row=nzt_row,
        )

        if not proj.empty:
            all_projections.append(proj)
            logger.debug(f"Generated NZT projection for {company}: "
                        f"{base_year}-{proj['year'].max()}")

    if all_projections:
        result = pd.concat(all_projections, ignore_index=True)
        logger.info(f"NZT projections: {len(result)} records, "
                    f"{result['company'].nunique()} companies")
        return result

    return pd.DataFrame()


# ============================================================================
# Main Integration
# ============================================================================

def run_steel_projections() -> pd.DataFrame:
    """Generate all projection scenarios for steel companies.

    Returns:
        DataFrame with comprehensive projection data
    """
    logger.info("=" * 60)
    logger.info("STEEL FORWARD PROJECTIONS")
    logger.info("=" * 60)

    all_projections = []

    # 1. Load TPI data
    logger.info("\n--- Loading TPI intensity data ---")
    tpi_df = load_tpi_steel()
    if not tpi_df.empty:
        # For now, just include intensity pathway
        # Full emissions calculation needs production projections
        tpi_proj = tpi_df.copy()
        tpi_proj["scenario"] = "tpi_intensity_pathway"
        all_projections.append(tpi_proj)

    # 2. Load Kampmann projections (BAU + TP)
    logger.info("\n--- Loading Kampmann ALD projections ---")
    kampmann_df = load_kampmann_projections()
    if not kampmann_df.empty:
        for scenario in kampmann_df["scenario"].unique():
            subset = kampmann_df[kampmann_df["scenario"] == scenario].copy()
            all_projections.append(subset)

    # 3. Load historical data for BAU trend projection
    logger.info("\n--- Computing BAU trend projections ---")
    defaults_file = PROCESSED_STEEL_DIR / "steel_defaults.csv"
    if defaults_file.exists():
        defaults = pd.read_csv(defaults_file)
        emissions = defaults[defaults["metric"] == "emissions_mt_co2"][
            ["company", "year", "value"]
        ].rename(columns={"value": "emissions_mt_co2"})

        bau_proj = project_bau(emissions)
        if not bau_proj.empty:
            all_projections.append(bau_proj)
    else:
        logger.warning(f"Defaults file not found: {defaults_file}")

    # 4. Load company commitments and generate projections
    logger.info("\n--- Loading company commitments (Oxford NZT) ---")
    nzt_df = load_oxford_nzt()
    if not nzt_df.empty:
        # Build historical emissions from BOTH defaults AND APA for full 26-company coverage
        hist_parts = []
        if defaults_file.exists():
            defaults = pd.read_csv(defaults_file)
            hist_parts.append(
                defaults[defaults["metric"] == "emissions_mt_co2"][
                    ["company", "year", "value"]
                ].rename(columns={"value": "emissions_mt_co2"})
            )
        # Also include APA emissions for companies not in defaults
        apa_file = OUTPUTS_COMPANY_STEEL / "steel_apa_emissions.csv"
        if apa_file.exists():
            apa = pd.read_csv(apa_file)
            existing = set(hist_parts[0]["company"].unique()) if hist_parts else set()
            apa_extra = apa[~apa["company"].isin(existing)][
                ["company", "year", "emissions_mt"]
            ].rename(columns={"emissions_mt": "emissions_mt_co2"})
            if len(apa_extra) > 0:
                hist_parts.append(apa_extra)
                logger.info(f"  Added APA historical for {apa_extra['company'].nunique()} extra NZT companies")

        if hist_parts:
            hist_emissions = pd.concat(hist_parts, ignore_index=True)
            nzt_proj = generate_nzt_projections(hist_emissions, nzt_df)
            if not nzt_proj.empty:
                all_projections.append(nzt_proj)

    # Combine all projections
    if all_projections:
        combined = pd.concat(all_projections, ignore_index=True)

        # Save output
        PROCESSED_STEEL_DIR.mkdir(parents=True, exist_ok=True)
        out_path = PROCESSED_STEEL_DIR / "steel_projections.csv"
        combined.to_csv(out_path, index=False)
        logger.info(f"\nSaved: {out_path} ({len(combined)} rows)")

        # Summary
        logger.info("\n--- Projection Summary ---")
        for scenario in combined["scenario"].unique():
            subset = combined[combined["scenario"] == scenario]
            logger.info(f"  {scenario}: {len(subset)} records, "
                        f"{subset['company'].nunique()} companies")

        return combined

    return pd.DataFrame()


# ============================================================================
# Export in ALD format (for R script compatibility)
# ============================================================================

def export_ald_format() -> pd.DataFrame:
    """Export historical + projections in Kampmann ALD format.

    Combines:
      - Historical data from steel_defaults.csv (best estimates)
      - BAU projections from steel_projections.csv
      - TP projections from Kampmann ALD

    Output format extends SteelALD.csv with source provenance:
      Company Name, Country, Variable, Unit, Year, Value, Source, Certainty

    Where Variable encodes metric + scenario:
      - Production (Historical), Production (BAU), Production (TP)
      - Emissions (Historical), Emissions (BAU), Emissions (TP)

    Source indicates data provenance:
      - annual_report: Company self-reported (audited)
      - climate_trace: Satellite/model-based (independent)
      - apa: Asset-based Planning Approach (calculated)
      - kampmann_ald: Original Kampmann projections

    Certainty is a 0-1 quality score (see integrate.py for methodology).
    """
    logger.info("=" * 60)
    logger.info("EXPORTING ALD FORMAT")
    logger.info("=" * 60)

    rows = []

    # 1. Load historical defaults
    defaults_file = PROCESSED_STEEL_DIR / "steel_defaults.csv"
    if not defaults_file.exists():
        logger.error(f"Run integrate.py first: {defaults_file}")
        return pd.DataFrame()

    defaults = pd.read_csv(defaults_file)
    defaults = defaults[defaults["is_default"] == True]

    logger.info(f"Loaded {len(defaults)} default records")

    # Get the companies we have good data for:
    # - Must have data from annual_report OR apa (not just climate_trace)
    # - For SDA, need both emissions AND production

    # Find companies with high-quality data (annual_report or apa)
    high_quality = defaults[defaults["source"].isin(["annual_report", "apa"])]
    hq_companies = set(high_quality["company"].unique())
    logger.info(f"Companies with annual_report or apa data: {len(hq_companies)}")

    # Of those, find which have both production and emissions
    production_data = defaults[
        (defaults["metric"] == "production_mt") &
        (defaults["company"].isin(hq_companies))
    ]
    emissions_data = defaults[
        (defaults["metric"] == "emissions_mt_co2") &
        (defaults["company"].isin(hq_companies))
    ]

    companies_with_production = set(production_data["company"].unique())
    companies_with_emissions = set(emissions_data["company"].unique())

    # Must have both for SDA
    good_companies = list(companies_with_production & companies_with_emissions)

    logger.info(f"  With production: {len(companies_with_production)}")
    logger.info(f"  With emissions: {len(companies_with_emissions)}")
    logger.info(f"  With BOTH (for SDA): {len(good_companies)}")
    logger.info(f"  Companies: {sorted(good_companies)}")

    # Filter to good companies
    defaults = defaults[defaults["company"].isin(good_companies)]

    # 2. Add historical data (with source provenance)
    for _, row in defaults.iterrows():
        company = row["company"]
        year = int(row["year"])
        metric = row["metric"]
        value = row["value"]
        source = row.get("source", "")
        certainty = row.get("certainty", None)

        if metric == "emissions_mt_co2":
            variable = "Emissions (Historical)"
            unit = "MtCO2"
        elif metric == "production_mt":
            variable = "Production (Historical)"
            unit = "MtSteel"
            value = value * 1000  # Convert Mt to kt for ALD format
        else:
            continue

        rows.append({
            "Company Name": company,
            "Country": "",  # Not tracked at company level
            "Variable": variable,
            "Unit": unit,
            "Year": year,
            "Value": value,
            "Source": source,
            "Certainty": certainty,
        })

    logger.info(f"Added {len(rows)} historical records")

    # 3. Load projections file reference (needed for NZT in step 3f)
    projections_file = PROCESSED_STEEL_DIR / "steel_projections.csv"

    # NOTE: Kampmann BAU is no longer exported here. The R pipeline computes all
    # BAU variants (bau_constant, bau_recent, bau_longterm) from APA historical
    # data for all 26 companies. Kampmann BAU was inconsistent with our APA
    # emissions baseline (e.g. ArcelorMittal Kampmann BAU = 145 Mt by 2029 vs
    # APA actual ~102 Mt). Step 3e now provides uniform APA-based constant BAU
    # for all 26 companies.

    # 3c. GEM closure TP — plant-level transition pathway for all 26 companies
    gem_tp_file = OUTPUTS_COMPANY_STEEL / "steel_gem_tp_annual.csv"
    if gem_tp_file.exists():
        gem_tp = pd.read_csv(gem_tp_file)
        gem_tp = gem_tp[gem_tp["tp_emissions_mt"].notna()]

        # Only add GEM TP for companies that have it
        gem_tp_companies = gem_tp["company"].unique()
        for _, row in gem_tp.iterrows():
            company = row["company"]
            year = int(row["year"])

            # Emissions (TP) from GEM closure model
            rows.append({
                "Company Name": company,
                "Country": "",
                "Variable": "Emissions (TP)",
                "Unit": "MtCO2",
                "Year": year,
                "Value": row["tp_emissions_mt"],
                "Source": "gem_closure",
                "Certainty": None,
            })

            # Production (TP) from GEM closure model
            if pd.notna(row.get("tp_production_mt")):
                rows.append({
                    "Company Name": company,
                    "Country": "",
                    "Variable": "Production (TP)",
                    "Unit": "MtSteel",
                    "Year": year,
                    "Value": row["tp_production_mt"] * 1000,  # Mt to kt
                    "Source": "gem_closure",
                    "Certainty": None,
                })

        logger.info(
            f"Added GEM closure TP for {len(gem_tp_companies)} companies, "
            f"total rows: {len(rows)}"
        )
    else:
        logger.warning(f"GEM closure TP file not found: {gem_tp_file}")
        logger.warning("  Run: python -m pipeline.gem_closure_tp")

    # 3d. Add APA-based historical/BAU for companies not yet in defaults
    # (Ensures all 26 tracked companies have historical data in the ALD file)
    apa_file = OUTPUTS_COMPANY_STEEL / "steel_apa_emissions.csv"
    if apa_file.exists():
        apa_data = pd.read_csv(apa_file)
        # Find companies in APA but not yet in our rows
        existing_companies = set(r["Company Name"] for r in rows
                                 if r["Variable"] == "Emissions (Historical)")
        apa_only = apa_data[~apa_data["company"].isin(existing_companies)]

        if len(apa_only) > 0:
            apa_only_companies = apa_only["company"].unique()
            for _, row in apa_only.iterrows():
                company = row["company"]
                year = int(row["year"])

                # Historical emissions from APA
                rows.append({
                    "Company Name": company,
                    "Country": "",
                    "Variable": "Emissions (Historical)",
                    "Unit": "MtCO2",
                    "Year": year,
                    "Value": row["emissions_mt"],
                    "Source": "apa",
                    "Certainty": None,
                })

                # Historical production from APA
                if pd.notna(row.get("production_mt")):
                    rows.append({
                        "Company Name": company,
                        "Country": "",
                        "Variable": "Production (Historical)",
                        "Unit": "MtSteel",
                        "Year": year,
                        "Value": row["production_mt"] * 1000,  # Mt to kt
                        "Source": "apa",
                        "Certainty": None,
                    })

            logger.info(
                f"Added APA historical for {len(apa_only_companies)} extra companies "
                f"({list(apa_only_companies)}), total rows: {len(rows)}"
            )

    # 3e. GEM plant-level BAU — includes capacity additions, no closures
    # Uses same GEM engine as TP but with skip_closures=True: pre-retirement
    # units keep running, while construction/announced units come online.
    # This replicates Kampmann's BAU methodology. APA-anchored at base year.
    # The R pipeline also computes simpler CAGR-based bau_constant/recent/longterm.
    gem_bau_file = OUTPUTS_COMPANY_STEEL / "steel_gem_bau_annual.csv"
    all_hist_companies = set(r["Company Name"] for r in rows
                             if r["Variable"] == "Emissions (Historical)")
    bau_companies_added = set()

    if gem_bau_file.exists():
        gem_bau = pd.read_csv(gem_bau_file)
        gem_bau = gem_bau[gem_bau["bau_emissions_mt"].notna()]

        for _, row in gem_bau.iterrows():
            company = row["company"]
            if company not in all_hist_companies:
                continue
            year = int(row["year"])

            rows.append({
                "Company Name": company,
                "Country": "",
                "Variable": "Emissions (BAU)",
                "Unit": "MtCO2",
                "Year": year,
                "Value": row["bau_emissions_mt"],
                "Source": "gem_plant_level",
                "Certainty": None,
            })

            if pd.notna(row.get("bau_production_mt")):
                rows.append({
                    "Company Name": company,
                    "Country": "",
                    "Variable": "Production (BAU)",
                    "Unit": "MtSteel",
                    "Year": year,
                    "Value": row["bau_production_mt"] * 1000,  # Mt to kt
                    "Source": "gem_plant_level",
                    "Certainty": None,
                })
            bau_companies_added.add(company)

        logger.info(
            f"Added GEM plant-level BAU for {len(bau_companies_added)} companies, "
            f"total rows: {len(rows)}"
        )
    else:
        logger.warning(f"GEM BAU file not found: {gem_bau_file}")
        logger.warning("  Run: python -m pipeline.gem_closure_tp")

    # Fallback: APA constant BAU for any remaining companies without GEM BAU
    remaining = all_hist_companies - bau_companies_added
    if remaining and apa_file.exists():
        apa_for_bau = pd.read_csv(apa_file)
        for company in sorted(remaining):
            comp_apa = apa_for_bau[apa_for_bau["company"] == company].sort_values("year")
            if comp_apa.empty:
                continue
            latest = comp_apa.iloc[-1]
            latest_em = latest["emissions_mt"]
            latest_prod = latest.get("production_mt", None)

            for year in range(2025, 2051):
                rows.append({
                    "Company Name": company,
                    "Country": "",
                    "Variable": "Emissions (BAU)",
                    "Unit": "MtCO2",
                    "Year": year,
                    "Value": latest_em,
                    "Source": "apa_constant",
                    "Certainty": None,
                })
                if pd.notna(latest_prod):
                    rows.append({
                        "Company Name": company,
                        "Country": "",
                        "Variable": "Production (BAU)",
                        "Unit": "MtSteel",
                        "Year": year,
                        "Value": latest_prod * 1000,
                        "Source": "apa_constant",
                        "Certainty": None,
                    })

        logger.info(
            f"Added APA constant BAU fallback for {len(remaining)} companies "
            f"({sorted(remaining)}), total rows: {len(rows)}"
        )

    # 3f. NZT commitment scenario (emissions only, no production needed)
    # Placed after step 3d so ALL 26 companies' historical data is present.
    # NZT doesn't require production, so filter against all companies with
    # historical emissions — not just good_companies (which requires production).
    if projections_file.exists():
        projections = pd.read_csv(projections_file)
        nzt_proj = projections[
            (projections["scenario"] == "nzt_commitment") &
            projections["emissions_mt_co2"].notna()
        ]

        all_hist_companies_nzt = set(
            r["Company Name"] for r in rows
            if r["Variable"] == "Emissions (Historical)"
        )
        nzt_proj = nzt_proj[nzt_proj["company"].isin(all_hist_companies_nzt)]

        for _, row in nzt_proj.iterrows():
            company = row["company"]
            year = int(row["year"])

            rows.append({
                "Company Name": company,
                "Country": "",
                "Variable": "Emissions (NZT)",
                "Unit": "MtCO2",
                "Year": year,
                "Value": row["emissions_mt_co2"],
                "Source": "oxford_nzt",
                "Certainty": None,
            })

        logger.info(
            f"Added NZT for {nzt_proj['company'].nunique()} companies, "
            f"total rows: {len(rows)}"
        )

    # 4. Create DataFrame and save
    result = pd.DataFrame(rows)

    if len(result) > 0:
        # Sort for readability
        result = result.sort_values(["Company Name", "Variable", "Year"])

        # Save
        out_path = PROCESSED_STEEL_DIR / "steel_ald_combined.csv"
        result.to_csv(out_path, index=False)
        logger.info(f"Saved: {out_path} ({len(result)} rows)")

        # Summary
        logger.info("\n--- ALD Export Summary ---")
        logger.info(f"Companies: {result['Company Name'].nunique()}")
        for var in result["Variable"].unique():
            subset = result[result["Variable"] == var]
            years = f"{subset['Year'].min()}-{subset['Year'].max()}"
            logger.info(f"  {var}: {len(subset)} records, years {years}")

    return result


# ============================================================================
# Standalone execution
# ============================================================================

if __name__ == "__main__":
    result = run_steel_projections()

    if not result.empty:
        print("\n" + "=" * 60)
        print("SAMPLE OUTPUT")
        print("=" * 60)
        print(result.head(20).to_string(index=False))

    # Also export ALD format
    print("\n")
    ald_result = export_ald_format()
