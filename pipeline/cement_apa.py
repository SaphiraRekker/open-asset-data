"""
Cement APA (Asset-based Planning Approach) Calculator

Replicates Kampmann's APA methodology for cement sector:
1. Load GEM Global Cement and Concrete Tracker (asset-level data)
2. Apply emissions factors by technology (wet/dry/semidry) and country
3. Aggregate to company level
4. Project BAU based on asset lifetimes (~40 years for cement kilns)

Output: cement_ald_combined.csv (same format as steel_ald_combined.csv)
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# =============================================================================
# EMISSIONS FACTORS BY TECHNOLOGY
# =============================================================================
# Sources: IPCC 2006, IEA Cement Roadmap, GCCA GNR Database
# Units: tCO2 per tonne of cement

EMISSIONS_FACTORS = {
    # Process type -> base emissions factor (tCO2/t cement)
    # These include both process emissions (calcination ~65%) and fuel combustion (~35%)
    "wet": 0.95,       # Older, less efficient
    "semidry": 0.80,   # Intermediate
    "dry": 0.70,       # Modern standard
    "mixed": 0.75,     # Mix of technologies
    "unknown": 0.75,   # Default assumption
}

# Regional adjustments (multiply base EF by this factor)
# Accounts for different fuel mixes and grid emissions
REGIONAL_FACTORS = {
    "China": 1.05,          # Higher coal use
    "India": 1.00,          # Mixed, improving
    "United States": 0.95,  # More efficient
    "Germany": 0.90,        # Best practice
    "Japan": 0.92,          # Efficient
    "default": 1.00,
}

# Standard cement kiln lifetime (years)
KILN_LIFETIME = 40

# Utilization rate assumption
DEFAULT_UTILIZATION = 0.80


# =============================================================================
# LOAD GEM CEMENT DATA
# =============================================================================

def load_gem_cement(gem_path: Path) -> pd.DataFrame:
    """Load and clean GEM Global Cement and Concrete Tracker data."""

    logger.info(f"Loading GEM cement data from {gem_path}")

    df = pd.read_excel(gem_path, sheet_name="Plant Data")

    # Clean column names
    df.columns = df.columns.str.strip()

    # Select and rename key columns
    df = df[[
        "GEM Plant ID",
        "GEM Asset name (English)",
        "Country/Area",
        "Cement Capacity (millions metric tonnes per annum)",
        "Clinker Capacity (millions metric tonnes per annum)",
        "Operating status",
        "Start date",
        "Owner name (English)",
        "Parent",
        "Production type",
        "CCS/CCUS",
        "Alternative Fuel",
    ]].copy()

    df = df.rename(columns={
        "GEM Plant ID": "plant_id",
        "GEM Asset name (English)": "plant_name",
        "Country/Area": "country",
        "Cement Capacity (millions metric tonnes per annum)": "capacity_mt",
        "Clinker Capacity (millions metric tonnes per annum)": "clinker_capacity_mt",
        "Operating status": "status",
        "Start date": "start_year",
        "Owner name (English)": "owner",
        "Parent": "parent",
        "Production type": "technology",
        "CCS/CCUS": "has_ccs",
        "Alternative Fuel": "has_alt_fuel",
    })

    # Clean technology type
    df["technology"] = df["technology"].str.lower().str.strip()
    df["technology"] = df["technology"].fillna("unknown")

    # Parse start year (handle various formats)
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce")

    # Clean capacity (already in Mt)
    df["capacity_mt"] = pd.to_numeric(df["capacity_mt"], errors="coerce")

    # Extract parent company name (remove percentage ownership)
    df["company"] = df["parent"].apply(extract_company_name)
    # Fallback to owner if no parent
    df.loc[df["company"].isna() | (df["company"] == ""), "company"] = \
        df.loc[df["company"].isna() | (df["company"] == ""), "owner"].apply(extract_company_name)

    logger.info(f"Loaded {len(df)} plants, {df['company'].nunique()} companies")

    return df


def extract_company_name(owner_str: str) -> str:
    """Extract company name from owner string like 'Holcim [50.0%]'."""
    if pd.isna(owner_str) or owner_str == "":
        return ""
    # Remove ownership percentage
    name = str(owner_str).split("[")[0].strip()
    return name


# =============================================================================
# APPLY EMISSIONS FACTORS
# =============================================================================

def get_emissions_factor(technology: str, country: str) -> float:
    """Get emissions factor for a plant based on technology and country."""

    # Base factor by technology
    base_ef = EMISSIONS_FACTORS.get(technology, EMISSIONS_FACTORS["unknown"])

    # Regional adjustment
    regional_factor = REGIONAL_FACTORS.get(country, REGIONAL_FACTORS["default"])

    return base_ef * regional_factor


def calculate_plant_emissions(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Calculate emissions for each plant in a given year."""

    df = df.copy()

    # Filter to operating plants
    operating_statuses = ["operating", "operating pre-retirement"]
    df_operating = df[df["status"].str.lower().isin(operating_statuses)].copy()

    # Get emissions factor for each plant
    df_operating["ef"] = df_operating.apply(
        lambda row: get_emissions_factor(row["technology"], row["country"]),
        axis=1
    )

    # Calculate emissions: capacity * utilization * emissions factor
    df_operating["production_mt"] = df_operating["capacity_mt"] * DEFAULT_UTILIZATION
    df_operating["emissions_mt"] = df_operating["production_mt"] * df_operating["ef"]

    df_operating["year"] = year

    return df_operating


# =============================================================================
# BAU PROJECTIONS
# =============================================================================

def project_bau(df: pd.DataFrame, base_year: int = 2024, end_year: int = 2050) -> pd.DataFrame:
    """
    Project BAU emissions assuming:
    - Current operating plants continue until end of lifetime (start_year + KILN_LIFETIME)
    - Plants are replaced with same technology at end of life
    - No new capacity additions beyond replacements
    """

    logger.info(f"Projecting BAU from {base_year} to {end_year}")

    results = []

    for year in range(base_year, end_year + 1):
        df_year = df.copy()

        # Calculate plant age
        df_year["age"] = year - df_year["start_year"]

        # Plants retire when age > KILN_LIFETIME
        # But in BAU, assume they're replaced with same technology
        # So capacity stays constant (no net retirements in pure BAU)

        # Calculate emissions for this year
        df_year_emissions = calculate_plant_emissions(df_year, year)

        # Aggregate to company level
        company_year = df_year_emissions.groupby("company").agg({
            "production_mt": "sum",
            "emissions_mt": "sum",
            "plant_id": "count",
        }).reset_index()

        company_year = company_year.rename(columns={"plant_id": "n_plants"})
        company_year["year"] = year
        company_year["scenario"] = "bau"

        results.append(company_year)

    return pd.concat(results, ignore_index=True)


# =============================================================================
# HISTORICAL BASELINE
# =============================================================================

def calculate_historical(df: pd.DataFrame, years: list = None) -> pd.DataFrame:
    """Calculate historical emissions for operating plants."""

    if years is None:
        # Go back to 2015 for meaningful pre-Paris baseline
        years = list(range(2015, 2025))

    logger.info(f"Calculating historical emissions for {years}")

    results = []

    for year in years:
        df_year_emissions = calculate_plant_emissions(df, year)

        company_year = df_year_emissions.groupby("company").agg({
            "production_mt": "sum",
            "emissions_mt": "sum",
            "plant_id": "count",
        }).reset_index()

        company_year = company_year.rename(columns={"plant_id": "n_plants"})
        company_year["year"] = year
        company_year["scenario"] = "historical"

        results.append(company_year)

    return pd.concat(results, ignore_index=True)


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def run_cement_apa(
    gem_path: Path,
    output_dir: Path,
    base_year: int = 2024,
    end_year: int = 2050,
) -> pd.DataFrame:
    """
    Run the cement APA calculation.

    Returns DataFrame with columns:
    - company, year, scenario, production_mt, emissions_mt, n_plants
    """

    logger.info("=" * 60)
    logger.info("CEMENT APA CALCULATOR")
    logger.info("=" * 60)

    # Load GEM data
    df = load_gem_cement(gem_path)

    # Calculate historical
    # Historical from 2015 (pre-Paris baseline) to present
    historical = calculate_historical(df, years=list(range(2015, 2025)))

    # Calculate BAU projections
    bau = project_bau(df, base_year=base_year, end_year=end_year)

    # Combine
    combined = pd.concat([historical, bau], ignore_index=True)

    # Calculate intensity
    combined["intensity"] = combined["emissions_mt"] / combined["production_mt"]

    # Sort
    combined = combined.sort_values(["company", "scenario", "year"])

    # Summary stats
    n_companies = combined["company"].nunique()
    total_emissions_2024 = combined[
        (combined["year"] == 2024) & (combined["scenario"] == "historical")
    ]["emissions_mt"].sum()

    logger.info(f"Companies: {n_companies}")
    logger.info(f"Total emissions 2024: {total_emissions_2024:.1f} Mt CO2")

    # Export
    output_path = output_dir / "cement_ald_combined.csv"
    combined.to_csv(output_path, index=False)
    logger.info(f"Exported: {output_path}")

    return combined


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # Default paths
    base_dir = Path(__file__).parent.parent.parent.parent.parent
    gem_path = base_dir / "data/raw/GEM/Global-Cement-and-Concrete-Tracker_July-2025.xlsx"
    output_dir = base_dir / "outputs"

    if not gem_path.exists():
        logger.error(f"GEM cement file not found: {gem_path}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    df = run_cement_apa(gem_path, output_dir)

    print("\n" + "=" * 60)
    print("TOP 10 COMPANIES BY 2024 EMISSIONS")
    print("=" * 60)
    top_10 = df[(df["year"] == 2024) & (df["scenario"] == "historical")].nlargest(10, "emissions_mt")
    print(top_10[["company", "emissions_mt", "production_mt", "n_plants"]].to_string(index=False))
