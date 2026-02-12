"""
Cement emissions projections module.

Combines multiple data sources for cement company emissions:
1. GEM Global Cement Tracker - Asset-level capacity, technology, BAU projections
2. InfluenceMap - Historical company-reported emissions (1990-2023)
3. Climate TRACE - Satellite-derived facility emissions (validation)

Outputs: cement_ald_combined.csv for use in PCP scripts

Usage:
    cd open-asset-data
    python -m pipeline.cement_projections
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .config import (
    PROCESSED_DATA_DIR,
    OUTPUTS_DIR,
    GEM_CEMENT_FILE,
    HISTORICAL_DATA_DIR,
)
from .cement_apa import run_cement_apa, load_gem_cement

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ============================================================================
# Paths
# ============================================================================

INFLUENCEMAP_FILE = HISTORICAL_DATA_DIR / "InfluenceMap" / "emissions_high_granularity.csv"

# ============================================================================
# Company name mapping (standardize names across sources)
# ============================================================================

COMPANY_NAME_MAP = {
    # InfluenceMap -> Canonical
    "Heidelberg Materials": "Heidelberg Materials AG",
    "HeidelbergCement": "Heidelberg Materials AG",
    "Holcim Group": "Holcim AG",
    "Holcim": "Holcim AG",
    "Cemex": "CEMEX SAB de CV",
    "CEMEX": "CEMEX SAB de CV",
    "CRH": "CRH PLC",
    "UltraTech Cement": "UltraTech Cement Ltd",
    "Taiheiyo Cement": "Taiheiyo Cement Corp",
    "Adani Group": "Adani Group",
    "China (Cement)": "China (Cement)",  # Aggregate

    # GEM -> Canonical
    "Heidelberg Materials AG": "Heidelberg Materials AG",
    "Holcim AG": "Holcim AG",
    "CEMEX SAB de CV": "CEMEX SAB de CV",
    "CRH PLC": "CRH PLC",
    "UltraTech Cement Ltd": "UltraTech Cement Ltd",
}


def normalize_company_name(name: str) -> str:
    """Normalize company name to canonical form."""
    if pd.isna(name):
        return name
    clean = str(name).strip()
    return COMPANY_NAME_MAP.get(clean, clean)


# ============================================================================
# Load InfluenceMap Historical Data
# ============================================================================

def load_influencemap_cement() -> pd.DataFrame:
    """Load InfluenceMap historical emissions for cement companies."""

    if not INFLUENCEMAP_FILE.exists():
        logger.warning(f"InfluenceMap file not found: {INFLUENCEMAP_FILE}")
        return pd.DataFrame()

    logger.info(f"Loading InfluenceMap data from {INFLUENCEMAP_FILE}")

    df = pd.read_csv(INFLUENCEMAP_FILE)

    # Filter to cement
    cement = df[df['commodity'] == 'Cement'].copy()

    if len(cement) == 0:
        logger.warning("No cement data found in InfluenceMap")
        return pd.DataFrame()

    logger.info(f"InfluenceMap cement: {len(cement)} records, {cement['parent_entity'].nunique()} companies")

    # Standardize columns
    # Note: production_value appears to be emissions based on unit "Million Tonnes CO2"
    # and total_emissions_MtCO2e is a calculated/adjusted value
    result = cement[['year', 'parent_entity', 'production_value', 'total_emissions_MtCO2e', 'source']].copy()
    result = result.rename(columns={
        'parent_entity': 'company',
        'total_emissions_MtCO2e': 'emissions_mt',
    })

    # Normalize company names
    result['company'] = result['company'].apply(normalize_company_name)

    # Add metadata
    result['data_source'] = 'InfluenceMap'
    result['scenario'] = 'historical'

    # For cement, we need production (activity) data too
    # InfluenceMap doesn't have separate production, so we'll need to get it from GEM
    # For now, estimate production from emissions using typical intensity (~0.6-0.7 tCO2/t)
    AVG_CEMENT_INTENSITY = 0.65  # tCO2 per tonne cement
    result['production_mt'] = result['emissions_mt'] / AVG_CEMENT_INTENSITY

    logger.info(f"InfluenceMap processed: {len(result)} records")
    logger.info(f"  Year range: {result['year'].min()}-{result['year'].max()}")
    logger.info(f"  Companies: {result['company'].nunique()}")

    return result


# ============================================================================
# Combine Data Sources
# ============================================================================

def combine_cement_data(
    gem_path: Path = GEM_CEMENT_FILE,
    output_dir: Path = OUTPUTS_DIR,
) -> pd.DataFrame:
    """
    Combine GEM APA projections with InfluenceMap historical data.

    Priority:
    1. InfluenceMap for historical (company-reported, higher quality)
    2. GEM APA for historical fallback and BAU projections
    """

    logger.info("=" * 60)
    logger.info("CEMENT DATA COMBINER")
    logger.info("=" * 60)

    # Load GEM APA data (historical + BAU projections)
    logger.info("\n--- Loading GEM APA data ---")
    gem_apa = run_cement_apa(gem_path, output_dir)
    gem_apa['company'] = gem_apa['company'].apply(normalize_company_name)

    # Load InfluenceMap historical
    logger.info("\n--- Loading InfluenceMap historical ---")
    influencemap = load_influencemap_cement()

    # Identify companies with InfluenceMap data
    im_companies = set(influencemap['company'].unique()) if len(influencemap) > 0 else set()
    gem_companies = set(gem_apa['company'].unique())

    logger.info(f"\nCompany overlap:")
    logger.info(f"  InfluenceMap: {len(im_companies)} companies")
    logger.info(f"  GEM APA: {len(gem_companies)} companies")
    logger.info(f"  Overlap: {len(im_companies & gem_companies)} companies")

    # Strategy:
    # - For companies with InfluenceMap data: use IM for historical, GEM for BAU
    # - For companies without InfluenceMap: use GEM for both

    results = []

    # 1. Companies with InfluenceMap data
    for company in im_companies:
        # Get InfluenceMap historical
        im_hist = influencemap[influencemap['company'] == company].copy()
        im_years = set(im_hist['year'])

        # Get GEM BAU projections (years not in InfluenceMap)
        gem_company = gem_apa[
            (gem_apa['company'] == company) &
            (~gem_apa['year'].isin(im_years))
        ].copy()

        # Also get GEM historical for activity data (production)
        # since InfluenceMap production is estimated
        gem_hist = gem_apa[
            (gem_apa['company'] == company) &
            (gem_apa['year'].isin(im_years)) &
            (gem_apa['scenario'] == 'historical')
        ].copy()

        if len(gem_hist) > 0:
            # Use GEM production values for InfluenceMap years
            im_hist = im_hist.merge(
                gem_hist[['year', 'production_mt']].rename(columns={'production_mt': 'gem_production_mt'}),
                on='year',
                how='left'
            )
            # Prefer GEM production if available
            im_hist['production_mt'] = im_hist['gem_production_mt'].fillna(im_hist['production_mt'])
            im_hist = im_hist.drop(columns=['gem_production_mt'])

        results.append(im_hist)

        if len(gem_company) > 0:
            gem_company['data_source'] = 'GEM_APA'
            results.append(gem_company[['company', 'year', 'scenario', 'production_mt', 'emissions_mt', 'data_source']])

    # 2. Companies without InfluenceMap data - use GEM only
    gem_only_companies = gem_companies - im_companies
    for company in gem_only_companies:
        gem_company = gem_apa[gem_apa['company'] == company].copy()
        gem_company['data_source'] = 'GEM_APA'
        results.append(gem_company[['company', 'year', 'scenario', 'production_mt', 'emissions_mt', 'data_source']])

    # Combine all
    combined = pd.concat(results, ignore_index=True)

    # Calculate intensity
    combined['intensity'] = combined['emissions_mt'] / combined['production_mt']
    combined['intensity'] = combined['intensity'].replace([np.inf, -np.inf], np.nan)

    # Sort
    combined = combined.sort_values(['company', 'scenario', 'year'])

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("COMBINED DATA SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total records: {len(combined)}")
    logger.info(f"Companies: {combined['company'].nunique()}")
    logger.info(f"Year range: {combined['year'].min()}-{combined['year'].max()}")

    logger.info("\nBy data source:")
    for source in combined['data_source'].unique():
        count = len(combined[combined['data_source'] == source])
        companies = combined[combined['data_source'] == source]['company'].nunique()
        logger.info(f"  {source}: {count} records, {companies} companies")

    logger.info("\nBy scenario:")
    for scenario in combined['scenario'].unique():
        count = len(combined[combined['scenario'] == scenario])
        logger.info(f"  {scenario}: {count} records")

    # Export
    output_path = output_dir / "cement_ald_combined.csv"
    combined.to_csv(output_path, index=False)
    logger.info(f"\nExported: {output_path}")

    return combined


# ============================================================================
# Top Companies Analysis
# ============================================================================

def get_top_cement_companies(df: pd.DataFrame, n: int = 20, year: int = 2023) -> pd.DataFrame:
    """Get top N cement companies by emissions in a given year."""

    latest = df[
        (df['year'] == year) &
        (df['scenario'].isin(['historical', 'bau']))
    ].copy()

    # Aggregate by company (in case of duplicates)
    top = latest.groupby('company').agg({
        'emissions_mt': 'sum',
        'production_mt': 'sum',
        'data_source': 'first',
    }).reset_index()

    top['intensity'] = top['emissions_mt'] / top['production_mt']
    top = top.nlargest(n, 'emissions_mt')

    return top


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    df = combine_cement_data()

    print("\n" + "=" * 60)
    print("TOP 20 CEMENT COMPANIES BY 2023 EMISSIONS")
    print("=" * 60)

    top = get_top_cement_companies(df, n=20, year=2023)
    print(top[['company', 'emissions_mt', 'production_mt', 'intensity', 'data_source']].to_string(index=False))
