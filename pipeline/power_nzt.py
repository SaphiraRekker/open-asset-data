"""
Power Net Zero Tracker (NZT) projections module.

Adds forward projections based on company commitments from:
1. Oxford Net Zero Tracker - End targets (net zero by 2050) and interim targets (2030)

Usage:
    cd open-asset-data
    python -m pipeline.power_nzt
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .config import (
    OUTPUTS_DIR,
    OUTPUTS_COMPANY_POWER,
    OXFORD_NZT_FILE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ============================================================================
# Company name mapping (NZT -> Canonical for power)
# ============================================================================

NZT_POWER_MAP = {
    # Oxford NZT name -> Our canonical names
    # Australian companies
    "AGL Energy": "AGL Energy Limited",
    "Origin Energy": "Origin Energy Limited",
    "EnergyAustralia": "EnergyAustralia Holdings Limited",

    # Global power companies (for future expansion)
    "Orsted": "Orsted A/S",
    "SSE": "SSE plc",
    "NRG Energy": "NRG Energy Inc",
    "E.ON": "E.ON SE",
    "EDP Energias de Portugal": "EDP - Energias de Portugal SA",
    "Iberdrola": "Iberdrola SA",
    "EDF": "Electricite de France SA",
    "Duke Energy": "Duke Energy Corporation",
    "Southern Company": "Southern Company",
    "American Electric Power Company": "American Electric Power Company Inc",
    "Xcel Energy": "Xcel Energy Inc",
    "NextEra Energy": "NextEra Energy Inc",
    "Exelon": "Exelon Corporation",
    "Tokyo Electric Power": "Tokyo Electric Power Company Holdings Inc",
    "Kansai Electric Power Co": "Kansai Electric Power Co Inc",
    "NTPC": "NTPC Limited",
    "Korea Electric Power": "Korea Electric Power Corporation",
    "Fortis (Canada)": "Fortis Inc",
    "CLP Holdings": "CLP Holdings Limited",
    "Fortum": "Fortum Oyj",
    "CEZ": "CEZ a.s.",
    "AES": "AES Corporation",
    "Emera": "Emera Incorporated",
    "PTT": "PTT Public Company Limited",
    "Constellation Energy": "Constellation Energy Corporation",
}


def harmonize_company(name: str) -> str:
    """Map company name to canonical form."""
    if pd.isna(name):
        return name
    clean = name.strip()
    return NZT_POWER_MAP.get(clean, clean)


# ============================================================================
# Load Oxford NZT targets for power companies
# ============================================================================

def load_oxford_nzt_power() -> pd.DataFrame:
    """Load Oxford Net Zero Tracker targets for power companies.

    Returns:
        DataFrame with columns:
          - company: Canonical company name
          - end_target_type: Type of 2050 target (Net zero, Carbon neutral, etc.)
          - end_target_year: Target year (2045, 2050, 2055)
          - interim_target_year: Interim target year (usually 2030)
          - interim_target_pct: Interim reduction percentage
    """
    if not OXFORD_NZT_FILE.exists():
        logger.warning(f"Oxford NZT file not found: {OXFORD_NZT_FILE}")
        return pd.DataFrame()

    try:
        # Read with skip for proper headers
        df = pd.read_excel(OXFORD_NZT_FILE, skiprows=1)

        logger.info(f"Oxford NZT: Loaded {len(df)} entities")

        # Filter to companies only
        df = df[df['Entity_type'] == 'Company'].copy()
        logger.info(f"Companies only: {len(df)}")

        # Find power companies - check both Industry column AND known power company names
        power_industries = ['Power generation']

        # Known power company names that may be classified under other industries
        power_company_names = [
            'agl energy', 'origin energy', 'energyaustralia', 'stanwell',
            'orsted', 'sse', 'nrg energy', 'e.on', 'edp', 'iberdrola',
            'edf', 'duke energy', 'southern company', 'american electric power',
            'xcel energy', 'nextera', 'exelon', 'tokyo electric', 'tepco',
            'kansai electric', 'ntpc', 'korea electric', 'kepco', 'fortis',
            'clp holdings', 'fortum', 'cez', 'aes', 'emera', 'ptt',
            'constellation energy', 'engie', 'enel', 'rwe', 'centrica',
            'national grid', 'dominion energy', 'entergy', 'pg&e', 'edison'
        ]

        # Match by industry OR by name
        industry_mask = df['Industry'].isin(power_industries)
        name_mask = df['Name'].str.lower().str.contains('|'.join(power_company_names), na=False)

        power = df[industry_mask | name_mask].copy()

        if len(power) == 0:
            logger.warning("No power companies found in Oxford NZT")
            return pd.DataFrame()

        logger.info(f"Power companies found: {len(power)}")

        # Extract relevant columns
        result = pd.DataFrame({
            'company_raw': power['Name'],
            'country': power['Country'],
            'industry': power['Industry'],
            'end_target_type': power.get('End_target', None),
            'end_target_year': power.get('End_target_year', None),
            'end_target_pct': power.get('End_target_percentage_reduction', None),
            'end_target_status': power.get('Status_of_end_target', None),
            'interim_target_type': power.get('Interim_target', None),
            'interim_target_year': power.get('Interim_target_year', None),
            'interim_target_pct': power.get('Interim_target_percentage_reduction', None),
            'interim_baseline_year': power.get('Interim_target_baseline_year', None),
        })

        # Normalize company names
        result['company'] = result['company_raw'].apply(harmonize_company)

        # Log found targets
        has_end = result['end_target_year'].notna().sum()
        has_interim = result['interim_target_year'].notna().sum()

        logger.info(f"Power companies in NZT: {len(result)}")
        logger.info(f"  With end targets: {has_end}")
        logger.info(f"  With interim targets: {has_interim}")

        # Show what we found
        for _, row in result.head(20).iterrows():
            logger.info(f"  {row['company_raw']} ({row['industry']}): "
                       f"{row['end_target_type']} by {row['end_target_year']}, "
                       f"interim {row['interim_target_pct']}% by {row['interim_target_year']}")

        return result

    except Exception as e:
        logger.error(f"Error loading Oxford NZT: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


# ============================================================================
# Generate NZT-based projections
# ============================================================================

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
        interim_pct = float(interim_pct)
        # Calculate target emissions (interim_pct is reduction % from baseline)
        interim_emissions = base_emissions * (1 - interim_pct / 100)
        if interim_year > base_year:
            trajectory.append((interim_year, max(0, interim_emissions)))

    # Add end target point
    if pd.notna(end_target_year):
        end_target_year = int(end_target_year)
        # Determine end target emissions
        if "net zero" in str(end_target_type).lower():
            # Net zero = residual emissions only (assume 2% residual for power - easier to decarbonize)
            end_emissions = base_emissions * 0.02
        elif "carbon neutral" in str(end_target_type).lower():
            # Carbon neutrality may include offsets, assume 5% residual
            end_emissions = base_emissions * 0.05
        elif "reduction" in str(end_target_type).lower():
            # Emissions reduction target
            end_pct = nzt_row.get("end_target_pct", 40)
            end_emissions = base_emissions * (1 - end_pct / 100) if pd.notna(end_pct) else base_emissions * 0.6
        elif "no target" in str(end_target_type).lower():
            # No target - skip
            return pd.DataFrame()
        else:
            # Unknown target type, assume 90% reduction for power sector
            end_emissions = base_emissions * 0.10

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
            emissions = trajectory[0][1]
        elif not after:
            emissions = trajectory[-1][1]
        else:
            # Linear interpolation
            y1, e1 = before[-1]
            y2, e2 = after[0]
            if y2 == y1:
                emissions = e1
            else:
                emissions = e1 + (e2 - e1) * (year - y1) / (y2 - y1)

        rows.append({
            "company": company,
            "year": year,
            "emissions_mt": round(max(0, emissions), 3),
            "scenario": "nzt",
            "data_source": "oxford_nzt",
        })

    return pd.DataFrame(rows)


def generate_power_nzt_projections(
    historical_df: pd.DataFrame,
    nzt_df: pd.DataFrame
) -> pd.DataFrame:
    """Generate commitment-based projections for power companies with NZT data.

    Args:
        historical_df: Historical emissions with columns: company, year, emissions_mt
        nzt_df: Oxford NZT data from load_oxford_nzt_power()

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
            # Try matching by raw name
            company_hist = historical_df[historical_df["company"] == nzt_row["company_raw"]]

        if company_hist.empty:
            logger.debug(f"No historical data for {company}, skipping NZT projection")
            continue

        # Get most recent year's emissions
        latest = company_hist.sort_values("year").iloc[-1]
        base_year = int(latest["year"])
        base_emissions = float(latest["emissions_mt"])

        # Generate projection
        proj = project_nzt_commitment(
            company=company,
            base_emissions=base_emissions,
            base_year=base_year,
            nzt_row=nzt_row,
        )

        if not proj.empty:
            all_projections.append(proj)
            target_2050 = proj[proj['year'] == 2050]['emissions_mt'].values
            target_2050_str = f"{target_2050[0]:.1f}" if len(target_2050) > 0 else "N/A"
            logger.info(f"Generated NZT projection for {company}: "
                       f"{base_year}-{proj['year'].max()}, "
                       f"{base_emissions:.1f} -> {target_2050_str} Mt")

    if all_projections:
        result = pd.concat(all_projections, ignore_index=True)
        logger.info(f"NZT projections: {len(result)} records, "
                   f"{result['company'].nunique()} companies")
        return result

    return pd.DataFrame()


# ============================================================================
# Main function to add NZT to power data
# ============================================================================

def add_nzt_to_power(
    power_data_path: Path = None,
    output_dir: Path = OUTPUTS_COMPANY_POWER,
) -> pd.DataFrame:
    """Add NZT commitment projections to power data.

    Reads power ALD data, adds NZT scenario, and saves back.
    """
    if power_data_path is None:
        # Try to find power data
        power_data_path = output_dir / "power_comprehensive_annual.csv"
        if not power_data_path.exists():
            power_data_path = OUTPUTS_COMPANY_POWER / "power_comprehensive_annual.csv"

    logger.info("=" * 60)
    logger.info("ADDING NZT PROJECTIONS TO POWER DATA")
    logger.info("=" * 60)

    # Load existing power data
    if not power_data_path.exists():
        logger.error(f"Power data not found: {power_data_path}")
        return pd.DataFrame()

    power = pd.read_csv(power_data_path)
    logger.info(f"Loaded power data: {len(power)} rows, {power['company'].nunique()} companies")

    # Load NZT targets
    logger.info("\n--- Loading Oxford NZT targets ---")
    nzt_df = load_oxford_nzt_power()

    if nzt_df.empty:
        logger.warning("No NZT targets found for power, returning original data")
        return power

    # Prepare historical data for matching
    # Need to extract latest ACTUAL emissions per company (not projections)
    historical = power.copy()

    # For power comprehensive data, we need to find the last year with actual cumulative data
    # and compute the annual emissions for that year
    if 'cumulative_emissions_2020' in historical.columns:
        # Filter to rows with actual cumulative data (not NA)
        historical = historical[historical['cumulative_emissions_2020'].notna()].copy()

        if len(historical) > 0:
            # Compute annual emissions from cumulative
            historical = historical.sort_values(['company', 'year'])
            historical['prev_cumulative'] = historical.groupby('company')['cumulative_emissions_2020'].shift(1)
            historical['emissions_mt'] = historical['cumulative_emissions_2020'] - historical['prev_cumulative'].fillna(0)

            # Get the most recent year with actual data per company
            idx = historical.groupby('company')['year'].idxmax()
            latest = historical.loc[idx].reset_index(drop=True)

            logger.info(f"Historical data: {len(latest)} companies with actual emissions")
            logger.info(f"Years range: {historical['year'].min()} - {historical['year'].max()}")
        else:
            logger.warning("No actual cumulative emissions data found")
            latest = pd.DataFrame()
    else:
        # Fallback - try to use BAU emissions
        if 'projected_emissions_bau_primary' in historical.columns:
            historical['emissions_mt'] = historical['projected_emissions_bau_primary']

        historical = historical.sort_values(['company', 'year'])
        latest = historical.groupby('company').first().reset_index()  # Use earliest year
        logger.info(f"Historical data (fallback): {len(latest)} companies")

    # Generate NZT projections
    logger.info("\n--- Generating NZT projections ---")
    nzt_projections = generate_power_nzt_projections(latest, nzt_df)

    if nzt_projections.empty:
        logger.warning("No NZT projections generated - no matching companies found")
        logger.info("Our companies: " + ", ".join(power['company'].unique()[:10]))
        logger.info("NZT companies: " + ", ".join(nzt_df['company'].unique()[:10]))
        return power

    # Save NZT projections separately for the comprehensive script to use
    nzt_output = output_dir / "power_nzt_projections.csv"
    nzt_projections.to_csv(nzt_output, index=False)
    logger.info(f"\nSaved NZT projections: {nzt_output}")

    # Summary
    logger.info("\n--- NZT Projections Summary ---")
    logger.info(f"Total records: {len(nzt_projections)}")
    logger.info(f"Companies: {nzt_projections['company'].nunique()}")

    return nzt_projections


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    df = add_nzt_to_power()

    if not df.empty:
        print("\n" + "=" * 60)
        print("NZT PROJECTIONS SAMPLE")
        print("=" * 60)

        # Show 2024 vs 2030 vs 2050 for each company
        years = [2024, 2030, 2050]
        sample = df[df['year'].isin(years)]
        pivot = sample.pivot_table(
            index='company',
            columns='year',
            values='emissions_mt',
            aggfunc='first'
        )
        print(pivot.to_string())
