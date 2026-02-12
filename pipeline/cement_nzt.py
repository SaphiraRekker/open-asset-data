"""
Cement Net Zero Tracker (NZT) projections module.

Adds forward projections based on company commitments from:
1. Oxford Net Zero Tracker - End targets (net zero by 2050) and interim targets (2030)

Usage:
    cd open-asset-data
    python -m pipeline.cement_nzt
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .config import (
    OUTPUTS_DIR,
    OUTPUTS_COMPANY_CEMENT,
    OXFORD_NZT_FILE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ============================================================================
# Company name mapping (NZT -> Canonical for cement)
# ============================================================================

NZT_CEMENT_MAP = {
    # Oxford NZT -> Our canonical names
    "Holcim Ltd": "Holcim AG",
    "Holcim": "Holcim AG",
    "HeidelbergCement": "Heidelberg Materials AG",
    "Heidelberg Materials": "Heidelberg Materials AG",
    "CEMEX": "CEMEX SAB de CV",
    "Cemex": "CEMEX SAB de CV",
    "CRH Plc": "CRH PLC",
    "CRH": "CRH PLC",
    "Siam Cement": "Siam Cement PCL",
    "Anhui Conch Cement": "Anhui Conch Cement Co Ltd",
    "China Resources Cement Holdings": "China Resources Cement Holdings Ltd",
    "UltraTech Cement": "UltraTech Cement Ltd",
    "Taiheiyo Cement": "Taiheiyo Cement Corp",
}


def harmonize_company(name: str) -> str:
    """Map company name to canonical form."""
    if pd.isna(name):
        return name
    clean = name.strip()
    return NZT_CEMENT_MAP.get(clean, clean)


# ============================================================================
# Load Oxford NZT targets for cement companies
# ============================================================================

def load_oxford_nzt_cement() -> pd.DataFrame:
    """Load Oxford Net Zero Tracker targets for cement companies.

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
        # Read with multi-level header
        df = pd.read_excel(OXFORD_NZT_FILE, header=[0, 1])

        # Flatten column names
        df.columns = ['_'.join(col).strip() for col in df.columns.values]

        logger.info(f"Oxford NZT: Loaded {len(df)} entities")

        # Search for cement-related companies
        name_col = 'Entity type and location_Name'
        cement_keywords = [
            'cement', 'holcim', 'heidelberg', 'cemex', 'crh', 'ultratech',
            'lafarge', 'buzzi', 'vicat', 'dangote', 'taiheiyo', 'siam cement',
            'anhui conch', 'china resources cement', 'shree cement', 'acc ltd',
            'ambuja', 'dalmia', 'ramco'
        ]

        # Filter to cement companies
        mask = df[name_col].str.lower().str.contains('|'.join(cement_keywords), na=False)
        cement = df[mask].copy()

        if len(cement) == 0:
            logger.warning("No cement companies found in Oxford NZT")
            return pd.DataFrame()

        # Extract relevant columns
        result = pd.DataFrame({
            'company_raw': cement[name_col],
            'end_target_type': cement.get('End target information_End_target', None),
            'end_target_year': cement.get('End target information_End_target_year', None),
            'end_target_pct': cement.get('End target information_End_target_percentage_reduction', None),
            'end_target_status': cement.get('End target information_Status_of_end_target', None),
            'interim_target_type': cement.get('Interim target information_Interim_target', None),
            'interim_target_year': cement.get('Interim target information_Interim_target_year', None),
            'interim_target_pct': cement.get('Interim target information_Interim_target_percentage_reduction', None),
            'interim_baseline_year': cement.get('Interim target information_Interim_target_baseline_year', None),
        })

        # Normalize company names
        result['company'] = result['company_raw'].apply(harmonize_company)

        # Log found targets
        has_end = result['end_target_year'].notna().sum()
        has_interim = result['interim_target_year'].notna().sum()

        logger.info(f"Cement companies in NZT: {len(result)}")
        logger.info(f"  With end targets: {has_end}")
        logger.info(f"  With interim targets: {has_interim}")

        # Show what we found
        for _, row in result.iterrows():
            logger.info(f"  {row['company']}: {row['end_target_type']} by {row['end_target_year']}, "
                       f"interim {row['interim_target_pct']}% by {row['interim_target_year']}")

        return result

    except Exception as e:
        logger.error(f"Error loading Oxford NZT: {e}")
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
            # Net zero = residual emissions only (assume 5% residual for cement - hard to decarbonize)
            end_emissions = base_emissions * 0.05
        elif "carbon neutral" in str(end_target_type).lower():
            # Carbon neutrality may include offsets, assume 10% residual
            end_emissions = base_emissions * 0.10
        elif "reduction" in str(end_target_type).lower():
            # Emissions reduction target
            end_pct = nzt_row.get("end_target_pct", 40)
            end_emissions = base_emissions * (1 - end_pct / 100) if pd.notna(end_pct) else base_emissions * 0.6
        elif "no target" in str(end_target_type).lower():
            # No target - skip
            return pd.DataFrame()
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


def generate_cement_nzt_projections(
    historical_df: pd.DataFrame,
    nzt_df: pd.DataFrame
) -> pd.DataFrame:
    """Generate commitment-based projections for cement companies with NZT data.

    Args:
        historical_df: Historical emissions with columns: company, year, emissions_mt
        nzt_df: Oxford NZT data from load_oxford_nzt_cement()

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
            logger.info(f"Generated NZT projection for {company}: "
                       f"{base_year}-{proj['year'].max()}, "
                       f"{base_emissions:.1f} -> {proj[proj['year']==2050]['emissions_mt'].values[0]:.1f} Mt")

    if all_projections:
        result = pd.concat(all_projections, ignore_index=True)
        logger.info(f"NZT projections: {len(result)} records, "
                   f"{result['company'].nunique()} companies")
        return result

    return pd.DataFrame()


# ============================================================================
# Main function to add NZT to cement data
# ============================================================================

def add_nzt_to_cement(
    cement_data_path: Path = None,
    output_dir: Path = OUTPUTS_COMPANY_CEMENT,
) -> pd.DataFrame:
    """Add NZT commitment projections to cement data.

    Reads cement comprehensive data, generates NZT projections, and saves separately.
    """
    if cement_data_path is None:
        cement_data_path = output_dir / "cement_comprehensive_annual.csv"
        if not cement_data_path.exists():
            cement_data_path = OUTPUTS_COMPANY_CEMENT / "cement_comprehensive_annual.csv"

    logger.info("=" * 60)
    logger.info("ADDING NZT PROJECTIONS TO CEMENT DATA")
    logger.info("=" * 60)

    # Load existing cement data
    if not cement_data_path.exists():
        logger.error(f"Cement data not found: {cement_data_path}")
        return pd.DataFrame()

    cement = pd.read_csv(cement_data_path)
    logger.info(f"Loaded cement data: {len(cement)} rows, {cement['company'].nunique()} companies")

    # Load NZT targets
    logger.info("\n--- Loading Oxford NZT targets ---")
    nzt_df = load_oxford_nzt_cement()

    if nzt_df.empty:
        logger.warning("No NZT targets found for cement, returning empty")
        return pd.DataFrame()

    # Prepare historical data for matching
    # For cement comprehensive data, we need to find the last year with actual cumulative data
    if 'cumulative_emissions_2020' in cement.columns:
        # Filter to rows with actual cumulative data (not NA)
        historical = cement[cement['cumulative_emissions_2020'].notna()].copy()

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
        logger.warning("cumulative_emissions_2020 column not found")
        latest = pd.DataFrame()

    if latest.empty:
        logger.warning("No historical data available for matching")
        return pd.DataFrame()

    # Generate NZT projections
    logger.info("\n--- Generating NZT projections ---")
    nzt_projections = generate_cement_nzt_projections(latest, nzt_df)

    if nzt_projections.empty:
        logger.warning("No NZT projections generated - no matching companies found")
        logger.info("Our companies (sample): " + ", ".join(cement['company'].unique()[:10]))
        logger.info("NZT companies: " + ", ".join(nzt_df['company'].unique()[:10]))
        return pd.DataFrame()

    # Save NZT projections separately for the comprehensive script to use
    nzt_output = output_dir / "cement_nzt_projections.csv"
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
    df = add_nzt_to_cement()

    if not df.empty:
        print("\n" + "=" * 60)
        print("NZT PROJECTIONS SAMPLE")
        print("=" * 60)

        nzt_data = df[df['scenario'] == 'nzt']
        if not nzt_data.empty:
            # Show 2024 vs 2030 vs 2050 for each company
            years = [2024, 2030, 2050]
            sample = nzt_data[nzt_data['year'].isin(years)]
            pivot = sample.pivot_table(
                index='company',
                columns='year',
                values='emissions_mt',
                aggfunc='first'
            )
            print(pivot.to_string())
