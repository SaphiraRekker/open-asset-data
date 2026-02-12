"""
Kampmann TP Source Audit Module.

Full source trace: for each of the 10 companies with Kampmann TP data,
traces the TP emission trajectory back to specific GEM closure dates,
unit-level announcements, and press releases.

Workflow:
1. Load Kampmann TP emissions (from SteelALD.csv) per company per year
2. Load GEM unit-level data (BF, BOF, EAF, DRI) with closure dates & statuses
3. Match units to companies via COMPANY_GEM_PATTERNS
4. Build GEM-derived closure trajectories (remove pre-retirement capacity at
   expected closure year, apply EFs)
5. Compare with Kampmann year-by-year; flag divergences > 5%
6. Output: kampmann_tp_audit.csv + kampmann_closure_inventory.csv

Usage:
    cd open-asset-data
    python -m pipeline.kampmann_audit
"""

import logging
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from .config import (
    GEM_STEEL_IRON_UNITS_FILE,
    GEM_STEEL_STEEL_UNITS_FILE,
    GEM_STEEL_PLANTS_FILE,
    KAMPMANN_ALD_FILE,
    KAMPMANN_EXCEL_FILE,
    OUTPUTS_COMPANY_STEEL,
    PROCESSED_STEEL_DIR,
    EF_BF_BOF,
)
from .apa_calculator import (
    COMPANY_GEM_PATTERNS,
    OWNERSHIP_TRANSFERS,
    EF_EAF,
    EF_DRI_COAL,
    EF_DRI_GAS,
    EF_REFERENCE_YEAR,
    EF_BF_BOF_ANNUAL_IMPROVEMENT,
    get_plant_ef,
    COUNTRY_TO_EF_REGION,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# Companies with Kampmann TP data
KAMPMANN_TP_COMPANIES = [
    "ArcelorMittal",
    "Baoshan Iron & Steel",
    "BlueScope Steel",
    "China Steel",
    "Nippon Steel",
    "POSCO Holdings",
    "SSAB",
    "Severstal",
    "Tata Steel",
    "ThyssenKrupp",
]

# Companies without Kampmann TP (use BAU fallback)
KAMPMANN_NO_TP = ["Gerdau", "JFE Holdings", "JSW Steel", "Nucor"]

# All 26 tracked companies
ALL_COMPANIES = KAMPMANN_TP_COMPANIES + KAMPMANN_NO_TP + [
    "US Steel", "Hyundai Steel", "Cleveland-Cliffs", "Kobe Steel",
    "voestalpine", "SAIL", "Steel Dynamics", "Salzgitter",
    "Ternium", "NLMK", "Evraz", "Liberty Steel",
]


# ============================================================================
# GEM Unit-Level Data Loading
# ============================================================================

def _parse_date_to_year(val) -> float:
    """Parse a mixed-format date value to a year (float).

    GEM files have: datetime objects, integer years, "unknown", NaN.
    Returns np.nan for unparseable values.
    """
    if pd.isna(val) or val == "unknown" or val == "":
        return np.nan
    if isinstance(val, (int, float)):
        y = float(val)
        return y if 1900 <= y <= 2100 else np.nan
    if isinstance(val, pd.Timestamp):
        return float(val.year)
    if isinstance(val, str):
        # Try extracting a 4-digit year
        match = re.search(r"(19|20)\d{2}", str(val))
        if match:
            return float(match.group())
    return np.nan


def load_gem_units() -> pd.DataFrame:
    """Load all GEM unit-level data (BF, BOF, EAF, DRI) into a single DataFrame.

    Returns DataFrame with columns:
        gem_plant_id, gem_unit_id, unit_name, unit_status,
        start_year, pre_retirement_announced_year, retired_year,
        capacity_ttpa, unit_type (bf/bof/eaf/dri), parent_company,
        country, process_type
    """
    all_units = []

    # ----- Blast Furnaces (Iron units) -----
    logger.info("Loading GEM Iron unit data...")
    iron_xls = pd.ExcelFile(GEM_STEEL_IRON_UNITS_FILE)

    if "Blast furnaces" in iron_xls.sheet_names:
        bf = pd.read_excel(iron_xls, sheet_name="Blast furnaces")
        bf_norm = pd.DataFrame({
            "gem_plant_id": bf["GEM Plant ID"],
            "gem_unit_id": bf["GEM Unit ID"],
            "unit_name": bf["Unit Name"],
            "unit_status": bf["Unit Status"].str.strip().str.lower(),
            "start_year": bf["Start Date"].apply(_parse_date_to_year),
            "pre_retirement_announced_year": bf["Pre-retirement Announcement Date"].apply(_parse_date_to_year),
            "retired_year": bf["Retired Date"].apply(_parse_date_to_year),
            "capacity_ttpa": pd.to_numeric(bf["Current Capacity (ttpa)"], errors="coerce"),
            "unit_type": "bf",
        })
        all_units.append(bf_norm)
        logger.info(f"  Blast furnaces: {len(bf_norm)} units")

    if "DRI furnaces" in iron_xls.sheet_names:
        dri = pd.read_excel(iron_xls, sheet_name="DRI furnaces")
        dri_norm = pd.DataFrame({
            "gem_plant_id": dri["GEM Plant ID"],
            "gem_unit_id": dri["GEM Unit ID"],
            "unit_name": dri["Unit Name"],
            "unit_status": dri["Unit Status"].str.strip().str.lower(),
            "start_year": dri["Start Date"].apply(_parse_date_to_year),
            "pre_retirement_announced_year": dri["Pre-retirement Announcement Date"].apply(_parse_date_to_year),
            "retired_year": dri["Retired Date"].apply(_parse_date_to_year),
            "capacity_ttpa": pd.to_numeric(dri["Current Capacity (ttpa)"], errors="coerce"),
            "unit_type": "dri",
            "reductant": dri.get("Reductant", pd.Series(dtype=str)).str.strip().str.lower()
            if "Reductant" in dri.columns else pd.Series(dtype=str),
        })
        all_units.append(dri_norm)
        logger.info(f"  DRI furnaces: {len(dri_norm)} units")

    # ----- Steel units (BOF, EAF, OHF) -----
    logger.info("Loading GEM Steel unit data...")
    steel_xls = pd.ExcelFile(GEM_STEEL_STEEL_UNITS_FILE)

    if "Basic oxygen furnaces" in steel_xls.sheet_names:
        bof = pd.read_excel(steel_xls, sheet_name="Basic oxygen furnaces")
        bof_norm = pd.DataFrame({
            "gem_plant_id": bof["GEM Plant ID"],
            "gem_unit_id": bof["GEM Unit ID"],
            "unit_name": bof.get("Unit name", bof.get("Unit Name", "")),
            "unit_status": bof["Unit Status"].str.strip().str.lower(),
            "start_year": bof["Start Date"].apply(_parse_date_to_year),
            "pre_retirement_announced_year": bof["Pre-retirement Announcement Date"].apply(_parse_date_to_year),
            "retired_year": bof["Retired Date"].apply(_parse_date_to_year),
            "capacity_ttpa": pd.to_numeric(bof["Current Capacity (ttpa)"], errors="coerce"),
            "unit_type": "bof",
        })
        all_units.append(bof_norm)
        logger.info(f"  Basic oxygen furnaces: {len(bof_norm)} units")

    if "Electric arc furnaces" in steel_xls.sheet_names:
        eaf = pd.read_excel(steel_xls, sheet_name="Electric arc furnaces")
        eaf_norm = pd.DataFrame({
            "gem_plant_id": eaf["GEM Plant ID"],
            "gem_unit_id": eaf["GEM Unit ID"],
            "unit_name": eaf.get("Unit name", eaf.get("Unit Name", "")),
            "unit_status": eaf["Unit Status"].str.strip().str.lower(),
            "start_year": eaf["Start Date"].apply(_parse_date_to_year),
            "pre_retirement_announced_year": eaf["Pre-retirement Announcement Date"].apply(_parse_date_to_year),
            "retired_year": eaf["Retired Date"].apply(_parse_date_to_year),
            "capacity_ttpa": pd.to_numeric(eaf["Current Capacity (ttpa)"], errors="coerce"),
            "unit_type": "eaf",
        })
        all_units.append(eaf_norm)
        logger.info(f"  Electric arc furnaces: {len(eaf_norm)} units")

    if "Open hearth furnaces" in steel_xls.sheet_names:
        ohf = pd.read_excel(steel_xls, sheet_name="Open hearth furnaces")
        ohf_cols = {"GEM Plant ID", "GEM Unit ID", "Unit Status", "Current Capacity (ttpa)"}
        if ohf_cols.issubset(set(ohf.columns)):
            ohf_norm = pd.DataFrame({
                "gem_plant_id": ohf["GEM Plant ID"],
                "gem_unit_id": ohf["GEM Unit ID"],
                "unit_name": ohf.get("Unit name", ohf.get("Unit Name", "")),
                "unit_status": ohf["Unit Status"].str.strip().str.lower(),
                "start_year": ohf.get("Start Date", pd.Series(dtype=float)).apply(_parse_date_to_year),
                "pre_retirement_announced_year": np.nan,
                "retired_year": np.nan,
                "capacity_ttpa": pd.to_numeric(ohf["Current Capacity (ttpa)"], errors="coerce"),
                "unit_type": "ohf",
            })
            all_units.append(ohf_norm)
            logger.info(f"  Open hearth furnaces: {len(ohf_norm)} units")

    if not all_units:
        logger.error("No GEM unit data loaded!")
        return pd.DataFrame()

    combined = pd.concat(all_units, ignore_index=True)

    # --- Join with plant-level data for parent/country ---
    # The "Plant data" sheet has Parent, Country, Plant name etc.
    # Column names are flexible so we match by substring (same approach as apa_calculator)
    logger.info("Joining with plant-level data for parent/country info...")
    plant_data = pd.read_excel(GEM_STEEL_PLANTS_FILE, sheet_name="Plant data")

    # Build flexible column map
    plant_col_map = {}
    desired = {
        "Plant ID": "gem_plant_id",
        "Plant name (English)": "plant_name",
        "Country": "country",
        "Parent": "parent",
    }
    for orig, new in desired.items():
        matches = [c for c in plant_data.columns if orig.lower() in c.lower()]
        if matches:
            plant_col_map[matches[0]] = new

    plant_info = plant_data[list(plant_col_map.keys())].rename(columns=plant_col_map).copy()

    # Deduplicate: one row per plant_id with parent/country
    plant_info_dedup = (
        plant_info.groupby("gem_plant_id")
        .first()
        .reset_index()[["gem_plant_id", "plant_name", "parent", "country"]]
    )

    combined = combined.merge(plant_info_dedup, on="gem_plant_id", how="left")

    # Determine process type for EF calculation
    # BF units → BF-BOF process, EAF units → Scrap-EAF, DRI → DRI-gas
    def _unit_to_process(unit_type: str) -> str:
        mapping = {"bf": "BF-BOF", "bof": "BF-BOF", "eaf": "Scrap-EAF",
                    "dri": "DRI-gas", "ohf": "BF-BOF"}
        return mapping.get(unit_type, "BF-BOF")

    combined["process_type"] = combined["unit_type"].apply(_unit_to_process)

    logger.info(
        f"Combined GEM units: {len(combined)} total, "
        f"{combined['gem_plant_id'].nunique()} plants"
    )

    # Status summary
    status_counts = combined["unit_status"].value_counts()
    for status, count in status_counts.items():
        logger.info(f"  {status}: {count}")

    return combined


# ============================================================================
# Company-Unit Matching
# ============================================================================

def match_units_to_company(units_df: pd.DataFrame, company: str) -> pd.DataFrame:
    """Filter GEM units belonging to a company using COMPANY_GEM_PATTERNS."""
    pattern = COMPANY_GEM_PATTERNS.get(company, company)
    mask = units_df["parent"].str.contains(pattern, case=False, na=False)
    result = units_df[mask].copy()

    if result.empty:
        logger.warning(f"No GEM units found for '{company}' (pattern: {pattern})")

    return result


# ============================================================================
# Closure Inventory
# ============================================================================

def build_closure_inventory(units_df: pd.DataFrame) -> pd.DataFrame:
    """Build a comprehensive closure inventory for all tracked companies.

    Returns one row per unit with closure-relevant information:
        company, gem_plant_id, gem_unit_id, unit_name, unit_type,
        unit_status, capacity_ttpa, country, process_type,
        start_year, pre_retirement_announced_year, retired_year,
        inferred_close_year, close_source
    """
    rows = []

    for company in ALL_COMPANIES:
        company_units = match_units_to_company(units_df, company)
        if company_units.empty:
            continue

        for _, u in company_units.iterrows():
            status = str(u.get("unit_status", "")).lower()

            # Infer closure year
            close_year = np.nan
            close_source = "none"

            if status in ("retired", "mothballed"):
                # Already closed — use retired_year if available
                if pd.notna(u.get("retired_year")):
                    close_year = u["retired_year"]
                    close_source = "gem_retired_date"
                else:
                    # Retired but no date — assume already closed by data vintage
                    close_year = 2024.0
                    close_source = "gem_status_inferred"

            elif "pre-retirement" in status:
                # Announced closure — use retired_year (future) if available
                if pd.notna(u.get("retired_year")):
                    close_year = u["retired_year"]
                    close_source = "gem_planned_retirement"
                else:
                    # Pre-retirement announced but no target date
                    # Default: 5 years from announcement, or 2030 if no announcement date
                    if pd.notna(u.get("pre_retirement_announced_year")):
                        close_year = u["pre_retirement_announced_year"] + 5
                        close_source = "inferred_5yr_from_announcement"
                    else:
                        close_year = 2030.0
                        close_source = "default_2030"

            rows.append({
                "company": company,
                "gem_plant_id": u.get("gem_plant_id"),
                "gem_unit_id": u.get("gem_unit_id"),
                "unit_name": u.get("unit_name"),
                "plant_name": u.get("plant_name"),
                "unit_type": u.get("unit_type"),
                "unit_status": status,
                "capacity_ttpa": u.get("capacity_ttpa"),
                "country": u.get("country"),
                "process_type": u.get("process_type"),
                "start_year": u.get("start_year"),
                "pre_retirement_announced_year": u.get("pre_retirement_announced_year"),
                "retired_year": u.get("retired_year"),
                "inferred_close_year": close_year,
                "close_source": close_source,
            })

    result = pd.DataFrame(rows)
    logger.info(f"Closure inventory: {len(result)} units across {result['company'].nunique()} companies")

    # Summary of closures
    closures = result[result["inferred_close_year"].notna()]
    logger.info(
        f"  Units with closure dates: {len(closures)} "
        f"({len(closures[closures['close_source'] == 'gem_planned_retirement'])} planned, "
        f"{len(closures[closures['close_source'] == 'gem_retired_date'])} already retired)"
    )

    return result


# ============================================================================
# GEM-Derived Closure Trajectory
# ============================================================================

def build_gem_closure_trajectory(
    units_df: pd.DataFrame,
    closure_inventory: pd.DataFrame,
    company: str,
    base_year: int = 2020,
    end_year: int = 2050,
    utilization_rate: float | None = None,
) -> pd.DataFrame:
    """Build year-by-year emissions trajectory assuming closures proceed.

    For each year:
    1. Start with all operating units as of base_year
    2. Remove units that close by that year (based on inferred_close_year)
    3. Add units under construction that come online by that year
    4. Calculate total capacity, apply UR, compute emissions

    Args:
        units_df: Full GEM unit data
        closure_inventory: Closure inventory with inferred_close_year
        company: Canonical company name
        base_year: Start year for trajectory
        end_year: End year for trajectory
        utilization_rate: Fixed UR (if None, use base_year UR from APA)

    Returns:
        DataFrame with: company, year, gem_tp_production_mt, gem_tp_emissions_mt,
                        active_capacity_ttpa, n_active_units
    """
    company_units = match_units_to_company(units_df, company)
    if company_units.empty:
        return pd.DataFrame()

    company_closures = closure_inventory[closure_inventory["company"] == company]

    # Build closure map: gem_unit_id -> inferred_close_year
    close_map = {}
    for _, row in company_closures.iterrows():
        uid = row["gem_unit_id"]
        cy = row["inferred_close_year"]
        if pd.notna(uid) and pd.notna(cy):
            close_map[uid] = cy

    # For each year, determine active units
    rows = []
    for year in range(base_year, end_year + 1):
        active_units = []

        for _, u in company_units.iterrows():
            uid = u["gem_unit_id"]
            status = str(u.get("unit_status", "")).lower()
            start = u.get("start_year", np.nan)

            # Skip units not yet started
            if pd.notna(start) and start > year:
                continue

            # Skip cancelled/announced units (not yet real)
            if status in ("cancelled", "announced"):
                continue

            # Check if this unit has closed by this year
            if uid in close_map and close_map[uid] <= year:
                continue

            # For retired/mothballed without close_map entry: already excluded
            # by default because they have inferred_close_year in inventory
            if status in ("retired", "mothballed") and uid not in close_map:
                # Assume closed before period if no date available
                continue

            active_units.append(u)

        if not active_units:
            rows.append({
                "company": company,
                "year": year,
                "gem_tp_production_mt": 0.0,
                "gem_tp_emissions_mt": 0.0,
                "active_capacity_ttpa": 0.0,
                "n_active_units": 0,
            })
            continue

        active_df = pd.DataFrame(active_units)
        total_capacity_ttpa = active_df["capacity_ttpa"].sum()
        total_capacity_mt = total_capacity_ttpa / 1000.0

        # Use provided UR or estimate from base year
        ur = utilization_rate if utilization_rate else 0.80  # default 80%
        production_mt = total_capacity_mt * ur

        # Calculate emissions per unit (apply year-specific EFs)
        total_emissions = 0.0
        for _, au in active_df.iterrows():
            cap_mt = au["capacity_ttpa"] / 1000.0
            plant_prod = cap_mt * ur
            country = str(au.get("country", ""))
            process = str(au.get("process_type", "BF-BOF"))
            ef = get_plant_ef(country, process, year=year)
            total_emissions += plant_prod * ef

        rows.append({
            "company": company,
            "year": year,
            "gem_tp_production_mt": round(production_mt, 3),
            "gem_tp_emissions_mt": round(total_emissions, 3),
            "active_capacity_ttpa": round(total_capacity_ttpa, 1),
            "n_active_units": len(active_units),
        })

    return pd.DataFrame(rows)


# ============================================================================
# Load Kampmann TP Data
# ============================================================================

def load_kampmann_tp() -> pd.DataFrame:
    """Load Kampmann TP emissions from SteelALD.csv.

    Returns DataFrame with: company, year, kampmann_tp_emissions_mt
    """
    if not KAMPMANN_ALD_FILE.exists():
        logger.error(f"Kampmann ALD not found: {KAMPMANN_ALD_FILE}")
        return pd.DataFrame()

    df = pd.read_csv(KAMPMANN_ALD_FILE)

    # Filter to Emissions (TP) rows
    tp_mask = df["Variable"].str.contains("Emissions.*TP", case=False, na=False)
    tp_data = df[tp_mask].copy()

    if tp_data.empty:
        logger.error("No Emissions (TP) data found in SteelALD.csv")
        return pd.DataFrame()

    # Harmonize company names
    name_map = {
        "POSCO": "POSCO Holdings",
        "thyssenkrupp": "ThyssenKrupp",
        "Thyssenkrupp": "ThyssenKrupp",
    }

    # Aggregate by company + year (Kampmann has per-country rows)
    tp_data["company"] = tp_data["Company Name"].map(
        lambda x: name_map.get(x.strip(), x.strip())
    )
    tp_agg = (
        tp_data.groupby(["company", "Year"])["Value"]
        .sum()
        .reset_index()
        .rename(columns={"Year": "year", "Value": "kampmann_tp_emissions_mt"})
    )

    logger.info(
        f"Kampmann TP: {len(tp_agg)} rows, "
        f"{tp_agg['company'].nunique()} companies, "
        f"years {tp_agg['year'].min()}-{tp_agg['year'].max()}"
    )

    return tp_agg


def load_kampmann_bau() -> pd.DataFrame:
    """Load Kampmann BAU emissions from SteelALD.csv.

    Returns DataFrame with: company, year, kampmann_bau_emissions_mt
    """
    if not KAMPMANN_ALD_FILE.exists():
        return pd.DataFrame()

    df = pd.read_csv(KAMPMANN_ALD_FILE)
    bau_mask = df["Variable"].str.contains("Emissions.*BAU", case=False, na=False)
    bau_data = df[bau_mask].copy()

    if bau_data.empty:
        return pd.DataFrame()

    name_map = {
        "POSCO": "POSCO Holdings",
        "thyssenkrupp": "ThyssenKrupp",
        "Thyssenkrupp": "ThyssenKrupp",
    }

    bau_data["company"] = bau_data["Company Name"].map(
        lambda x: name_map.get(x.strip(), x.strip())
    )
    bau_agg = (
        bau_data.groupby(["company", "Year"])["Value"]
        .sum()
        .reset_index()
        .rename(columns={"Year": "year", "Value": "kampmann_bau_emissions_mt"})
    )

    return bau_agg


# ============================================================================
# Audit: Compare Kampmann TP vs GEM-Derived
# ============================================================================

def run_audit() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run full Kampmann TP audit.

    Returns:
        (audit_df, closure_inventory_df)
    """
    logger.info("=" * 70)
    logger.info("KAMPMANN TP SOURCE AUDIT")
    logger.info("=" * 70)

    # 1. Load data
    kampmann_tp = load_kampmann_tp()
    kampmann_bau = load_kampmann_bau()
    units = load_gem_units()
    closure_inventory = build_closure_inventory(units)

    if kampmann_tp.empty or units.empty:
        logger.error("Cannot proceed without Kampmann TP and GEM unit data")
        return pd.DataFrame(), closure_inventory

    # 2. For each company with Kampmann TP, build GEM trajectory and compare
    audit_rows = []

    for company in KAMPMANN_TP_COMPANIES:
        logger.info(f"\n--- {company} ---")

        company_tp = kampmann_tp[kampmann_tp["company"] == company]
        if company_tp.empty:
            logger.warning(f"  No Kampmann TP data for {company}")
            continue

        company_bau = kampmann_bau[kampmann_bau["company"] == company]

        # Get company's closure units
        company_closures = closure_inventory[closure_inventory["company"] == company]
        pre_ret = company_closures[
            company_closures["unit_status"].str.contains("pre-retirement", na=False)
        ]
        logger.info(
            f"  GEM units: {len(company_closures)} total, "
            f"{len(pre_ret)} pre-retirement"
        )

        if not pre_ret.empty:
            for _, u in pre_ret.iterrows():
                logger.info(
                    f"    {u['unit_name']} ({u['unit_type'].upper()}, "
                    f"{u['plant_name']}, {u['country']}): "
                    f"capacity {u['capacity_ttpa']:.0f} ttpa, "
                    f"close {u['inferred_close_year']:.0f} "
                    f"[{u['close_source']}]"
                )

        # Estimate base year UR from Kampmann data
        # Use Kampmann's BAU 2020 production if available
        ur_estimate = 0.80
        # (Could be refined with actual production data)

        gem_trajectory = build_gem_closure_trajectory(
            units, closure_inventory, company,
            base_year=2020, end_year=2050,
            utilization_rate=ur_estimate,
        )

        if gem_trajectory.empty:
            logger.warning(f"  Could not build GEM trajectory for {company}")
            continue

        # Merge and compare
        comparison = company_tp.merge(
            gem_trajectory[["company", "year", "gem_tp_emissions_mt",
                            "active_capacity_ttpa", "n_active_units"]],
            on=["company", "year"],
            how="left",
        )

        if not company_bau.empty:
            comparison = comparison.merge(
                company_bau[["company", "year", "kampmann_bau_emissions_mt"]],
                on=["company", "year"],
                how="left",
            )

        # Calculate delta
        comparison["delta_mt"] = (
            comparison["gem_tp_emissions_mt"] - comparison["kampmann_tp_emissions_mt"]
        )
        comparison["delta_pct"] = np.where(
            comparison["kampmann_tp_emissions_mt"].abs() > 0.001,
            comparison["delta_mt"] / comparison["kampmann_tp_emissions_mt"] * 100,
            np.nan,
        )

        # Classify divergence
        def classify_divergence(row):
            pct = abs(row.get("delta_pct", 0) or 0)
            if pct < 5:
                return "aligned"
            elif pct < 15:
                return "minor_divergence"
            elif pct < 30:
                return "moderate_divergence"
            else:
                return "major_divergence"

        comparison["divergence_class"] = comparison.apply(classify_divergence, axis=1)

        # Infer likely source of divergence
        def infer_source(row):
            pct = abs(row.get("delta_pct", 0) or 0)
            if pct < 5:
                return "gem_closure_data"
            elif row.get("gem_tp_emissions_mt", 0) > row.get("kampmann_tp_emissions_mt", 0):
                return "kampmann_uses_additional_closures"
            else:
                return "kampmann_uses_fewer_closures_or_different_efs"

        comparison["likely_source"] = comparison.apply(infer_source, axis=1)

        audit_rows.append(comparison)

        # Summary stats
        aligned = (comparison["divergence_class"] == "aligned").sum()
        total = len(comparison)
        logger.info(
            f"  Comparison: {aligned}/{total} years aligned (<5% delta), "
            f"avg delta: {comparison['delta_pct'].mean():.1f}%"
        )

    # 3. Combine audit results
    if audit_rows:
        audit_df = pd.concat(audit_rows, ignore_index=True)
    else:
        audit_df = pd.DataFrame()

    return audit_df, closure_inventory


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Run audit and save outputs."""
    audit_df, closure_inventory = run_audit()

    if not audit_df.empty:
        # Save audit results
        audit_path = OUTPUTS_COMPANY_STEEL / "kampmann_tp_audit.csv"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_df.to_csv(audit_path, index=False, float_format="%.3f")
        logger.info(f"\nSaved audit: {audit_path} ({len(audit_df)} rows)")

        # Print summary per company
        logger.info("\n" + "=" * 70)
        logger.info("AUDIT SUMMARY")
        logger.info("=" * 70)

        for company in KAMPMANN_TP_COMPANIES:
            comp_data = audit_df[audit_df["company"] == company]
            if comp_data.empty:
                continue
            aligned = (comp_data["divergence_class"] == "aligned").sum()
            total = len(comp_data)
            avg_delta = comp_data["delta_pct"].mean()
            max_delta = comp_data["delta_pct"].abs().max()
            logger.info(
                f"  {company:30s} | {aligned:2d}/{total:2d} aligned | "
                f"avg delta: {avg_delta:+6.1f}% | max |delta|: {max_delta:5.1f}%"
            )

    if not closure_inventory.empty:
        # Save closure inventory
        inventory_path = OUTPUTS_COMPANY_STEEL / "kampmann_closure_inventory.csv"
        closure_inventory.to_csv(inventory_path, index=False, float_format="%.1f")
        logger.info(f"Saved closure inventory: {inventory_path} ({len(closure_inventory)} rows)")

        # Summary: pre-retirement units per company
        pre_ret = closure_inventory[
            closure_inventory["unit_status"].str.contains("pre-retirement", na=False)
        ]
        logger.info("\nPre-retirement units by company:")
        for company in ALL_COMPANIES:
            comp = pre_ret[pre_ret["company"] == company]
            if not comp.empty:
                total_cap = comp["capacity_ttpa"].sum()
                years = sorted(comp["inferred_close_year"].dropna().unique())
                years_str = ", ".join(str(int(y)) for y in years)
                logger.info(
                    f"  {company:30s} | {len(comp):3d} units | "
                    f"{total_cap:,.0f} ttpa | close years: {years_str}"
                )


if __name__ == "__main__":
    main()
