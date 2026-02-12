"""
GEM-Based Transition Pathway (TP) Generator — Plant-Level, Country-Specific.

Replicates and extends Kampmann's methodology using GEM GIST unit-level data:
  1. Country-level modelling with country-specific emission factors
  2. Technology switches (BF→DRI/EAF) from GEM construction/announced data
  3. Production growth from new capacity additions

IMPORTANT: Only steel-making units (BOF + EAF) are counted for capacity.
BF and DRI are iron-making inputs that are paired with BOF/EAF respectively.
Counting both BF and BOF would double-count integrated steelworks.

Process / EF assignment:
  - BOF unit  →  BF-BOF emission factor (country-specific, time-varying)
  - EAF unit with DRI at same plant  →  DRI-gas emission factor
  - EAF unit without DRI (scrap-fed) →  Scrap-EAF emission factor (0.40)

For each company and future year:
  - Enumerate all active steel-making units (BOF + EAF)
  - Remove units that close/retire by that year
  - Add new units (construction, announced) that come online
  - Per-unit: capacity × UR × EF(country, process, year) = emissions
  - Sum = company total TP emissions

Uses APA historical production to calibrate the utilization rate per company
at the base year, then holds UR constant (Kampmann assumption).

Usage:
    cd open-asset-data
    python -m pipeline.gem_closure_tp
"""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from .config import (
    OUTPUTS_COMPANY_STEEL,
    PROCESSED_STEEL_DIR,
)
from .kampmann_audit import (
    load_gem_units,
    build_closure_inventory,
    match_units_to_company,
    ALL_COMPANIES,
    KAMPMANN_TP_COMPANIES,
    KAMPMANN_NO_TP,
    load_kampmann_tp,
    load_kampmann_bau,
)
from .apa_calculator import (
    COMPANY_GEM_PATTERNS,
    get_plant_ef,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ============================================================================
# Load APA Historical Data
# ============================================================================

def load_apa_emissions() -> pd.DataFrame:
    """Load APA emissions for all 26 companies.

    Returns DataFrame: company, year, emissions_mt, production_mt
    """
    apa_file = OUTPUTS_COMPANY_STEEL / "steel_apa_emissions.csv"
    if not apa_file.exists():
        logger.error(f"APA emissions file not found: {apa_file}")
        return pd.DataFrame()

    df = pd.read_csv(apa_file)
    logger.info(
        f"APA emissions: {len(df)} rows, "
        f"{df['company'].nunique()} companies, "
        f"years {df['year'].min()}-{df['year'].max()}"
    )
    return df


# ============================================================================
# Unit Filtering: Steel-Making Units Only (BOF + EAF)
# ============================================================================

def filter_steel_making_units(units_df: pd.DataFrame) -> pd.DataFrame:
    """Filter to steel-making units only (BOF + EAF).

    BF and DRI are iron-making inputs — counting them alongside BOF/EAF
    would double-count integrated steelworks capacity.

    For each EAF, checks if there's a DRI unit at the same plant to
    determine the correct process type (DRI-fed vs scrap-fed).
    Uses GEM "Reductant" column on DRI units to distinguish:
      - hydrogen DRI → "DRI-H2" (EF ≈ 0.05)
      - coal DRI → "DRI-coal" (EF ≈ 3.10)
      - methane / gas DRI → "DRI-gas" (EF ≈ 1.05)
    """
    steel_types = {"bof", "eaf"}
    steel_units = units_df[units_df["unit_type"].isin(steel_types)].copy()

    # Build plant → DRI reductant mapping
    # A plant may have multiple DRI units with different reductants.
    # We take the most specific (hydrogen > coal > methane > unknown).
    dri_units = units_df[units_df["unit_type"] == "dri"].copy()

    # Map reductant to process type
    REDUCTANT_MAP = {
        "hydrogen": "DRI-H2",
        "coal": "DRI-coal",
        "methane": "DRI-gas",
        "syngas (reformed methane)": "DRI-gas",
        "waste gas recovery (coke oven gas)": "DRI-gas",
        "unknown": "DRI-gas",  # Default for DRI without reductant info
    }

    # Build plant_name → best DRI process type
    plant_dri_process = {}
    for _, dri_row in dri_units.iterrows():
        plant = str(dri_row.get("plant_name", ""))
        if not plant or plant == "nan":
            continue
        reductant = str(dri_row.get("reductant", "unknown")).lower().strip()
        dri_process = REDUCTANT_MAP.get(reductant, "DRI-gas")

        # Priority: DRI-H2 > DRI-coal > DRI-gas
        current = plant_dri_process.get(plant)
        if current is None:
            plant_dri_process[plant] = dri_process
        elif dri_process == "DRI-H2":
            plant_dri_process[plant] = "DRI-H2"
        elif dri_process == "DRI-coal" and current != "DRI-H2":
            plant_dri_process[plant] = "DRI-coal"

    # Also handle known H2-DRI projects by plant name pattern
    # (some announced units have reductant=unknown but are known H2-DRI)
    H2_DRI_PLANT_PATTERNS = [
        "HYBRIT",
        "H2 Green Steel",
        "Salzgitter Flachstahl.*DRI",  # SALCOS project
    ]
    import re
    for plant_name in plant_dri_process:
        for pattern in H2_DRI_PLANT_PATTERNS:
            if re.search(pattern, plant_name, re.IGNORECASE):
                plant_dri_process[plant_name] = "DRI-H2"

    # Also check plant names that have DRI units but aren't in plant_dri_process yet
    for _, dri_row in dri_units.iterrows():
        plant = str(dri_row.get("plant_name", ""))
        if plant in plant_dri_process:
            continue
        for pattern in H2_DRI_PLANT_PATTERNS:
            if re.search(pattern, plant, re.IGNORECASE):
                plant_dri_process[plant] = "DRI-H2"

    n_h2_plants = sum(1 for v in plant_dri_process.values() if v == "DRI-H2")
    n_coal_plants = sum(1 for v in plant_dri_process.values() if v == "DRI-coal")
    n_gas_plants = sum(1 for v in plant_dri_process.values() if v == "DRI-gas")
    logger.info(
        f"DRI plant mapping: {len(plant_dri_process)} plants with DRI "
        f"(H2: {n_h2_plants}, coal: {n_coal_plants}, gas: {n_gas_plants})"
    )

    def assign_process(row):
        """Assign process type and EF category based on unit type."""
        unit_type = str(row.get("unit_type", "")).lower()
        if unit_type == "bof":
            return "BF-BOF"
        elif unit_type == "eaf":
            plant = str(row.get("plant_name", ""))
            if plant in plant_dri_process:
                return plant_dri_process[plant]
            return "Scrap-EAF"
        return "BF-BOF"  # fallback

    steel_units["process_type"] = steel_units.apply(assign_process, axis=1)

    logger.info(
        f"Steel-making units: {len(steel_units)} "
        f"(BOF: {(steel_units['unit_type'] == 'bof').sum()}, "
        f"EAF: {(steel_units['unit_type'] == 'eaf').sum()}) "
        f"from {len(units_df)} total units"
    )
    for pt in ["BF-BOF", "Scrap-EAF", "DRI-gas", "DRI-coal", "DRI-H2"]:
        n = (steel_units["process_type"] == pt).sum()
        if n > 0:
            logger.info(f"  {pt}: {n} units")

    return steel_units


# ============================================================================
# Unit-Level TP Engine (Kampmann Replication)
# ============================================================================

def _infer_close_year(unit_row: pd.Series) -> float:
    """Infer when a unit closes based on GEM status and dates."""
    status = str(unit_row.get("unit_status", "")).lower()

    if status in ("retired", "mothballed"):
        if pd.notna(unit_row.get("retired_year")):
            return unit_row["retired_year"]
        # Already closed, no date — assume closed before analysis period
        return 2023.0

    if "pre-retirement" in status:
        if pd.notna(unit_row.get("retired_year")):
            return unit_row["retired_year"]
        if pd.notna(unit_row.get("pre_retirement_announced_year")):
            return unit_row["pre_retirement_announced_year"] + 5
        return 2030.0  # Default for announced with no date

    return np.nan  # Operating / construction / announced — no closure


def _unit_is_active(unit_row: pd.Series, year: int, close_year: float) -> bool:
    """Determine if a unit is active in a given year.

    Rules:
    - Must have started by this year
    - Must not have closed by this year
    - Announced units are included (Kampmann includes planned capacity)
    - Cancelled units are excluded
    """
    status = str(unit_row.get("unit_status", "")).lower()

    if status == "cancelled":
        return False

    start = unit_row.get("start_year")
    if pd.notna(start) and start > year:
        return False

    if pd.notna(close_year) and close_year <= year:
        return False

    return True


def build_plant_level_tp(
    steel_units: pd.DataFrame,
    company: str,
    base_year: int = 2024,
    end_year: int = 2050,
    base_ur: float = 0.80,
    skip_closures: bool = False,
) -> pd.DataFrame:
    """Build plant-level, country-specific TP trajectory for one company.

    Only uses steel-making units (BOF + EAF) — no BF or DRI.

    For each year from base_year to end_year:
    1. Determine which steel-making units are active (started, not yet closed)
    2. For each active unit: emissions = (capacity_ttpa / 1000) * UR * EF(country, process, year)
    3. Sum by country and across all units

    Args:
        steel_units: GEM unit data filtered to BOF+EAF only
        company: Canonical company name
        base_year: Start year (use latest APA data year)
        end_year: End year for projections
        base_ur: Utilization rate (calibrated from APA production / GEM capacity)
        skip_closures: If True, no units ever close (BAU mode). New capacity
            from construction/announced units still comes online per start_year.

    Returns:
        DataFrame with: company, year, country, process_type, unit_type,
                        n_units, capacity_ttpa, production_mt, emissions_mt
    """
    company_units = match_units_to_company(steel_units, company)
    if company_units.empty:
        return pd.DataFrame()

    # Pre-compute close years for all units
    company_units = company_units.copy()
    company_units["close_year"] = company_units.apply(_infer_close_year, axis=1)

    # BAU mode: no units ever close (but new capacity still comes online)
    if skip_closures:
        company_units["close_year"] = np.nan

    rows = []
    for year in range(base_year, end_year + 1):
        # Determine active units for this year
        for _, u in company_units.iterrows():
            if not _unit_is_active(u, year, u["close_year"]):
                continue

            cap_ttpa = u.get("capacity_ttpa", 0)
            if pd.isna(cap_ttpa) or cap_ttpa <= 0:
                continue

            country = str(u.get("country", ""))
            process = str(u.get("process_type", "BF-BOF"))
            unit_type = str(u.get("unit_type", ""))

            # Production and emissions for this unit
            cap_mt = cap_ttpa / 1000.0
            prod_mt = cap_mt * base_ur
            ef = get_plant_ef(country, process, year=year)
            em_mt = prod_mt * ef

            rows.append({
                "company": company,
                "year": year,
                "country": country,
                "process_type": process,
                "unit_type": unit_type,
                "gem_unit_id": u.get("gem_unit_id", ""),
                "unit_name": u.get("unit_name", ""),
                "plant_name": u.get("plant_name", ""),
                "unit_status": u.get("unit_status", ""),
                "capacity_ttpa": cap_ttpa,
                "production_mt": round(prod_mt, 4),
                "emissions_mt": round(em_mt, 4),
            })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def generate_company_tp(
    steel_units: pd.DataFrame,
    company: str,
    apa_df: pd.DataFrame,
    base_year: int = 2024,
    end_year: int = 2050,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate TP for one company: calibrate UR from APA, build plant-level TP.

    Returns:
        (company_annual_df, plant_detail_df)

        company_annual_df: company, year, tp_emissions_mt, tp_production_mt,
                           n_active_units, active_capacity_ttpa
        plant_detail_df: full unit-level detail per year
    """
    company_units = match_units_to_company(steel_units, company)
    if company_units.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Calibrate UR from APA production at base year
    company_apa = apa_df[
        (apa_df["company"] == company) & (apa_df["year"] == base_year)
    ]
    if company_apa.empty:
        # Try latest available year
        company_apa_all = apa_df[apa_df["company"] == company].sort_values("year")
        if company_apa_all.empty:
            logger.warning(f"  {company}: No APA data")
            return pd.DataFrame(), pd.DataFrame()
        company_apa = company_apa_all.iloc[[-1]]
        actual_base_year = int(company_apa.iloc[0]["year"])
        logger.debug(f"  {company}: Using APA year {actual_base_year} instead of {base_year}")
    else:
        actual_base_year = base_year

    apa_production = company_apa.iloc[0].get("production_mt", np.nan)

    # Calculate base year capacity from active steel-making units
    status_active = {"operating", "operating pre-retirement"}
    active_mask = (
        company_units["unit_status"].isin(status_active) &
        (company_units["start_year"].fillna(2000) <= actual_base_year)
    )
    base_active = company_units[active_mask]
    base_capacity_mt = base_active["capacity_ttpa"].sum() / 1000.0

    if base_capacity_mt > 0 and pd.notna(apa_production) and apa_production > 0:
        calibrated_ur = apa_production / base_capacity_mt
        # Cap at reasonable range
        calibrated_ur = max(0.40, min(calibrated_ur, 1.0))
    else:
        calibrated_ur = 0.80

    # Build plant-level TP
    detail = build_plant_level_tp(
        steel_units, company,
        base_year=base_year, end_year=end_year,
        base_ur=calibrated_ur,
    )

    if detail.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Aggregate to company-year level
    annual = (
        detail.groupby(["company", "year"])
        .agg(
            tp_emissions_raw=("emissions_mt", "sum"),
            tp_production_raw=("production_mt", "sum"),
            n_active_units=("emissions_mt", "count"),
            active_capacity_ttpa=("capacity_ttpa", "sum"),
        )
        .reset_index()
    )
    annual["calibrated_ur"] = calibrated_ur

    # --- APA Anchoring ---
    # Scale the GEM-derived trajectory so base year matches APA actual emissions.
    # Future years use relative change: TP(year) = APA_base × (GEM(year) / GEM(base))
    # This removes entity-boundary and UR-calibration mismatches.
    apa_emissions = company_apa.iloc[0].get("emissions_mt", np.nan)
    gem_base = annual.loc[annual["year"] == base_year, "tp_emissions_raw"].values
    gem_base_em = gem_base[0] if len(gem_base) > 0 and gem_base[0] > 0 else np.nan

    if pd.notna(apa_emissions) and apa_emissions > 0 and pd.notna(gem_base_em) and gem_base_em > 0:
        scale_factor = apa_emissions / gem_base_em
        annual["tp_emissions_mt"] = (annual["tp_emissions_raw"] * scale_factor).round(3)
        annual["tp_production_mt"] = (annual["tp_production_raw"] * scale_factor).round(3)
        annual["apa_anchored"] = True
    else:
        annual["tp_emissions_mt"] = annual["tp_emissions_raw"].round(3)
        annual["tp_production_mt"] = annual["tp_production_raw"].round(3)
        annual["apa_anchored"] = False
        scale_factor = 1.0

    annual["scale_factor"] = round(scale_factor, 4)

    # Also aggregate by country for validation
    by_country = (
        detail.groupby(["company", "year", "country"])
        .agg(
            emissions_mt=("emissions_mt", "sum"),
            production_mt=("production_mt", "sum"),
            capacity_ttpa=("capacity_ttpa", "sum"),
            n_units=("emissions_mt", "count"),
        )
        .reset_index()
    )

    return annual, detail


# ============================================================================
# Generate TP for All 26 Companies
# ============================================================================

def generate_all_tp(
    steel_units: pd.DataFrame,
    apa_df: pd.DataFrame,
    base_year: int = 2024,
    end_year: int = 2050,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate TP for all 26 companies.

    Returns:
        (all_annual_df, all_detail_df)
    """
    all_annual = []
    all_detail = []

    for company in ALL_COMPANIES:
        annual, detail = generate_company_tp(
            steel_units, company, apa_df,
            base_year=base_year, end_year=end_year,
        )

        if annual.empty:
            # Fallback: use APA latest-year constant as TP (no GEM data)
            company_apa = apa_df[apa_df["company"] == company].sort_values("year")
            if not company_apa.empty:
                latest = company_apa.iloc[-1]
                for year in range(base_year, end_year + 1):
                    all_annual.append({
                        "company": company,
                        "year": year,
                        "tp_emissions_mt": round(latest["emissions_mt"], 3),
                        "tp_production_mt": round(latest.get("production_mt", np.nan), 3)
                        if pd.notna(latest.get("production_mt")) else np.nan,
                        "n_active_units": 0,
                        "active_capacity_ttpa": 0,
                        "calibrated_ur": np.nan,
                        "source": "apa_constant_fallback",
                    })
                logger.info(
                    f"  {company:30s} | NO GEM UNITS → APA constant fallback | "
                    f"TP={latest['emissions_mt']:.1f} Mt/yr"
                )
            else:
                logger.warning(f"  {company}: No GEM or APA data — skipping entirely")
            continue

        annual["source"] = "gem_plant_level"
        all_annual.append(annual)
        all_detail.append(detail)

        # Log summary
        base_em = annual[annual["year"] == base_year]["tp_emissions_mt"].values
        final_em = annual[annual["year"] == end_year]["tp_emissions_mt"].values
        ur = annual["calibrated_ur"].iloc[0]
        base_str = f"{base_em[0]:.1f}" if len(base_em) > 0 else "?"
        final_str = f"{final_em[0]:.1f}" if len(final_em) > 0 else "?"

        # Count technology changes
        n_closing = detail[
            (detail["year"] == base_year) &
            (detail["unit_status"].str.contains("pre-retirement", na=False))
        ]["gem_unit_id"].nunique()
        n_new = detail[
            (detail["year"] == end_year) &
            (detail["unit_status"].isin(["construction", "announced"]))
        ]["gem_unit_id"].nunique()

        # Count by process type at base and end
        base_detail = detail[detail["year"] == base_year]
        end_detail = detail[detail["year"] == end_year]
        base_bof = base_detail[base_detail["process_type"] == "BF-BOF"]["capacity_ttpa"].sum()
        base_eaf = base_detail[base_detail["process_type"].isin(["Scrap-EAF", "DRI-gas"])]["capacity_ttpa"].sum()
        end_bof = end_detail[end_detail["process_type"] == "BF-BOF"]["capacity_ttpa"].sum()
        end_eaf = end_detail[end_detail["process_type"].isin(["Scrap-EAF", "DRI-gas"])]["capacity_ttpa"].sum()

        logger.info(
            f"  {company:30s} | UR={ur:.2f} | "
            f"{base_year}={base_str} Mt → {end_year}={final_str} Mt | "
            f"{n_closing} closing, {n_new} new | "
            f"BOF: {base_bof/1000:.0f}→{end_bof/1000:.0f} Mt, "
            f"EAF: {base_eaf/1000:.0f}→{end_eaf/1000:.0f} Mt"
        )

    # Combine
    if all_annual:
        annual_list = []
        for item in all_annual:
            if isinstance(item, pd.DataFrame):
                annual_list.append(item)
            elif isinstance(item, dict):
                annual_list.append(pd.DataFrame([item]))
        annual_df = pd.concat(annual_list, ignore_index=True)
    else:
        annual_df = pd.DataFrame()

    detail_df = pd.concat(all_detail, ignore_index=True) if all_detail else pd.DataFrame()

    logger.info(
        f"\nTP generated: {len(annual_df)} company-year rows, "
        f"{annual_df['company'].nunique()} companies"
    )
    return annual_df, detail_df


# ============================================================================
# Generate BAU for All 26 Companies (No Closures, New Capacity Comes Online)
# ============================================================================

def generate_company_bau(
    steel_units: pd.DataFrame,
    company: str,
    apa_df: pd.DataFrame,
    base_year: int = 2024,
    end_year: int = 2050,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate BAU for one company: same as TP but no units close.

    Closures are disabled so pre-retirement units keep running.
    New capacity (construction/announced) still comes online per start_year.
    APA anchoring ensures base year matches actual emissions.

    Returns:
        (company_annual_df, plant_detail_df)
    """
    company_units = match_units_to_company(steel_units, company)
    if company_units.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Calibrate UR from APA production at base year (same as TP)
    company_apa = apa_df[
        (apa_df["company"] == company) & (apa_df["year"] == base_year)
    ]
    if company_apa.empty:
        company_apa_all = apa_df[apa_df["company"] == company].sort_values("year")
        if company_apa_all.empty:
            logger.warning(f"  {company}: No APA data")
            return pd.DataFrame(), pd.DataFrame()
        company_apa = company_apa_all.iloc[[-1]]
        actual_base_year = int(company_apa.iloc[0]["year"])
    else:
        actual_base_year = base_year

    apa_production = company_apa.iloc[0].get("production_mt", np.nan)

    # Calculate base year capacity from active steel-making units
    status_active = {"operating", "operating pre-retirement"}
    active_mask = (
        company_units["unit_status"].isin(status_active) &
        (company_units["start_year"].fillna(2000) <= actual_base_year)
    )
    base_active = company_units[active_mask]
    base_capacity_mt = base_active["capacity_ttpa"].sum() / 1000.0

    if base_capacity_mt > 0 and pd.notna(apa_production) and apa_production > 0:
        calibrated_ur = apa_production / base_capacity_mt
        calibrated_ur = max(0.40, min(calibrated_ur, 1.0))
    else:
        calibrated_ur = 0.80

    # Build plant-level BAU (skip_closures=True: no units ever close)
    detail = build_plant_level_tp(
        steel_units, company,
        base_year=base_year, end_year=end_year,
        base_ur=calibrated_ur,
        skip_closures=True,
    )

    if detail.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Aggregate to company-year level
    annual = (
        detail.groupby(["company", "year"])
        .agg(
            bau_emissions_raw=("emissions_mt", "sum"),
            bau_production_raw=("production_mt", "sum"),
            n_active_units=("emissions_mt", "count"),
            active_capacity_ttpa=("capacity_ttpa", "sum"),
        )
        .reset_index()
    )
    annual["calibrated_ur"] = calibrated_ur

    # --- APA Anchoring (same as TP) ---
    apa_emissions = company_apa.iloc[0].get("emissions_mt", np.nan)
    gem_base = annual.loc[annual["year"] == base_year, "bau_emissions_raw"].values
    gem_base_em = gem_base[0] if len(gem_base) > 0 and gem_base[0] > 0 else np.nan

    if pd.notna(apa_emissions) and apa_emissions > 0 and pd.notna(gem_base_em) and gem_base_em > 0:
        scale_factor = apa_emissions / gem_base_em
        annual["bau_emissions_mt"] = (annual["bau_emissions_raw"] * scale_factor).round(3)
        annual["bau_production_mt"] = (annual["bau_production_raw"] * scale_factor).round(3)
        annual["apa_anchored"] = True
    else:
        annual["bau_emissions_mt"] = annual["bau_emissions_raw"].round(3)
        annual["bau_production_mt"] = annual["bau_production_raw"].round(3)
        annual["apa_anchored"] = False
        scale_factor = 1.0

    annual["scale_factor"] = round(scale_factor, 4)

    return annual, detail


def generate_all_bau(
    steel_units: pd.DataFrame,
    apa_df: pd.DataFrame,
    base_year: int = 2024,
    end_year: int = 2050,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate BAU for all 26 companies (no closures, new capacity online).

    Returns:
        (all_annual_df, all_detail_df)
    """
    all_annual = []
    all_detail = []

    for company in ALL_COMPANIES:
        annual, detail = generate_company_bau(
            steel_units, company, apa_df,
            base_year=base_year, end_year=end_year,
        )

        if annual.empty:
            # Fallback: use APA latest-year constant as BAU (no GEM data)
            company_apa = apa_df[apa_df["company"] == company].sort_values("year")
            if not company_apa.empty:
                latest = company_apa.iloc[-1]
                for year in range(base_year, end_year + 1):
                    all_annual.append({
                        "company": company,
                        "year": year,
                        "bau_emissions_mt": round(latest["emissions_mt"], 3),
                        "bau_production_mt": round(latest.get("production_mt", np.nan), 3)
                        if pd.notna(latest.get("production_mt")) else np.nan,
                        "n_active_units": 0,
                        "active_capacity_ttpa": 0,
                        "calibrated_ur": np.nan,
                        "source": "apa_constant_fallback",
                    })
                logger.info(
                    f"  {company:30s} | NO GEM UNITS → APA constant fallback | "
                    f"BAU={latest['emissions_mt']:.1f} Mt/yr"
                )
            else:
                logger.warning(f"  {company}: No GEM or APA data — skipping entirely")
            continue

        annual["source"] = "gem_plant_level_bau"
        all_annual.append(annual)
        all_detail.append(detail)

        # Log summary
        base_em = annual[annual["year"] == base_year]["bau_emissions_mt"].values
        final_em = annual[annual["year"] == end_year]["bau_emissions_mt"].values
        ur = annual["calibrated_ur"].iloc[0]
        base_str = f"{base_em[0]:.1f}" if len(base_em) > 0 else "?"
        final_str = f"{final_em[0]:.1f}" if len(final_em) > 0 else "?"

        # Count new units coming online
        n_new = detail[
            (detail["year"] == end_year) &
            (detail["unit_status"].isin(["construction", "announced"]))
        ]["gem_unit_id"].nunique()

        logger.info(
            f"  {company:30s} | UR={ur:.2f} | "
            f"{base_year}={base_str} Mt → {end_year}={final_str} Mt | "
            f"{n_new} new units | "
            f"delta={((float(final_str) / float(base_str) - 1) * 100) if base_str != '?' and final_str != '?' else '?':+.1f}%"
        )

    # Combine
    if all_annual:
        annual_list = []
        for item in all_annual:
            if isinstance(item, pd.DataFrame):
                annual_list.append(item)
            elif isinstance(item, dict):
                annual_list.append(pd.DataFrame([item]))
        annual_df = pd.concat(annual_list, ignore_index=True)
    else:
        annual_df = pd.DataFrame()

    detail_df = pd.concat(all_detail, ignore_index=True) if all_detail else pd.DataFrame()

    logger.info(
        f"\nBAU generated: {len(annual_df)} company-year rows, "
        f"{annual_df['company'].nunique()} companies"
    )
    return annual_df, detail_df


# ============================================================================
# Validate Against Kampmann
# ============================================================================

def validate_against_kampmann(
    tp_df: pd.DataFrame,
    kampmann_tp: pd.DataFrame,
) -> pd.DataFrame:
    """Compare GEM plant-level TP with Kampmann TP for the 10 companies."""
    validation_rows = []

    for company in KAMPMANN_TP_COMPANIES:
        comp_tp = tp_df[tp_df["company"] == company]
        comp_kamp = kampmann_tp[kampmann_tp["company"] == company]

        if comp_tp.empty or comp_kamp.empty:
            continue

        merged = comp_tp[["company", "year", "tp_emissions_mt", "calibrated_ur"]].merge(
            comp_kamp[["company", "year", "kampmann_tp_emissions_mt"]],
            on=["company", "year"],
            how="inner",
        )

        merged["delta_mt"] = merged["tp_emissions_mt"] - merged["kampmann_tp_emissions_mt"]
        merged["delta_pct"] = np.where(
            merged["kampmann_tp_emissions_mt"].abs() > 0.001,
            merged["delta_mt"] / merged["kampmann_tp_emissions_mt"] * 100,
            np.nan,
        )

        validation_rows.append(merged)

    if validation_rows:
        result = pd.concat(validation_rows, ignore_index=True)
        logger.info(f"\nValidation against Kampmann: {len(result)} comparison points")

        for company in KAMPMANN_TP_COMPANIES:
            comp = result[result["company"] == company]
            if not comp.empty:
                avg = comp["delta_pct"].mean()
                aligned = (comp["delta_pct"].abs() < 10).sum()
                close = (comp["delta_pct"].abs() < 25).sum()
                ur = comp["calibrated_ur"].iloc[0]
                base = comp[comp["year"] == 2024]
                end = comp[comp["year"] == 2050]
                base_gem = base["tp_emissions_mt"].values[0] if len(base) else "?"
                base_kamp = base["kampmann_tp_emissions_mt"].values[0] if len(base) else "?"
                end_gem = end["tp_emissions_mt"].values[0] if len(end) else "?"
                end_kamp = end["kampmann_tp_emissions_mt"].values[0] if len(end) else "?"
                logger.info(
                    f"  {company:30s} | UR={ur:.2f} | "
                    f"{aligned:2d}/{len(comp)} <10%, {close:2d}/{len(comp)} <25% | "
                    f"avg: {avg:+.1f}% | "
                    f"2024: {base_gem:.1f} vs {base_kamp:.1f}, "
                    f"2050: {end_gem:.1f} vs {end_kamp:.1f}"
                )

        return result

    return pd.DataFrame()


def validate_bau_against_kampmann(
    bau_df: pd.DataFrame,
    kampmann_bau: pd.DataFrame,
) -> pd.DataFrame:
    """Compare GEM plant-level BAU with Kampmann BAU for overlapping companies.

    Kampmann BAU is country-level, so we sum across countries first.
    """
    # Kampmann BAU may have country-level rows; aggregate to company-year
    kampmann_agg = (
        kampmann_bau.groupby(["company", "year"])
        .agg(kampmann_bau_emissions_mt=("kampmann_bau_emissions_mt", "sum"))
        .reset_index()
    )

    validation_rows = []
    kampmann_companies = kampmann_agg["company"].unique()

    for company in kampmann_companies:
        comp_bau = bau_df[bau_df["company"] == company]
        comp_kamp = kampmann_agg[kampmann_agg["company"] == company]

        if comp_bau.empty or comp_kamp.empty:
            continue

        merged = comp_bau[["company", "year", "bau_emissions_mt"]].merge(
            comp_kamp[["company", "year", "kampmann_bau_emissions_mt"]],
            on=["company", "year"],
            how="inner",
        )

        merged["delta_mt"] = merged["bau_emissions_mt"] - merged["kampmann_bau_emissions_mt"]
        merged["delta_pct"] = np.where(
            merged["kampmann_bau_emissions_mt"].abs() > 0.001,
            merged["delta_mt"] / merged["kampmann_bau_emissions_mt"] * 100,
            np.nan,
        )

        validation_rows.append(merged)

    if validation_rows:
        result = pd.concat(validation_rows, ignore_index=True)
        logger.info(f"BAU validation: {len(result)} comparison points, "
                    f"{result['company'].nunique()} companies")

        for company in sorted(result["company"].unique()):
            comp = result[result["company"] == company]
            if not comp.empty:
                avg = comp["delta_pct"].mean()
                base = comp[comp["year"] == 2024]
                end = comp[comp["year"] == 2050]
                base_gem = f"{base['bau_emissions_mt'].values[0]:.1f}" if len(base) else "?"
                base_kamp = f"{base['kampmann_bau_emissions_mt'].values[0]:.1f}" if len(base) else "?"
                end_gem = f"{end['bau_emissions_mt'].values[0]:.1f}" if len(end) else "?"
                end_kamp = f"{end['kampmann_bau_emissions_mt'].values[0]:.1f}" if len(end) else "?"
                logger.info(
                    f"  {company:30s} | avg: {avg:+.1f}% | "
                    f"2024: {base_gem} vs {base_kamp}, "
                    f"2050: {end_gem} vs {end_kamp}"
                )

        return result

    return pd.DataFrame()


# ============================================================================
# Main
# ============================================================================

def main():
    logger.info("=" * 70)
    logger.info("GEM PLANT-LEVEL TP GENERATOR v2 (Steel-Making Units Only)")
    logger.info("=" * 70)

    # 1. Load data
    apa = load_apa_emissions()
    units = load_gem_units()

    if apa.empty or units.empty:
        logger.error("Missing required data")
        return

    # 2. Filter to steel-making units only (BOF + EAF)
    steel_units = filter_steel_making_units(units)

    # 3. Generate plant-level TP
    logger.info("\nGenerating plant-level TP trajectories...")
    annual_df, detail_df = generate_all_tp(steel_units, apa, base_year=2024, end_year=2050)

    # 3b. Generate plant-level BAU (no closures, new capacity still comes online)
    logger.info("\n" + "=" * 70)
    logger.info("GEM PLANT-LEVEL BAU (No Closures, New Capacity Online)")
    logger.info("=" * 70)
    bau_annual_df, bau_detail_df = generate_all_bau(
        steel_units, apa, base_year=2024, end_year=2050
    )

    # 4. Validate TP against Kampmann
    kampmann_tp = load_kampmann_tp()
    if not kampmann_tp.empty and not annual_df.empty:
        validation = validate_against_kampmann(annual_df, kampmann_tp)
        if not validation.empty:
            val_path = OUTPUTS_COMPANY_STEEL / "gem_tp_validation.csv"
            validation.to_csv(val_path, index=False, float_format="%.3f")
            logger.info(f"Saved validation: {val_path}")

    # 4b. Validate BAU against Kampmann BAU
    kampmann_bau = load_kampmann_bau()
    if not kampmann_bau.empty and not bau_annual_df.empty:
        logger.info("\n--- BAU Validation: GEM BAU vs Kampmann BAU ---")
        bau_validation = validate_bau_against_kampmann(bau_annual_df, kampmann_bau)
        if not bau_validation.empty:
            val_path = OUTPUTS_COMPANY_STEEL / "gem_bau_validation.csv"
            bau_validation.to_csv(val_path, index=False, float_format="%.3f")
            logger.info(f"Saved BAU validation: {val_path}")

    # 5. Save TP outputs
    if not annual_df.empty:
        tp_path = OUTPUTS_COMPANY_STEEL / "steel_gem_tp_annual.csv"
        annual_df.to_csv(tp_path, index=False, float_format="%.3f")
        logger.info(f"\nSaved annual TP: {tp_path} ({len(annual_df)} rows)")

    if not detail_df.empty:
        detail_path = OUTPUTS_COMPANY_STEEL / "steel_gem_tp_detail.csv"
        detail_df.to_csv(detail_path, index=False, float_format="%.4f")
        logger.info(f"Saved unit-level detail: {detail_path} ({len(detail_df)} rows)")

    # 5b. Save BAU outputs
    if not bau_annual_df.empty:
        bau_path = OUTPUTS_COMPANY_STEEL / "steel_gem_bau_annual.csv"
        bau_annual_df.to_csv(bau_path, index=False, float_format="%.3f")
        logger.info(f"Saved annual BAU: {bau_path} ({len(bau_annual_df)} rows)")

    if not bau_detail_df.empty:
        bau_detail_path = OUTPUTS_COMPANY_STEEL / "steel_gem_bau_detail.csv"
        bau_detail_df.to_csv(bau_detail_path, index=False, float_format="%.4f")
        logger.info(f"Saved BAU unit-level detail: {bau_detail_path} ({len(bau_detail_df)} rows)")

    # 6. Summary
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    if not annual_df.empty:
        gem = annual_df[annual_df["source"] == "gem_plant_level"]
        fallback = annual_df[annual_df["source"] == "apa_constant_fallback"]
        logger.info(f"  TP - Companies with plant-level: {gem['company'].nunique()}")
        logger.info(f"  TP - Companies with APA fallback: {fallback['company'].nunique()}")
        logger.info(f"  TP - Total companies: {annual_df['company'].nunique()}")

    if not bau_annual_df.empty:
        gem_bau = bau_annual_df[bau_annual_df["source"] == "gem_plant_level_bau"]
        fallback_bau = bau_annual_df[bau_annual_df["source"] == "apa_constant_fallback"]
        logger.info(f"  BAU - Companies with plant-level: {gem_bau['company'].nunique()}")
        logger.info(f"  BAU - Companies with APA fallback: {fallback_bau['company'].nunique()}")
        logger.info(f"  BAU - Total companies: {bau_annual_df['company'].nunique()}")

        # Show BAU trajectory highlights (companies with significant capacity additions)
        logger.info("\n  BAU trajectory highlights (2024 → 2050):")
        for company in sorted(bau_annual_df["company"].unique()):
            base_row = bau_annual_df[
                (bau_annual_df["company"] == company) & (bau_annual_df["year"] == 2024)
            ]
            end_row = bau_annual_df[
                (bau_annual_df["company"] == company) & (bau_annual_df["year"] == 2050)
            ]
            if base_row.empty or end_row.empty:
                continue
            base_em = base_row["bau_emissions_mt"].values[0]
            end_em = end_row["bau_emissions_mt"].values[0]
            if base_em > 0:
                pct = (end_em / base_em - 1) * 100
                if abs(pct) > 1:  # Only show companies with >1% change
                    logger.info(
                        f"    {company:30s} | {base_em:7.1f} → {end_em:7.1f} Mt | "
                        f"{pct:+.1f}%"
                    )


if __name__ == "__main__":
    main()
