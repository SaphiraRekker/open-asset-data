"""
Configuration and path management for open-asset-data pipeline.
All paths are relative to the project root (open-asset-data/).
"""

from pathlib import Path

# Project root: one level up from pipeline/
_THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _THIS_DIR.parent  # pipeline/ -> open-asset-data/

# ── Data directories ──────────────────────────────────────────────────────────

# Raw company-level input data
COMPANY_DATA_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DATA_DIR = COMPANY_DATA_DIR  # Backwards compatibility alias

# Processed data (pipeline outputs consumed by R)
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_STEEL_DIR = PROCESSED_DATA_DIR / "steel"
PROCESSED_POWER_DIR = PROCESSED_DATA_DIR / "power"
PROCESSED_COUNTRY_DIR = PROCESSED_DATA_DIR / "country"

# Historical data (observed emissions, production)
ANNUAL_REPORTS_DIR = COMPANY_DATA_DIR / "AnnualReports"
CLIMATE_TRACE_DIR = COMPANY_DATA_DIR / "ClimateTrace"

# Forward-looking data (targets, commitments)
FORWARD_LOOKING_DIR = COMPANY_DATA_DIR / "ForwardLooking"
OXFORD_NZT_DIR = FORWARD_LOOKING_DIR / "oxfordNZtracker"

# Asset-level databases (contain both historical capacity and projected closures)
KAMPMANN_ALD_DIR = COMPANY_DATA_DIR / "KampmannALD"
GEM_DIR = COMPANY_DATA_DIR / "GEM"
TPI_DIR = COMPANY_DATA_DIR / "TPI"

# AU-NGER (Australian power sector)
AU_NGER_DIR = COMPANY_DATA_DIR / "AU_NGER"

# InfluenceMap
INFLUENCEMAP_DIR = COMPANY_DATA_DIR / "InfluenceMap"

# SBTi
SBTI_DIR = FORWARD_LOOKING_DIR / "SBTi"

# ── Output directories ────────────────────────────────────────────────────────

# Company data outputs (pre-analysis)
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
OUTPUTS_COMPANY_STEEL = OUTPUTS_DIR / "steel"
OUTPUTS_COMPANY_CEMENT = OUTPUTS_DIR / "cement"
OUTPUTS_COMPANY_POWER = OUTPUTS_DIR / "power"
OUTPUTS_COMPANY_ALUMINIUM = OUTPUTS_DIR / "aluminium"
OUTPUTS_COMPANY_FOSSIL = OUTPUTS_DIR / "fossil_fuels"
OUTPUTS_CROSS_SECTOR = OUTPUTS_DIR / "cross_sector"

# ── Registry and manifest files ──────────────────────────────────────────────

REPORT_URLS_FILE = _THIS_DIR / "report_urls.json"
DOWNLOAD_MANIFEST_FILE = ANNUAL_REPORTS_DIR / "download_manifest.json"

# ── Specific data files ──────────────────────────────────────────────────────

# Curated country-level production (hand-verified from annual reports)
CURATED_COUNTRY_PRODUCTION_FILE = PROCESSED_STEEL_DIR / "steel_country_production_curated.csv"

# Ownership mapping
OWNERSHIP_MAPPING_FILE = OUTPUTS_COMPANY_STEEL / "steel_ownership_mapping.csv"
OWNERSHIP_MISMATCHES_FILE = OUTPUTS_COMPANY_STEEL / "steel_ownership_mismatches.csv"

# Extracted data output files
EXTRACTED_PRODUCTION_FILE = PROCESSED_STEEL_DIR / "steel_production_extracted.csv"
EXTRACTED_EMISSIONS_FILE = PROCESSED_STEEL_DIR / "steel_emissions_extracted.csv"
EXTRACTION_REPORT_FILE = OUTPUTS_CROSS_SECTOR / "extraction_quality_report.csv"

# Existing data sources (for validation)
KAMPMANN_ALD_FILE = KAMPMANN_ALD_DIR / "SteelALD.csv"
KAMPMANN_EXCEL_FILE = KAMPMANN_ALD_DIR / "Copy of 1_Output Sheet_based on GEM_20240801_new UR.xlsx"
CLIMATE_TRACE_FILE = OUTPUTS_COMPANY_STEEL / "climatetrace_steel_company_annual.csv"

# GEM tracker files — Steel (GIST December 2025 V1)
GEM_STEEL_PLANTS_FILE = GEM_DIR / "Plant-level-data-Global-Iron-and-Steel-Tracker-December-2025-V1.xlsx"
GEM_STEEL_IRON_UNITS_FILE = GEM_DIR / "Iron-unit-data-Global-Iron-and-Steel-Tracker-December-2025-V1.xlsx"
GEM_STEEL_STEEL_UNITS_FILE = GEM_DIR / "Steel-unit-data-Global-Iron-and-Steel-Tracker-December-2025-V1.xlsx"
GEM_FILE = GEM_STEEL_PLANTS_FILE  # Backwards compatibility

# GEM tracker files — Other sectors
GEM_CEMENT_FILE = GEM_DIR / "Global-Cement-and-Concrete-Tracker_July-2025.xlsx"
GEM_COAL_FILE = GEM_DIR / "Global-Coal-Mine-Tracker-May-2025-V2.xlsx"
GEM_OIL_GAS_FILE = GEM_DIR / "Global-Oil-and-Gas-Extraction-Tracker-Feb-2025.xlsx"
GEM_OWNERSHIP_FILE = GEM_DIR / "Global-Energy-Ownership-Tracker-January-2026-V1.xlsx"

# TPI files
TPI_ASSESSMENTS_FILE = TPI_DIR / "Latest_CP_Assessments.csv"

# Oxford NZT files
OXFORD_NZT_FILE = OXFORD_NZT_DIR / "current_snapshot_2026-01-20_06-37-51.xlsx"

# ── Emission factors ─────────────────────────────────────────────────────────
# Source: Koolen & Vidovic (2022) JRC129297 Table A4
# "Greenhouse gas intensities of the EU steel industry and its trading partners"
# Values represent Scope 1 + Upstream emissions for BF-BOF integrated route
#
# Countries marked "Kampmann-derived" use implied EFs from Kampmann ALD (Aug 2024)
# where JRC129297 does not provide a country-specific value and the nearest proxy
# would be misleading.
EF_BF_BOF = {
    # JRC129297 Table A4 — direct country values
    "Brazil": 2.19,
    "China": 1.76,
    "EU": 1.77,
    "India": 3.72,
    "Japan": 2.05,
    "Russia": 2.79,
    "Serbia": 2.06,
    "South Africa": 3.57,
    "South Korea": 2.00,
    "Taiwan": 2.02,
    "Turkey": 2.17,
    "Ukraine": 2.30,
    "United Kingdom": 2.05,
    "United States": 1.94,
    # Kampmann-derived — countries without JRC values
    "Australia": 2.31,
    "New Zealand": 3.10,
    "Indonesia": 2.31,
    "Kazakhstan": 2.31,
    "Canada": 1.84,
    # Global fallback
    "Global": 2.314,
}

EF_TECHNOLOGY = {
    "Scrap-EAF": 0.40,
    "DRI-gas": 1.05,
    "DRI-coal": 3.10,
    "DRI-H2": 0.05,
    "BF-BOF": 2.314,  # global average fallback
}

# ── HTTP settings ─────────────────────────────────────────────────────────────

REQUEST_TIMEOUT = 60
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (academic research; open-asset-data)"
}
