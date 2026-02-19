# =============================================================================
# FILE: r_pipeline/00_config.R
# Central configuration for the open-asset-data R pipeline.
# Mirrors pipeline/config.py paths for R scripts.
# =============================================================================

# Use here::here() anchored to open-asset-data/.here
if (!requireNamespace("here", quietly = TRUE)) {
  install.packages("here", repos = "https://cloud.r-project.org")
}
library(here)

project_root <- here::here()

cat("open-asset-data R pipeline\n")
cat("  Project root:", project_root, "\n")

# =============================================================================
# DATA DIRECTORIES
# =============================================================================

raw_data_dir    <- file.path(project_root, "data", "raw")
processed_dir   <- file.path(project_root, "data", "processed")
outputs_dir     <- file.path(project_root, "outputs")

# Processed subdirectories
processed_steel_dir       <- file.path(processed_dir, "steel")
processed_power_dir       <- file.path(processed_dir, "power")
processed_fossil_dir      <- file.path(processed_dir, "fossil_fuels")
processed_sbti_dir        <- file.path(processed_dir, "sbti")

# Output subdirectories (pre-analysis company data)
outputs_steel_dir         <- file.path(outputs_dir, "steel")
outputs_cement_dir        <- file.path(outputs_dir, "cement")
outputs_power_dir         <- file.path(outputs_dir, "power")
outputs_aluminium_dir     <- file.path(outputs_dir, "aluminium")
outputs_fossil_dir        <- file.path(outputs_dir, "fossil_fuels")
outputs_cross_sector_dir  <- file.path(outputs_dir, "cross_sector")

# =============================================================================
# RAW DATA SOURCE DIRECTORIES
# =============================================================================

AU_NGER_path       <- file.path(raw_data_dir, "AU_NGER")
ClimateTrace_path  <- file.path(raw_data_dir, "ClimateTrace")
InfluenceMap_path  <- file.path(raw_data_dir, "InfluenceMap")
AnnualReports_path <- file.path(raw_data_dir, "AnnualReports")
GEM_path           <- file.path(raw_data_dir, "GEM")
TPI_path           <- file.path(raw_data_dir, "TPI")
KampmannALD_path   <- file.path(raw_data_dir, "KampmannALD")

# Forward-looking data
forward_looking_dir <- file.path(raw_data_dir, "ForwardLooking")
OxfordNZT_path      <- file.path(forward_looking_dir, "oxfordNZtracker")
SBTi_path           <- file.path(forward_looking_dir, "SBTi")

# =============================================================================
# SPECIFIC DATA FILES
# =============================================================================

# AU-NGER
nger_closure_file  <- file.path(AU_NGER_path, "Generating Unit Expected Closure Year Jan 2025.xlsx")
nger_mapping_file  <- file.path(AU_NGER_path, "facility_name_mappings.csv")
nger_data_pattern  <- file.path(AU_NGER_path, "greenhouse-and-energy-information-designated-generation-facilit*.csv")

# InfluenceMap
influencemap_file  <- file.path(InfluenceMap_path, "emissions_high_granularity.csv")

# GEM
gem_goget_file     <- file.path(GEM_path, "Global-Oil-and-Gas-Extraction-Tracker-Feb-2025.xlsx")
gem_gcmt_file      <- file.path(GEM_path, "Global-Coal-Mine-Tracker-May-2025-V2.xlsx")
gem_cement_file    <- file.path(GEM_path, "Global-Cement-and-Concrete-Tracker_July-2025.xlsx")
gem_ownership_file <- file.path(GEM_path, "Global-Energy-Ownership-Tracker-January-2026-V1.xlsx")

# TPI
tpi_cp_file        <- file.path(TPI_path, "CP_Assessments_22012026.csv")
tpi_latest_file    <- file.path(TPI_path, "Latest_CP_Assessments.csv")
tpi_benchmarks_file <- file.path(TPI_path, "Sector_Benchmarks_22012026.csv")

# Oxford NZT
oxford_nzt_file    <- file.path(OxfordNZT_path, "current_snapshot_2026-01-20_06-37-51.xlsx")

# SBTi
sbti_companies_file <- file.path(SBTi_path, "companies-excel.xlsx")

# Kampmann ALD
steel_ald_original <- file.path(KampmannALD_path, "SteelALD.csv")
power_ald_path     <- file.path(KampmannALD_path, "PowerALD.csv")

# Processed steel files
steel_ald_combined <- file.path(processed_steel_dir, "steel_ald_combined.csv")
steel_projections  <- file.path(processed_steel_dir, "steel_projections.csv")

# ClimateTrace sub-paths
climatetrace_power_path         <- file.path(ClimateTrace_path, "Power")
climatetrace_manufacturing_path <- file.path(ClimateTrace_path, "Manufacturing")
climatetrace_transport_path     <- file.path(ClimateTrace_path, "Transportation")
climatetrace_buildings_path     <- file.path(ClimateTrace_path, "Buildings")

# ClimateTrace specific files (v5.2.0)
ct_power_emissions_file   <- file.path(climatetrace_power_path, "electricity-generation_emissions_sources_v5_2_0.csv")
ct_power_ownership_file   <- file.path(climatetrace_power_path, "electricity-generation_emissions_sources_ownership_v5_2_0.csv")
ct_steel_emissions_file   <- file.path(climatetrace_manufacturing_path, "steel_emissions_sources_v5_2_0.csv")
ct_steel_ownership_file   <- file.path(climatetrace_manufacturing_path, "steel_emissions_sources_ownership_v5_2_0.csv")
ct_cement_emissions_file  <- file.path(climatetrace_manufacturing_path, "cement_emissions_sources_v5_2_0.csv")
ct_cement_ownership_file  <- file.path(climatetrace_manufacturing_path, "cement_emissions_sources_ownership_v5_2_0.csv")

# =============================================================================
# CREATE DIRECTORIES IF NEEDED
# =============================================================================

for (dir in c(processed_steel_dir, processed_power_dir, processed_fossil_dir,
              processed_sbti_dir,
              outputs_steel_dir, outputs_cement_dir, outputs_power_dir,
              outputs_aluminium_dir, outputs_fossil_dir, outputs_cross_sector_dir)) {
  if (!dir.exists(dir)) dir.create(dir, recursive = TRUE)
}

# =============================================================================
# LOAD UTILITIES
# =============================================================================

source(file.path(project_root, "r_pipeline", "00_utilities.R"))

cat("  Configuration loaded\n\n")
