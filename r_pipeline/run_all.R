# =============================================================================
# FILE: r_pipeline/run_all.R
# Master script for the open-asset-data R pipeline.
# Processes company-level data from various sources into standardised CSVs.
#
# Usage: cd open-asset-data && Rscript r_pipeline/run_all.R
#
# Prerequisite: Python pipeline should have been run first for steel data:
#   python -m pipeline.orchestrator
# =============================================================================

cat("=== open-asset-data R Pipeline ===\n")
cat(paste(rep("=", 50), collapse = ""), "\n\n")

start_time <- Sys.time()

# Load configuration and utilities
source("r_pipeline/00_config.R")

# =============================================================================
# STEP 1: IMPORT — Raw company data from external sources
# =============================================================================

cat("STEP 1: Importing company data\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

# Australian power sector (NGER)
source("r_pipeline/import/nger_power.R")

# InfluenceMap fossil fuel production
source("r_pipeline/import/influencemap.R")

# GEM reserves + ownership matching (depends on influencemap)
source("r_pipeline/import/gem_reserves.R")

# TPI steel carbon intensity
source("r_pipeline/import/tpi_steel.R")

# SBTi target matching
source("r_pipeline/import/sbti.R")

# ClimateTrace satellite-based emissions (all sectors)
for (f in sort(list.files("r_pipeline/import/climatetrace",
                          full.names = TRUE, pattern = "\\.R$"))) {
  source(f)
}

# =============================================================================
# STEP 2: PROCESS — Transform and project company data
# =============================================================================

cat("\nSTEP 2: Processing company data\n")
cat(paste(rep("-", 50), collapse = ""), "\n")

# Convert InfluenceMap production to EJ
source("r_pipeline/process/convert_to_EJ.R")

# NGER scenario projections
source("r_pipeline/process/nger_scenarios.R")

# NGER forward projections to 2050
source("r_pipeline/process/nger_forward.R")

# Oxford NZT fossil fuel projections
source("r_pipeline/process/fossil_fuel_nzt.R")

# =============================================================================
# COMPLETE
# =============================================================================

end_time <- Sys.time()
duration <- round(difftime(end_time, start_time, units = "mins"), 1)

cat("\n", paste(rep("=", 50), collapse = ""), "\n")
cat("R Pipeline complete!\n")
cat("  Duration:", duration, "minutes\n")
cat("  Outputs:", outputs_dir, "\n")
cat("  Processed:", processed_dir, "\n")
cat(paste(rep("=", 50), collapse = ""), "\n\n")
