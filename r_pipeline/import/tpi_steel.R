# =============================================================================
# FILE: r_pipeline/import/tpi_steel.R
# Import TPI Carbon Performance data for steel companies
# Migrated from CarbonBudgetTracker/02.Scripts/01_import/09_tpi_steel.R
# =============================================================================
#
# TPI provides INTENSITY data (tCO2/t steel), not absolute emissions.
# To get absolute emissions: Emissions = Intensity x Production
# Production must come from another source (WSA, company reports).
#
# INPUT:  data/raw/TPI/Latest_CP_Assessments.csv
# OUTPUT: data/processed/steel/tpi_steel_intensity.csv
# =============================================================================

library(tidyverse)

cat(">> Importing TPI steel data...\n\n")

# =============================================================================
# IMPORT
# =============================================================================

import_tpi_steel <- function(input_path = tpi_latest_file) {

  cat("1. Reading TPI CP_Assessments...\n")
  tpi_raw <- read_csv(input_path, show_col_types = FALSE)
  cat("   Total companies:", nrow(tpi_raw), "\n\n")

  cat("2. Filtering to Steel sector...\n")
  tpi_steel <- tpi_raw %>% filter(Sector == "Steel")
  cat("   Steel companies:", nrow(tpi_steel), "\n\n")

  cat("   Companies found:\n")
  for (co in sort(unique(tpi_steel$`Company Name`))) {
    cat("     -", co, "\n")
  }
  cat("\n")

  year_cols <- as.character(2013:2050)

  cat("3. Reshaping to long format...\n")
  tpi_long <- tpi_steel %>%
    select(
      company_id       = `Company ID`,
      company          = `Company Name`,
      country          = Geography,
      country_code     = `Geography Code`,
      ca100            = `CA100 Focus Company`,
      alignment_2030   = `Carbon Performance Alignment 2035`,
      alignment_2050   = `Carbon Performance Alignment 2050`,
      unit             = `CP Unit`,
      cutoff_year      = `History to Projection Cutoff Year`,
      all_of(year_cols)
    ) %>%
    pivot_longer(
      cols = all_of(year_cols),
      names_to = "year",
      values_to = "intensity_tco2_per_t"
    ) %>%
    mutate(
      year = as.integer(year),
      intensity_tco2_per_t = as.numeric(intensity_tco2_per_t),
      is_historical = year <= as.integer(cutoff_year),
      data_type = if_else(is_historical, "historical", "projected")
    ) %>%
    filter(!is.na(intensity_tco2_per_t)) %>%
    arrange(company, year)

  cat("   Records:", nrow(tpi_long), "\n")
  cat("   Years:", min(tpi_long$year), "-", max(tpi_long$year), "\n\n")

  # Summary
  cat("4. Summary by company:\n")
  summary_df <- tpi_long %>%
    group_by(company, country) %>%
    summarise(
      years_historical = sum(is_historical),
      years_projected  = sum(!is_historical),
      first_year       = min(year),
      last_year        = max(year),
      latest_intensity = last(intensity_tco2_per_t[is_historical]),
      alignment_2050   = first(alignment_2050),
      .groups = "drop"
    )

  for (i in 1:nrow(summary_df)) {
    cat(sprintf("   %-30s %s  Hist:%d  Proj:%d  Latest:%.2f  Align:%s\n",
                summary_df$company[i],
                summary_df$country[i],
                summary_df$years_historical[i],
                summary_df$years_projected[i],
                summary_df$latest_intensity[i],
                summary_df$alignment_2050[i]))
  }

  tpi_long <- tpi_long %>%
    mutate(
      source       = "TPI",
      source_file  = basename(input_path),
      source_url   = "https://www.transitionpathwayinitiative.org/sectors/steel",
      accessed_date = Sys.Date()
    )

  return(tpi_long)
}

# =============================================================================
# RUN
# =============================================================================

tpi_steel <- import_tpi_steel()

tpi_output_path <- file.path(processed_steel_dir, "tpi_steel_intensity.csv")
cat("\n5. Saving to:", tpi_output_path, "\n")
write_csv(tpi_steel, tpi_output_path)
cat("   Done!\n\n")

cat(">> TPI steel import complete\n")
cat("   NOTE: TPI provides INTENSITY only, not production.\n\n")
