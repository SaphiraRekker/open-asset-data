# =============================================================================
# FILE: r_pipeline/process/fossil_fuel_nzt.R
# Process Oxford Net Zero Tracker targets for fossil fuel companies
# Migrated from CarbonBudgetTracker/02.Scripts/02_process/fossil_fuels/fossil_fuel_nzt.R
# =============================================================================

library(readr)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)

cat(">> Processing Oxford NZT targets for fossil fuels...\n\n")

# =============================================================================
# CONFIGURATION
# =============================================================================

ACTUAL_DATA_END <- 2023
PROJECTION_END  <- 2050

# =============================================================================
# LOAD NZT DATA
# =============================================================================

cat("Loading Oxford Net Zero Tracker data...\n")

nzt_raw <- read_excel(oxford_nzt_file, sheet = "Current Snapshot", skip = 1)
names(nzt_raw) <- make.names(names(nzt_raw), unique = TRUE)

cat("  Loaded", nrow(nzt_raw), "entities from NZT\n")

# Find columns dynamically
name_col <- names(nzt_raw)[grep("^Name$", names(nzt_raw), ignore.case = TRUE)[1]]
industry_col <- names(nzt_raw)[grep("Industry", names(nzt_raw), ignore.case = TRUE)[1]]
end_target_year_col <- names(nzt_raw)[grep("End_target_year$|End\\.target\\.year$", names(nzt_raw), ignore.case = TRUE)[1]]
end_target_pct_col <- names(nzt_raw)[grep("End.*target.*percentage", names(nzt_raw), ignore.case = TRUE)[1]]
interim_year_col <- names(nzt_raw)[grep("Interim.*target.*year", names(nzt_raw), ignore.case = TRUE)[1]]
interim_pct_col <- names(nzt_raw)[grep("Interim.*target.*percentage", names(nzt_raw), ignore.case = TRUE)[1]]
interim_baseline_year_col <- names(nzt_raw)[grep("Interim.*target.*baseline.*year", names(nzt_raw), ignore.case = TRUE)[1]]
end_baseline_year_col <- names(nzt_raw)[grep("End.*target.*baseline.*year", names(nzt_raw), ignore.case = TRUE)[1]]
scope1_col <- names(nzt_raw)[grep("Scope.*1.*coverage", names(nzt_raw), ignore.case = TRUE)[1]]
scope2_col <- names(nzt_raw)[grep("Scope.*2.*coverage", names(nzt_raw), ignore.case = TRUE)[1]]
scope3_col <- names(nzt_raw)[grep("Scope.*3.*coverage", names(nzt_raw), ignore.case = TRUE)[1]]

# Filter to fossil fuel companies
nzt_fossil <- nzt_raw %>%
  filter(.data[[industry_col]] == "Fossil Fuels") %>%
  select(
    nzt_name = all_of(name_col),
    industry = all_of(industry_col),
    end_target_year = all_of(end_target_year_col),
    end_target_pct = all_of(end_target_pct_col),
    end_baseline_year = all_of(end_baseline_year_col),
    interim_target_year = all_of(interim_year_col),
    interim_target_pct = all_of(interim_pct_col),
    interim_baseline_year = all_of(interim_baseline_year_col),
    scope1_coverage = all_of(scope1_col),
    scope2_coverage = all_of(scope2_col),
    scope3_coverage = all_of(scope3_col)
  ) %>%
  mutate(
    end_target_year = as.numeric(end_target_year),
    interim_target_year = as.numeric(interim_target_year),
    end_target_pct = as.numeric(end_target_pct),
    interim_target_pct = as.numeric(interim_target_pct),
    end_baseline_year = as.numeric(end_baseline_year),
    interim_baseline_year = as.numeric(interim_baseline_year)
  )

cat("  Found", nrow(nzt_fossil), "fossil fuel companies in NZT\n")

# =============================================================================
# LOAD COMPANY NAME MAPPING
# =============================================================================

cat("\nLoading company name mapping...\n")

mapping_file_path <- file.path(processed_fossil_dir, "nzt_company_mapping.csv")

if (file.exists(mapping_file_path)) {
  company_mapping <- read_csv(mapping_file_path, show_col_types = FALSE) %>%
    filter(!is.na(nzt_name), nzt_name != "")
  cat("  Loaded mapping for", nrow(company_mapping), "companies\n")

  nzt_matched <- nzt_fossil %>%
    inner_join(company_mapping, by = "nzt_name") %>%
    filter(!is.na(influencemap_name))
  cat("  Matched", nrow(nzt_matched), "companies with InfluenceMap\n")
} else {
  cat("  Warning: Mapping file not found at", mapping_file_path, "\n")
  nzt_matched <- tibble()
}

# =============================================================================
# LOAD HISTORICAL EMISSIONS DATA (from CSV instead of RDS)
# =============================================================================

cat("\nLoading InfluenceMap EJ data...\n")

im_ej_file <- file.path(outputs_fossil_dir, "influencemap_production_EJ.csv")
if (!file.exists(im_ej_file)) {
  stop("influencemap_production_EJ.csv not found. Run r_pipeline/process/convert_to_EJ.R first.")
}
influencemap_ej <- read_csv(im_ej_file, show_col_types = FALSE)

emissions_by_year <- influencemap_ej %>%
  group_by(parent_entity, year) %>%
  summarise(
    total_emissions_MtCO2 = sum(product_emissions, na.rm = TRUE),
    total_production_EJ = sum(production_EJ, na.rm = TRUE),
    .groups = "drop"
  )

latest_emissions <- emissions_by_year %>% filter(year == ACTUAL_DATA_END)

cat("  Emissions data from", min(emissions_by_year$year), "to", max(emissions_by_year$year), "\n")
cat("  Latest emissions (", ACTUAL_DATA_END, ") for", nrow(latest_emissions), "companies\n")

# =============================================================================
# GENERATE NZT-BASED PROJECTIONS
# =============================================================================

cat("\nGenerating NZT-based projections...\n")

if (nrow(nzt_matched) > 0) {
  nzt_projections <- list()

  for (i in 1:nrow(nzt_matched)) {
    row <- nzt_matched[i, ]
    company <- row$influencemap_name
    baseline <- latest_emissions %>% filter(parent_entity == company)

    if (nrow(baseline) == 0) {
      cat("  No emissions data for", company, ", skipping\n")
      next
    }

    base_emissions  <- baseline$total_emissions_MtCO2
    base_production <- baseline$total_production_EJ
    end_year <- row$end_target_year
    end_pct  <- row$end_target_pct
    end_baseline_yr <- row$end_baseline_year
    interim_year <- row$interim_target_year
    interim_pct  <- row$interim_target_pct
    interim_baseline_yr <- row$interim_baseline_year

    if (is.na(end_year)) {
      cat("  No end target for", company, ", skipping\n")
      next
    }
    if (is.na(end_pct)) end_pct <- 100

    # Off-track assessment
    baseline_year <- if (!is.na(interim_baseline_yr)) interim_baseline_yr else end_baseline_yr
    baseline_row <- emissions_by_year %>%
      filter(parent_entity == company, year == baseline_year)
    baseline_emissions <- if (nrow(baseline_row) > 0) baseline_row$total_emissions_MtCO2 else NA
    on_track_emissions_now <- NA
    off_track_gap_pct <- NA

    if (!is.na(baseline_emissions) && !is.na(baseline_year)) {
      has_interim <- !is.na(interim_year) & !is.na(interim_pct)
      if (has_interim && ACTUAL_DATA_END <= interim_year && baseline_year < interim_year) {
        target_at_interim <- baseline_emissions * (1 - interim_pct / 100)
        progress <- (ACTUAL_DATA_END - baseline_year) / (interim_year - baseline_year)
        on_track_emissions_now <- baseline_emissions - (baseline_emissions - target_at_interim) * progress
      } else if (!has_interim && baseline_year < end_year) {
        target_at_end <- baseline_emissions * (1 - end_pct / 100)
        progress <- (ACTUAL_DATA_END - baseline_year) / (end_year - baseline_year)
        on_track_emissions_now <- baseline_emissions - (baseline_emissions - target_at_end) * progress
      }
      if (!is.na(on_track_emissions_now) && on_track_emissions_now > 0) {
        off_track_gap_pct <- ((base_emissions - on_track_emissions_now) / on_track_emissions_now) * 100
      }
    }

    # Create projection
    years <- ACTUAL_DATA_END:PROJECTION_END
    has_interim <- !is.na(interim_year) & !is.na(interim_pct)
    end_emissions <- base_emissions * (1 - end_pct / 100)
    interim_emissions <- if (has_interim) base_emissions * (1 - interim_pct / 100) else NA
    emission_intensity <- base_emissions / base_production

    emissions_pathway <- tibble(year = years) %>%
      mutate(
        target_emissions = case_when(
          year <= ACTUAL_DATA_END ~ base_emissions,
          year >= end_year ~ end_emissions,
          has_interim & year <= interim_year ~ {
            progress <- (year - ACTUAL_DATA_END) / (interim_year - ACTUAL_DATA_END)
            base_emissions - (base_emissions - interim_emissions) * progress
          },
          has_interim & year > interim_year ~ {
            progress <- (year - interim_year) / (end_year - interim_year)
            interim_emissions - (interim_emissions - end_emissions) * progress
          },
          TRUE ~ {
            progress <- (year - ACTUAL_DATA_END) / (end_year - ACTUAL_DATA_END)
            base_emissions - (base_emissions - end_emissions) * progress
          }
        ),
        target_production_EJ = target_emissions / emission_intensity,
        company = company,
        scenario = "nzt",
        data_source = "oxford_nzt",
        nzt_baseline_year = baseline_year,
        nzt_baseline_emissions = baseline_emissions,
        nzt_on_track_now = on_track_emissions_now,
        nzt_actual_now = base_emissions,
        nzt_off_track_gap_pct = off_track_gap_pct
      ) %>%
      mutate(
        target_emissions = pmax(target_emissions, 0),
        target_production_EJ = pmax(target_production_EJ, 0)
      )

    nzt_projections[[company]] <- emissions_pathway
  }

  if (length(nzt_projections) > 0) {
    nzt_proj_all <- bind_rows(nzt_projections) %>%
      group_by(company) %>%
      arrange(year) %>%
      mutate(
        annual_production_nzt = target_production_EJ,
        cumulative_production_nzt = cumsum(target_production_EJ)
      ) %>%
      ungroup()

    cat("  Generated NZT projections for", n_distinct(nzt_proj_all$company), "companies\n")

    nzt_output_file <- file.path(processed_fossil_dir, "nzt_projections.csv")
    write_csv(nzt_proj_all, nzt_output_file)
    cat("  Saved to:", nzt_output_file, "\n")
  } else {
    cat("  Warning: No NZT projections generated\n")
  }

} else {
  cat("  No matched companies for NZT projections\n")
}

cat("\n>> Fossil fuel NZT processing complete!\n\n")
