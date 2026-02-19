# =============================================================================
# FILE: r_pipeline/process/nger_scenarios.R
# Match NGER facilities to closure dates
# Migrated from CarbonBudgetTracker/02.Scripts/02_process/04_nger_scenarios.R
# =============================================================================

library(readr)
library(dplyr)
library(tidyr)
library(readxl)
library(stringr)

cat(">> Matching NGER facilities to closure dates...\n\n")

# =============================================================================
# CONFIG
# =============================================================================

closure_file       <- nger_closure_file
manual_mapping_file <- nger_mapping_file

# =============================================================================
# LOAD DATA (from CSVs produced by r_pipeline/import/nger_power.R)
# =============================================================================

nger_facilities_file <- file.path(outputs_power_dir, "nger_facilities.csv")
if (!file.exists(nger_facilities_file)) {
  stop("nger_facilities.csv not found. Run r_pipeline/import/nger_power.R first.")
}
nger_facilities <- read_csv(nger_facilities_file, show_col_types = FALSE)

# Get facilities active in 2023
facility_list <- nger_facilities %>%
  group_by(company, facility) %>%
  summarise(
    last_year = max(year, na.rm = TRUE),
    first_year = min(year, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(last_year >= 2023) %>%
  arrange(company, facility)

cat(">> NGER facilities still operating in 2023:", nrow(facility_list), "\n")

closures_raw <- read_excel(closure_file, sheet = "Expected Closure Year")

closures <- closures_raw %>%
  transmute(
    facility_excel = str_trim(Comp),
    closure_year = as.integer(`Expected Closure Year`)
  ) %>%
  filter(!is.na(facility_excel), !is.na(closure_year))

cat(">> Closure dates loaded:", nrow(closures), "\n\n")

# =============================================================================
# NAME CLEANING
# =============================================================================

clean_facility_name <- function(x) {
  x %>%
    str_to_upper() %>% str_trim() %>%
    str_replace_all("\\bWIND FARM\\b", "") %>%
    str_replace_all("\\bSOLAR FARM\\b", "") %>%
    str_replace_all("\\bSOLAR PARK\\b", "") %>%
    str_replace_all("\\bPOWER STATION\\b", "") %>%
    str_replace_all("\\bGENERATING STATION\\b", "") %>%
    str_replace_all("\\bENERGY PROJECT\\b", "") %>%
    str_replace_all("\\bPROJECT\\b", "") %>%
    str_replace_all("\\bSTATION\\b", "") %>%
    str_replace_all("\\bHYDRO\\b", "") %>%
    str_replace_all("[^A-Z0-9 ]", "") %>%
    str_replace_all("\\s+", " ") %>%
    str_trim()
}

facility_list <- facility_list %>% mutate(facility_clean = clean_facility_name(facility))
closures <- closures %>% mutate(facility_clean = clean_facility_name(facility_excel))

# =============================================================================
# MANUAL MAPPINGS
# =============================================================================

manual_mappings <- tibble(nger_name = character(), excel_name = character())

if (file.exists(manual_mapping_file)) {
  manual_mappings <- read_csv(manual_mapping_file, show_col_types = FALSE)
  cat(">> Loaded", nrow(manual_mappings), "manual mappings\n\n")
}

# =============================================================================
# MATCHING
# =============================================================================

auto_matched <- facility_list %>%
  left_join(closures %>% select(facility_clean, closure_year, facility_excel),
            by = "facility_clean")

if (nrow(manual_mappings) > 0) {
  manual_match_lookup <- closures %>%
    inner_join(manual_mappings, by = c("facility_excel" = "excel_name")) %>%
    select(nger_name, closure_year)

  auto_matched <- auto_matched %>%
    left_join(manual_match_lookup, by = c("facility" = "nger_name"), suffix = c("", "_manual")) %>%
    mutate(
      closure_year = coalesce(closure_year, closure_year_manual),
      match_type = case_when(
        !is.na(closure_year_manual) ~ "Manual",
        !is.na(closure_year) ~ "Auto",
        TRUE ~ "Unmatched"
      )
    ) %>%
    select(-closure_year_manual, -facility_excel)
} else {
  auto_matched <- auto_matched %>%
    mutate(match_type = if_else(!is.na(closure_year), "Auto", "Unmatched"))
}

cat(">> MATCHING RESULTS:\n")
cat("   Auto matched:", sum(auto_matched$match_type == "Auto"), "\n")
if (nrow(manual_mappings) > 0) {
  cat("   Manual matched:", sum(auto_matched$match_type == "Manual"), "\n")
}
cat("   Unmatched:", sum(auto_matched$match_type == "Unmatched"), "\n\n")

# =============================================================================
# EXPORT
# =============================================================================

export_csv(auto_matched, "facility_closure_matching_complete.csv", dir = processed_power_dir)

matched_only <- auto_matched %>% filter(match_type != "Unmatched")
export_csv(matched_only, "facilities_matched.csv", dir = processed_power_dir)

# Assign default closure for unmatched (2050)
default_closure_year <- 2050L
default_facilities <- auto_matched %>%
  filter(match_type == "Unmatched") %>%
  mutate(closure_year = default_closure_year)
export_csv(default_facilities, "facilities_default_closure.csv", dir = processed_power_dir)

cat("\n>> Closure matching complete!\n")
cat("   Matched:", nrow(matched_only), "facilities\n")
cat("   Default (", default_closure_year, "):", nrow(default_facilities), "facilities\n\n")
