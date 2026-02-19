# =============================================================================
# FILE: r_pipeline/import/climatetrace/cement.R
# Import and process Climate Trace cement (cement) data
# Migrated from CarbonBudgetTracker/02.Scripts/01_import/climatetrace/cement.R
# =============================================================================

library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(lubridate)

cat(">> Importing Climate Trace Cement Data...\n\n")

# =============================================================================
# CONFIGURATION
# =============================================================================

ct_version <- "v5_2_0"
ct_manufacturing_path <- file.path(ClimateTrace_path, "Manufacturing")

ct_cement_emissions <- file.path(ct_manufacturing_path,
                                paste0("cement_emissions_sources_", ct_version, ".csv"))
ct_cement_ownership <- file.path(ct_manufacturing_path,
                                paste0("cement_emissions_sources_ownership_", ct_version, ".csv"))

if (!file.exists(ct_cement_emissions)) stop("Cement emissions file not found: ", ct_cement_emissions)
if (!file.exists(ct_cement_ownership)) stop("Cement ownership file not found: ", ct_cement_ownership)

cat(">> Loading files:\n")
cat("   Emissions:", basename(ct_cement_emissions), "\n")
cat("   Ownership:", basename(ct_cement_ownership), "\n\n")

# =============================================================================
# IMPORT RAW DATA
# =============================================================================

cat(">> Reading emissions data...\n")
emissions_raw <- read_csv(ct_cement_emissions, show_col_types = FALSE)
cat("   Loaded", nrow(emissions_raw), "emission records\n")

cat(">> Reading ownership data...\n")
ownership_raw <- read_csv(ct_cement_ownership, show_col_types = FALSE)
cat("   Loaded", nrow(ownership_raw), "ownership records\n\n")

# =============================================================================
# CLEAN EMISSIONS DATA
# =============================================================================

emissions_clean <- emissions_raw %>%
  mutate(year = year(start_time)) %>%
  filter(gas == "co2e_100yr") %>%
  select(
    source_id,
    source_name,
    source_type,
    facility_country = iso3_country,
    year,
    emissions_t = emissions_quantity,
    activity_t = activity,
    activity_units,
    emissions_factor,
    capacity_t = capacity,
    capacity_factor
  ) %>%
  mutate(
    emissions_t = as.numeric(emissions_t),
    activity_t = as.numeric(activity_t),
    capacity_t = as.numeric(capacity_t),
    capacity_factor = as.numeric(capacity_factor)
  )

cat("   Filtered to co2e_100yr:", nrow(emissions_clean), "records\n")
cat("   Years:", min(emissions_clean$year, na.rm = TRUE), "-",
    max(emissions_clean$year, na.rm = TRUE), "\n\n")

# =============================================================================
# AGGREGATE TO ANNUAL FACILITY LEVEL
# =============================================================================

facility_annual <- emissions_clean %>%
  group_by(source_id, source_name, source_type, facility_country, year) %>%
  summarise(
    emissions_t = sum(emissions_t, na.rm = TRUE),
    activity_t = sum(activity_t, na.rm = TRUE),
    capacity_t = mean(capacity_t, na.rm = TRUE) * 12,
    capacity_factor = mean(capacity_factor, na.rm = TRUE),
    months_reported = n(),
    .groups = "drop"
  ) %>%
  mutate(intensity = if_else(activity_t > 0, emissions_t / activity_t, NA_real_))

# =============================================================================
# CLEAN OWNERSHIP DATA
# =============================================================================

operator_hq <- ownership_raw %>%
  filter(immediate_source_owner == parent_name) %>%
  select(company = immediate_source_owner, company_country = parent_headquarter_country) %>%
  distinct()

ownership_clean <- ownership_raw %>%
  select(source_id, company = immediate_source_owner) %>%
  distinct() %>%
  filter(!is.na(company), company != "unknown", company != "") %>%
  left_join(operator_hq, by = "company") %>%
  mutate(ownership_share = 1)

# =============================================================================
# JOIN EMISSIONS WITH OWNERSHIP
# =============================================================================

facility_with_owners <- facility_annual %>%
  inner_join(
    ownership_clean %>% select(source_id, company, company_country, ownership_share),
    by = "source_id"
  )

facility_allocated <- facility_with_owners %>%
  mutate(
    ownership_share_used = coalesce(ownership_share, 1),
    ownership_imputed = is.na(ownership_share),
    emissions_allocated = emissions_t * ownership_share_used,
    activity_allocated = activity_t * ownership_share_used
  )

# =============================================================================
# AGGREGATE TO COMPANY-COUNTRY-YEAR AND COMPANY-YEAR
# =============================================================================

company_country_annual <- facility_allocated %>%
  group_by(company, company_country, facility_country, year) %>%
  summarise(
    emissions_mt = sum(emissions_allocated, na.rm = TRUE) / 1e6,
    activity_mt = sum(activity_allocated, na.rm = TRUE) / 1e6,
    n_facilities = n_distinct(source_id),
    facility_types = paste(unique(source_type), collapse = ", "),
    avg_ownership_share = mean(ownership_share_used, na.rm = TRUE),
    any_ownership_imputed = any(ownership_imputed),
    .groups = "drop"
  ) %>%
  mutate(intensity = if_else(activity_mt > 0, emissions_mt / activity_mt, NA_real_)) %>%
  arrange(company, facility_country, year)

company_annual <- facility_allocated %>%
  group_by(company, company_country, year) %>%
  summarise(
    emissions_mt = sum(emissions_allocated, na.rm = TRUE) / 1e6,
    activity_mt = sum(activity_allocated, na.rm = TRUE) / 1e6,
    n_facilities = n_distinct(source_id),
    n_countries = n_distinct(facility_country),
    facility_types = paste(unique(source_type), collapse = ", "),
    .groups = "drop"
  ) %>%
  mutate(intensity = if_else(activity_mt > 0, emissions_mt / activity_mt, NA_real_)) %>%
  arrange(company, year)

# =============================================================================
# EXPORT
# =============================================================================

export_csv(company_annual,         "climatetrace_cement_company_annual.csv",
           dir = outputs_cement_dir)
export_csv(company_country_annual, "climatetrace_cement_company_country_annual.csv",
           dir = outputs_cement_dir)
export_csv(facility_annual,        "climatetrace_cement_facility_annual.csv",
           dir = outputs_cement_dir)

cat("\n>> Climate Trace cement import complete!\n")
cat("   Companies:", n_distinct(company_annual$company), "\n")
cat("   Years:", min(company_annual$year), "-", max(company_annual$year), "\n\n")
