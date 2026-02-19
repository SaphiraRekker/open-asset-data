# =============================================================================
# FILE: r_pipeline/import/climatetrace/power.R
# Import and process Climate Trace electricity generation data
# Migrated from CarbonBudgetTracker/02.Scripts/01_import/climatetrace/power.R
# =============================================================================

library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(lubridate)

cat(">> Importing Climate Trace Power Data...\n\n")

# =============================================================================
# CONFIGURATION
# =============================================================================

ct_version <- "v5_2_0"
ct_power_path <- file.path(ClimateTrace_path, "Power")

ct_power_emissions <- file.path(ct_power_path,
                                paste0("electricity-generation_emissions_sources_", ct_version, ".csv"))
ct_power_ownership <- file.path(ct_power_path,
                                paste0("electricity-generation_emissions_sources_ownership_", ct_version, ".csv"))

if (!file.exists(ct_power_emissions)) stop("Power emissions file not found: ", ct_power_emissions)
if (!file.exists(ct_power_ownership)) stop("Power ownership file not found: ", ct_power_ownership)

cat(">> Loading files:\n")
cat("   Emissions:", basename(ct_power_emissions), "\n")
cat("   Ownership:", basename(ct_power_ownership), "\n\n")

# =============================================================================
# IMPORT RAW DATA
# =============================================================================

cat(">> Reading emissions data...\n")
emissions_raw <- read_csv(ct_power_emissions, show_col_types = FALSE)
cat("   Loaded", nrow(emissions_raw), "emission records\n")

cat(">> Reading ownership data...\n")
ownership_raw <- read_csv(ct_power_ownership, show_col_types = FALSE)
cat("   Loaded", nrow(ownership_raw), "ownership records\n\n")

# =============================================================================
# EXPLORE DATA STRUCTURE
# =============================================================================

cat(">> DATA STRUCTURE:\n")
cat("Emissions columns:", paste(names(emissions_raw), collapse = ", "), "\n\n")
cat("Ownership columns:", paste(names(ownership_raw), collapse = ", "), "\n\n")

# Check for activity column - power may use different name
activity_cols <- names(emissions_raw)[grepl("activity|generation|capacity|output", names(emissions_raw), ignore.case = TRUE)]
cat("Potential activity columns:", paste(activity_cols, collapse = ", "), "\n\n")

# Check source types (fuel types)
if ("source_type" %in% names(emissions_raw)) {
  cat("Source types (fuel types):\n")
  print(table(emissions_raw$source_type))
  cat("\n")
}

# =============================================================================
# PROCESS EMISSIONS DATA
# =============================================================================

cat(">> Processing emissions data...\n")

emissions_clean <- emissions_raw %>%
  mutate(year = year(start_time)) %>%
  filter(!is.na(year))

# Check what activity column exists
if ("activity" %in% names(emissions_clean)) {
  activity_col <- "activity"
} else if ("generation" %in% names(emissions_clean)) {
  activity_col <- "generation"
} else {
  cat(">> WARNING: No 'activity' column found. Numeric columns:\n")
  print(names(emissions_clean)[sapply(emissions_clean, is.numeric)])
  activity_col <- "activity"  # Will fail gracefully
}

cat("   Using activity column:", activity_col, "\n")

# Aggregate monthly to annual at facility level
facility_annual <- emissions_clean %>%
  group_by(source_id, iso3_country, source_type, year) %>%
  summarise(
    emissions_tonnes = sum(emissions_quantity, na.rm = TRUE),
    activity_value = sum(get(activity_col), na.rm = TRUE),
    n_months = n(),
    .groups = "drop"
  )

cat("   Aggregated to", nrow(facility_annual), "facility-year records\n")

# =============================================================================
# PROCESS OWNERSHIP DATA
# =============================================================================

cat(">> Processing ownership data...\n")

# Use immediate_source_owner (operator) instead of parent_name (ultimate shareholder)
# This avoids attributing emissions to BlackRock, Vanguard, pension funds etc.

# Step 1: Get operator HQ countries (where operator is also listed as shareholder)
operator_hq <- ownership_raw %>%
  filter(immediate_source_owner == parent_name) %>%
  select(company = immediate_source_owner, company_country = parent_headquarter_country) %>%
  distinct()

cat("   Operator HQ countries found:", nrow(operator_hq), "\n")

# Step 2: Get unique operators per facility
ownership_clean <- ownership_raw %>%
  select(source_id, company = immediate_source_owner) %>%
  distinct() %>%
  filter(!is.na(company), company != "unknown", company != "") %>%
  left_join(operator_hq, by = "company") %>%
  mutate(ownership_share = 1)

cat("   Processed", nrow(ownership_clean), "ownership records\n")
cat("   Operating companies found:", n_distinct(ownership_clean$company), "\n\n")

# =============================================================================
# JOIN EMISSIONS WITH OWNERSHIP
# =============================================================================

cat(">> Joining emissions with ownership...\n")

facility_with_owners <- facility_annual %>%
  left_join(ownership_clean, by = "source_id", relationship = "many-to-many")

# Check for facilities without ownership
no_owner <- facility_with_owners %>% filter(is.na(company))
if (nrow(no_owner) > 0) {
  cat("   >>", n_distinct(no_owner$source_id), "facilities have no ownership data\n")
  cat("      These will be assigned 'Unknown' company with 100% ownership\n")

  facility_with_owners <- facility_with_owners %>%
    mutate(
      company = ifelse(is.na(company), "Unknown", company),
      company_country = ifelse(is.na(company_country), "Unknown", company_country),
      ownership_share = ifelse(is.na(ownership_share), 1, ownership_share)
    )
}

cat("   Joined data has", nrow(facility_with_owners), "records\n\n")

# =============================================================================
# ALLOCATE BY OWNERSHIP SHARE
# =============================================================================

cat(">> Allocating emissions by ownership share...\n")

facility_allocated <- facility_with_owners %>%
  mutate(
    emissions_allocated = emissions_tonnes * ownership_share,
    activity_allocated = activity_value * ownership_share
  )

# =============================================================================
# AGGREGATE TO COMPANY LEVEL
# =============================================================================

cat(">> Aggregating to company level...\n")

# Company-year level (main output for SDA/ACA)
company_annual <- facility_allocated %>%
  group_by(company, company_country, year) %>%
  summarise(
    emissions_tonnes = sum(emissions_allocated, na.rm = TRUE),
    activity_value = sum(activity_allocated, na.rm = TRUE),
    n_facilities = n_distinct(source_id),
    n_countries = n_distinct(iso3_country),
    fuel_types = paste(sort(unique(source_type)), collapse = "; "),
    .groups = "drop"
  ) %>%
  mutate(
    # Convert to Mt for emissions, TWh for activity (assuming activity is in MWh)
    emissions_mt = emissions_tonnes / 1e6,
    activity_twh = activity_value / 1e6,  # MWh to TWh
    # Calculate intensity (tCO2e per MWh, or kgCO2/kWh)
    intensity = ifelse(activity_value > 0, emissions_tonnes / activity_value * 1000, NA)  # gCO2/kWh
  )

cat("   Company-year records:", nrow(company_annual), "\n")

# Company-country-year level (for equity analysis)
company_country_annual <- facility_allocated %>%
  group_by(company, company_country, iso3_country, year) %>%
  summarise(
    emissions_tonnes = sum(emissions_allocated, na.rm = TRUE),
    activity_value = sum(activity_allocated, na.rm = TRUE),
    n_facilities = n_distinct(source_id),
    fuel_types = paste(sort(unique(source_type)), collapse = "; "),
    .groups = "drop"
  ) %>%
  mutate(
    emissions_mt = emissions_tonnes / 1e6,
    activity_twh = activity_value / 1e6,
    intensity = ifelse(activity_value > 0, emissions_tonnes / activity_value * 1000, NA)
  )

cat("   Company-country-year records:", nrow(company_country_annual), "\n\n")

# =============================================================================
# EXPORT
# =============================================================================

export_csv(company_annual,         "climatetrace_power_company_annual.csv",
           dir = outputs_power_dir)
export_csv(company_country_annual, "climatetrace_power_company_country_annual.csv",
           dir = outputs_power_dir)
export_csv(facility_annual,        "climatetrace_power_facility_annual.csv",
           dir = outputs_power_dir)

cat("\n>> Climate Trace power import complete!\n")
cat("   Companies:", n_distinct(company_annual$company), "\n")
cat("   Years:", min(company_annual$year), "-", max(company_annual$year), "\n\n")
