# =============================================================================
# FILE: r_pipeline/process/convert_to_EJ.R
# Apply conversion factors to convert fossil fuel production to EJ
# Migrated from CarbonBudgetTracker/02.Scripts/02_process/03_convert_to_EJ.R
# =============================================================================

library(dplyr)
library(tidyr)
library(readr)
library(stringr)

cat(">> Converting fossil fuel production to EJ...\n\n")

# =============================================================================
# LOAD DATA
# =============================================================================

# Load conversion factors (exported from CBT's 02_conversion_factors.R)
conversion_factors_file <- file.path(processed_fossil_dir, "conversion_factors.csv")
if (!file.exists(conversion_factors_file)) {
  stop("conversion_factors.csv not found at: ", conversion_factors_file,
       "\nRun CBT's 02_conversion_factors.R first, or commit the file to open-asset-data")
}
conversion_factors <- read_csv(conversion_factors_file, show_col_types = FALSE)
cat(">> Conversion factors loaded:", nrow(conversion_factors), "factors\n")

# Load InfluenceMap data (produced by r_pipeline/import/influencemap.R)
influencemap_clean_file <- file.path(outputs_fossil_dir, "influencemap_emissions_clean.csv")
if (!file.exists(influencemap_clean_file)) {
  stop("influencemap_emissions_clean.csv not found. Run r_pipeline/import/influencemap.R first.")
}
influencemap_clean <- read_csv(influencemap_clean_file, show_col_types = FALSE)

cat(">> InfluenceMap data loaded:", nrow(influencemap_clean), "records\n")
cat("   Companies:", n_distinct(influencemap_clean$parent_entity), "\n")
cat("   Years:", min(influencemap_clean$year), "-", max(influencemap_clean$year), "\n")

# Ensure Fuel column exists
if (!"Fuel" %in% names(influencemap_clean)) {
  influencemap_clean <- influencemap_clean %>%
    mutate(
      Fuel = case_when(
        commodity %in% c("Sub-Bituminous Coal", "Bituminous Coal", "Thermal Coal") ~ "Coal",
        commodity == "Oil & NGL" ~ "Oil",
        commodity == "Natural Gas" ~ "Gas",
        TRUE ~ NA_character_
      )
    )
}
cat("\n")

# =============================================================================
# APPLY CONVERSION TO EJ
# =============================================================================

cat(">> Converting production values to EJ...\n")

influencemap_ej <- convert_to_EJ(influencemap_clean, conversion_factors)

# Ensure Fuel column
if (!"Fuel" %in% names(influencemap_ej)) {
  influencemap_ej <- influencemap_ej %>%
    mutate(
      Fuel = case_when(
        commodity %in% c("Sub-Bituminous Coal", "Bituminous Coal", "Thermal Coal") ~ "Coal",
        commodity == "Oil & NGL" ~ "Oil",
        commodity == "Natural Gas" ~ "Gas",
        TRUE ~ NA_character_
      )
    )
}

cat("   Records with EJ values:", sum(!is.na(influencemap_ej$production_EJ)), "\n")
cat("   Records missing EJ:", sum(is.na(influencemap_ej$production_EJ)), "\n\n")

# =============================================================================
# AGGREGATE COAL TYPES
# =============================================================================

cat(">> Aggregating coal types...\n")

coal_aggregated <- influencemap_ej %>%
  filter(Fuel == "Coal") %>%
  group_by(parent_entity, year, Fuel, parent_type) %>%
  summarise(
    n_reporting_entities = sum(n_reporting_entities, na.rm = TRUE),
    production_value = sum(production_value, na.rm = TRUE),
    production_EJ = sum(production_EJ, na.rm = TRUE),
    product_emissions = sum(product_emissions, na.rm = TRUE),
    commodity = "Coal (All Types)",
    production_unit = "million_tonnes (aggregated)",
    coal_types_included = paste(unique(commodity), collapse = " + "),
    .groups = "drop"
  )

non_coal <- influencemap_ej %>% filter(Fuel != "Coal")

influencemap_final <- bind_rows(coal_aggregated, non_coal) %>%
  arrange(parent_entity, year, Fuel)

cat("   Before:", nrow(influencemap_ej), "rows -> After:", nrow(influencemap_final), "rows\n")
cat("   Total production:", round(sum(influencemap_final$production_EJ, na.rm = TRUE), 2), "EJ\n")
cat("   Total emissions:", round(sum(influencemap_final$product_emissions, na.rm = TRUE), 2), "Mt CO2\n\n")

# =============================================================================
# SUMMARIES
# =============================================================================

commodity_summary <- influencemap_ej %>%
  group_by(commodity, Fuel) %>%
  summarise(
    companies = n_distinct(parent_entity),
    records = n(),
    total_production_EJ = sum(production_EJ, na.rm = TRUE),
    total_emissions = sum(product_emissions, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(Fuel, desc(total_production_EJ))

fuel_summary <- influencemap_final %>%
  group_by(Fuel) %>%
  summarise(
    companies = n_distinct(parent_entity),
    records = n(),
    total_production_EJ = sum(production_EJ, na.rm = TRUE),
    total_emissions = sum(product_emissions, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(total_production_EJ))

top_companies <- influencemap_final %>%
  group_by(parent_entity) %>%
  summarise(
    total_production_EJ = sum(production_EJ, na.rm = TRUE),
    total_emissions = sum(product_emissions, na.rm = TRUE),
    years_covered = n_distinct(year),
    fuels = paste(unique(Fuel), collapse = ", "),
    .groups = "drop"
  ) %>%
  arrange(desc(total_production_EJ)) %>%
  head(10)

# =============================================================================
# EXPORT
# =============================================================================

export_csv(influencemap_final,  "influencemap_production_EJ.csv",        dir = outputs_fossil_dir)
export_csv(commodity_summary,   "influencemap_commodity_summary_EJ.csv", dir = outputs_fossil_dir)
export_csv(fuel_summary,        "influencemap_fuel_summary_EJ.csv",      dir = outputs_fossil_dir)
export_csv(top_companies,       "influencemap_top_companies_EJ.csv",     dir = outputs_fossil_dir)

cat("\n>> EJ conversion complete!\n\n")
