# =============================================================================
# FILE: r_pipeline/import/influencemap.R
# Import InfluenceMap fossil fuel company emissions data
# Migrated from CarbonBudgetTracker/02.Scripts/01_import/07_influencemap.R
# =============================================================================

library(readr)
library(dplyr)
library(tidyr)
library(stringr)

cat(">> Importing InfluenceMap fossil fuel data...\n\n")

# =============================================================================
# CONFIGURATION
# =============================================================================

keep_commodities <- c(
  "Oil & NGL",
  "Natural Gas",
  "Sub-Bituminous Coal",
  "Bituminous Coal",
  "Thermal Coal"
)

# =============================================================================
# IMPORT DATA
# =============================================================================

if (!file.exists(influencemap_file)) {
  cat(">> File not found:", influencemap_file, "\n")
  if (dir.exists(InfluenceMap_path)) {
    cat(">> Available CSV files:\n")
    print(list.files(InfluenceMap_path, pattern = "\\.csv$"))
  }
  stop("Please place 'emissions_high_granularity.csv' in the InfluenceMap folder")
}

cat(">> Loading data from:", basename(influencemap_file), "\n\n")

influencemap_raw <- read_csv(influencemap_file, show_col_types = FALSE)

cat(">> Raw data: ", nrow(influencemap_raw), " rows, ", ncol(influencemap_raw), " columns\n\n")

# =============================================================================
# SELECT AND FILTER COLUMNS
# =============================================================================

# Core required columns
required_cols <- c("parent_entity", "year", "commodity", "production_value",
                   "production_unit", "parent_type")

if ("reporting_entity" %in% names(influencemap_raw)) {
  required_cols <- c(required_cols, "reporting_entity")
}

# Handle product_emissions column name variants
emissions_col <- if ("product_emissions_MtCO2" %in% names(influencemap_raw)) {
  "product_emissions_MtCO2"
} else if ("product_emissions" %in% names(influencemap_raw)) {
  "product_emissions"
} else {
  NULL
}

if (!is.null(emissions_col)) {
  required_cols <- c(required_cols, emissions_col)
}

available_cols <- intersect(required_cols, names(influencemap_raw))

influencemap_selected <- influencemap_raw %>%
  select(all_of(available_cols))

if ("product_emissions_MtCO2" %in% names(influencemap_selected)) {
  influencemap_selected <- influencemap_selected %>%
    rename(product_emissions = product_emissions_MtCO2)
}

# Filter to target commodities
if ("commodity" %in% names(influencemap_selected)) {
  influencemap_filtered <- influencemap_selected %>%
    filter(commodity %in% keep_commodities)
  cat(">> Filtered to", nrow(influencemap_filtered), "rows (from", nrow(influencemap_selected), ")\n\n")
} else {
  influencemap_filtered <- influencemap_selected
}

# =============================================================================
# CLEAN AND STANDARDIZE
# =============================================================================

cat(">> Cleaning and standardizing...\n")

influencemap_clean <- influencemap_filtered %>%
  mutate(
    parent_entity = str_trim(parent_entity) %>% str_squish(),
    reporting_entity = if ("reporting_entity" %in% names(.)) {
      str_trim(reporting_entity) %>% str_squish()
    } else NA_character_,
    year = as.integer(year),
    commodity = str_trim(commodity),
    Fuel = case_when(
      commodity %in% c("Sub-Bituminous Coal", "Bituminous Coal", "Thermal Coal") ~ "Coal",
      commodity == "Oil & NGL" ~ "Oil",
      commodity == "Natural Gas" ~ "Gas",
      TRUE ~ NA_character_
    ),
    production_value = as.numeric(production_value),
    production_unit = if ("production_unit" %in% names(.)) str_trim(production_unit) else NA_character_,
    parent_type = if ("parent_type" %in% names(.)) str_trim(parent_type) else NA_character_,
    product_emissions = if ("product_emissions" %in% names(.)) as.numeric(product_emissions) else NA_real_
  ) %>%
  filter(!is.na(parent_entity), !is.na(year), !is.na(commodity))

# =============================================================================
# AGGREGATE TO PARENT LEVEL
# =============================================================================

cat(">> Aggregating to parent entity level...\n")

influencemap_detailed <- influencemap_clean

influencemap_aggregated <- influencemap_clean %>%
  group_by(parent_entity, year, commodity, Fuel, production_unit, parent_type) %>%
  summarise(
    n_reporting_entities = if ("reporting_entity" %in% names(.)) n_distinct(reporting_entity) else NA_integer_,
    production_value = sum(production_value, na.rm = TRUE),
    product_emissions = sum(product_emissions, na.rm = TRUE),
    .groups = "drop"
  )

cat("   Before aggregation:", nrow(influencemap_clean), "rows\n")
cat("   After aggregation:", nrow(influencemap_aggregated), "rows\n")

# Handle any remaining duplicates
remaining_dupes <- influencemap_aggregated %>%
  group_by(parent_entity, year, commodity, Fuel) %>%
  filter(n() > 1) %>%
  ungroup()

if (nrow(remaining_dupes) > 0) {
  influencemap_aggregated <- influencemap_aggregated %>%
    group_by(parent_entity, year, commodity, Fuel, parent_type) %>%
    summarise(
      n_reporting_entities = sum(n_reporting_entities, na.rm = TRUE),
      production_value = sum(production_value, na.rm = TRUE),
      product_emissions = sum(product_emissions, na.rm = TRUE),
      production_unit = paste(unique(production_unit), collapse = "; "),
      .groups = "drop"
    )
}

influencemap_clean <- influencemap_aggregated

# =============================================================================
# SUMMARY
# =============================================================================

cat("\n>> SUMMARY:\n")
cat("   Companies:", n_distinct(influencemap_clean$parent_entity), "\n")
cat("   Years:", min(influencemap_clean$year, na.rm = TRUE), "-",
    max(influencemap_clean$year, na.rm = TRUE), "\n")
cat("   Records:", nrow(influencemap_clean), "\n")
cat("   Total emissions:", round(sum(influencemap_clean$product_emissions, na.rm = TRUE)),
    "Mt CO2\n\n")

# =============================================================================
# EXPORT
# =============================================================================

export_csv(influencemap_clean, "influencemap_emissions_clean.csv", dir = outputs_fossil_dir)

if (exists("influencemap_detailed") && "reporting_entity" %in% names(influencemap_detailed)) {
  export_csv(influencemap_detailed, "influencemap_emissions_detailed.csv", dir = outputs_fossil_dir)
}

cat("\n>> InfluenceMap import complete!\n\n")
