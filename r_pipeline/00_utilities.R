# =============================================================================
# FILE: r_pipeline/00_utilities.R
# Shared helper functions for the open-asset-data R pipeline.
# Adapted from CarbonBudgetTracker/02.Scripts/00_config/00_utilities.R
# =============================================================================

# Required libraries
suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
  library(stringr)
  library(stringi)
  library(countrycode)
})

# =============================================================================
# COUNTRY NAME CLEANING
# =============================================================================

clean_country_names <- function(country) {
  country <- stringi::stri_trans_general(country, "Latin-ASCII")
  country <- str_replace_all(country, "&", "and")
  country <- str_to_lower(country)
  country <- str_squish(country)
  country <- str_to_title(country)

  ifelse(
    country %in% c("World"),
    country,
    countrycode::countrycode(
      country,
      origin = "country.name",
      destination = "country.name",
      custom_match = c(
        "United States Of America (And Dependencies)" = "United States",
        "China (Including Hong Kong And Macao)" = "China",
        "Dem. People's Republic Of Korea" = "North Korea",
        "Republic Of Korea" = "South Korea",
        "Venezuela (Bolivarian Republic Of)" = "Venezuela",
        "Iran (Islamic Republic Of)" = "Iran",
        "Russian Federation" = "Russia",
        "United Kingdom Of Great Britain And Northern Ireland" = "United Kingdom",
        "Bosnia And Herzegovina" = "Bosnia-Herzegovina",
        "Bosnia and Herzegovina" = "Bosnia-Herzegovina",
        "Bosnia & Herzegovina" = "Bosnia-Herzegovina",
        "Viet Nam" = "Vietnam",
        "Cote D'ivoire" = "Ivory Coast",
        "Trinidad And Tobago" = "Trinidad & Tobago",
        "Turkiye" = "Turkey",
        "D.P.R. Korea" = "North Korea",
        "D.R. Congo" = "Democratic Republic of the Congo",
        "Congo (Zaire)" = "Democratic Republic of the Congo",
        "Taiwan, China" = "Taiwan",
        "Myanmar" = "Burma",
        "Macedonia" = "North Macedonia",
        "Yugoslavia" = "Serbia",
        "Czechia" = "Czech Republic",
        "Montenegro" = "Montenegro",
        "Dem. People's Republic of Korea" = "North Korea"
      )
    )
  )
}

# =============================================================================
# DATA SUMMARY
# =============================================================================

print_data_summary <- function(df, name) {
  cat("\n=================================\n")
  cat("SUMMARY:", name, "\n")
  cat("=================================\n")
  cat("Rows:", nrow(df), "\n")
  if ("company" %in% names(df)) {
    cat("Companies:", n_distinct(df$company), "\n")
  } else if ("parent_entity" %in% names(df)) {
    cat("Companies:", n_distinct(df$parent_entity), "\n")
  } else if ("Company Name" %in% names(df)) {
    cat("Companies:", n_distinct(df$`Company Name`), "\n")
  }
  if ("country" %in% names(df)) {
    cat("Countries:", n_distinct(df$country), "\n")
  }
  if ("year" %in% names(df)) {
    cat("Year range:", min(df$year, na.rm = TRUE), "-", max(df$year, na.rm = TRUE), "\n")
  }
  cat("=================================\n\n")
}

# =============================================================================
# EXPORT HELPERS
# =============================================================================

#' Export a data frame as CSV to a specific output directory
#'
#' @param df Data frame to write
#' @param filename File name (just the basename, not full path)
#' @param dir Output directory (defaults to outputs_dir from config)
#' @param message Optional message to print
export_csv <- function(df, filename, dir = outputs_dir, message = NULL) {
  filepath <- file.path(dir, filename)
  # Ensure parent directory exists
  parent <- dirname(filepath)
  if (!dir.exists(parent)) dir.create(parent, recursive = TRUE)
  readr::write_csv(df, filepath)

  if (!is.null(message)) {
    cat(">>", message, "\n")
  } else {
    cat(">> Exported:", filepath, "\n")
  }
}

# =============================================================================
# ENERGY CONVERSION
# =============================================================================

#' Convert fossil fuel production to EJ (Exajoules)
#'
#' @param data Data frame with columns: commodity, production_value
#' @param conversion_factors Data frame with columns: commodity, conversion_factor
#' @return Data frame with additional column: production_EJ
convert_to_EJ <- function(data, conversion_factors) {
  data %>%
    left_join(
      conversion_factors %>% select(commodity, conversion_factor),
      by = "commodity"
    ) %>%
    mutate(production_EJ = production_value * conversion_factor) %>%
    select(-conversion_factor)
}

# =============================================================================
cat("  Utilities loaded\n")
