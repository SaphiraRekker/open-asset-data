# =============================================================================
# FILE: r_pipeline/import/nger_power.R
# Import NGER (Australian National Greenhouse & Energy Reporting) power data
# Migrated from CarbonBudgetTracker/02.Scripts/01_import/06_nger_power.R
# =============================================================================

library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)

cat(">> Importing NGER power data...\n\n")

# =============================================================================
# CONFIG
# =============================================================================

nger_folder  <- AU_NGER_path
mapping_file <- nger_mapping_file

# =============================================================================
# FUNCTIONS
# =============================================================================

extract_year <- function(filepath) {
  filename <- basename(filepath)
  year_match <- str_extract(filename, "\\d{4}")
  as.integer(year_match)
}

standardize_name <- function(x) {
  x %>%
    str_to_upper() %>%
    str_replace_all("\\s+", " ") %>%
    str_replace_all("PTY\\.?\\s*LTD\\.?", "PTY LTD") %>%
    str_replace_all("\\bLIMITED\\b", "LTD") %>%
    str_replace_all("\\(.*?\\)", "") %>%
    str_replace_all("[^A-Z0-9 ]", "") %>%
    str_trim()
}

clean_numeric <- function(x) {
  x %>%
    str_trim() %>%
    str_replace_all(",", "") %>%
    str_replace_all("[^0-9.-]", "") %>%
    as.numeric()
}

get_col <- function(df, patterns) {
  for (pattern in patterns) {
    matches <- grep(pattern, names(df), ignore.case = TRUE, value = TRUE)
    if (length(matches) > 0) return(df[[matches[1]]])
  }
  return(rep(NA_character_, nrow(df)))
}

# =============================================================================
# IMPORT
# =============================================================================

csv_files <- list.files(nger_folder, pattern = "greenhouse.*\\.csv$", full.names = TRUE)
cat("Found", length(csv_files), "files\n\n")

all_data <- map_dfr(csv_files, function(file) {
  year <- extract_year(file)
  cat("Reading:", basename(file), "\n")

  df <- read_csv(file, show_col_types = FALSE, col_types = cols(.default = "c"))

  tibble(
    company_raw    = get_col(df, c("Reporting Entity", "Controlling Corporation")),
    facility       = get_col(df, c("Facility Name", "Facility")),
    type           = get_col(df, c("^Type$")),
    generation_mwh = get_col(df, c("Electricity Production \\(MWh\\)", "Electricity production")),
    emissions_tco2 = get_col(df, c(
      "Total Scope 1 Emissions",
      "Greenhouse Gas Emissions Scope 1",
      "Scope 1 \\(t CO2"
    )),
    year = year
  )
})

cat("\n>> Loaded", nrow(all_data), "records\n")

if (nrow(all_data) == 0) {
  cat(">> No NGER files found, writing empty outputs\n")
  write_csv(tibble(), file.path(outputs_power_dir, "nger_facilities.csv"))
  write_csv(tibble(), file.path(outputs_power_dir, "nger_corporate.csv"))
  write_csv(tibble(), file.path(outputs_power_dir, "nger_ald_historical.csv"))
  cat("\n>> Done (no data)\n\n")
} else {

# =============================================================================
# CLEAN
# =============================================================================

cat("\n>> Cleaning data...\n")

nger_clean <- all_data %>%
  mutate(
    generation_mwh = clean_numeric(generation_mwh),
    emissions_tco2 = clean_numeric(emissions_tco2),
    generation_twh = generation_mwh / 1e6,
    emissions_mt   = emissions_tco2 / 1e6
  )

cat("   Cleaned", nrow(nger_clean), "records\n")

# =============================================================================
# LOAD MAPPINGS
# =============================================================================

if (file.exists(mapping_file)) {
  company_mapping <- read_csv(mapping_file, show_col_types = FALSE)
  if ("original_name" %in% names(company_mapping)) {
    company_mapping <- company_mapping %>%
      mutate(original_name = standardize_name(original_name))
    mapping_lookup <- setNames(company_mapping$canonical_name, company_mapping$original_name)
  } else if ("nger_name" %in% names(company_mapping)) {
    company_mapping <- company_mapping %>%
      mutate(nger_name_std = standardize_name(nger_name))
    mapping_lookup <- setNames(company_mapping$excel_name, company_mapping$nger_name_std)
  } else {
    mapping_lookup <- c()
  }
  cat("   Loaded", length(mapping_lookup), "company/facility mappings\n")
} else {
  mapping_lookup <- c()
  cat("   No mapping file found\n")
}

nger_clean <- nger_clean %>%
  mutate(
    company_std = standardize_name(company_raw),
    company = if_else(
      company_std %in% names(mapping_lookup) & !is.na(company_std),
      mapping_lookup[company_std],
      company_raw
    )
  )

# =============================================================================
# SPLIT: FACILITIES vs CORPORATE TOTALS
# =============================================================================

nger_facilities <- nger_clean %>%
  filter(type == "F", !is.na(facility), !is.na(company)) %>%
  group_by(company, facility, year) %>%
  slice(1) %>%
  ungroup() %>%
  select(company, facility, year, generation_twh, emissions_mt)

nger_corporate <- nger_clean %>%
  filter(type == "C", !is.na(company)) %>%
  select(company, year, generation_twh, emissions_mt)

cat("\n>> Results:\n")
cat("   Facilities:", nrow(nger_facilities), "records\n")
cat("   Corporate totals:", nrow(nger_corporate), "records\n")
cat("   Companies:", n_distinct(nger_corporate$company), "\n")

# =============================================================================
# CREATE ALD FORMAT
# =============================================================================

nger_ald <- bind_rows(
  nger_corporate %>% mutate(Variable = "Emissions (Historical)", Value = emissions_mt, Unit = "MtCO2"),
  nger_corporate %>% mutate(Variable = "Generation (Historical)", Value = generation_twh, Unit = "TWh")
) %>%
  transmute(
    `Company Name` = company,
    Country = "Australia",
    Year = year,
    Variable,
    Value,
    Unit
  ) %>%
  filter(!is.na(`Company Name`), !is.na(Value))

# =============================================================================
# EXPORT CSVs
# =============================================================================

export_csv(nger_facilities, "nger_facilities.csv",       dir = outputs_power_dir)
export_csv(nger_corporate,  "nger_corporate.csv",        dir = outputs_power_dir)
export_csv(nger_ald,        "nger_ald_historical.csv",   dir = outputs_power_dir)

cat("\n>> Done!\n")
cat("   Facilities:", n_distinct(nger_facilities$facility), "\n")
cat("   Companies:", n_distinct(nger_corporate$company), "\n")
cat("   Years:", paste(range(nger_clean$year, na.rm = TRUE), collapse = "-"), "\n\n")

} # end if (nrow(all_data) > 0)
