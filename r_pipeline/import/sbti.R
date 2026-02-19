# =============================================================================
# FILE: r_pipeline/import/sbti.R
# Import SBTi (Science Based Targets initiative) company targets
# Migrated from CarbonBudgetTracker/02.Scripts/01_import/10_sbti.R
# =============================================================================
#
# This script handles the IMPORT and MATCHING portion only.
# Trajectory generation (which needs PCP base-year emissions from CBT analysis)
# stays in CarbonBudgetTracker.
#
# INPUT:  data/raw/ForwardLooking/SBTi/companies-excel.xlsx
# OUTPUT: data/processed/sbti/sbti_matched_targets.csv
# =============================================================================

cat(">> Loading SBTi targets...\n")

library(readxl)
library(dplyr)
library(tidyr)
library(stringr)

# =============================================================================
# LOAD SBTI DATA
# =============================================================================

if (!file.exists(sbti_companies_file)) {
  cat("  >> SBTi file not found at:", sbti_companies_file, "\n")
  cat("  >> Writing empty output\n")
  write_csv(tibble(), file.path(processed_sbti_dir, "sbti_matched_targets.csv"))
} else {

sbti_raw <- read_excel(sbti_companies_file)
cat("  Loaded", nrow(sbti_raw), "SBTi companies\n")

# =============================================================================
# COMPANY MATCHING
# =============================================================================
# Manual matching table: pipeline company name -> SBTi company_name

sbti_match_table <- tibble::tribble(
  ~pipeline_company,    ~sbti_company_name,             ~pipeline_sector,
  # Steel
  "SSAB",               "SSAB",                          "steel",
  "ThyssenKrupp",       "thyssenkrupp Steel Europe AG",  "steel",
  # Power
  "EDF",                "EDF Group",                     "power",
  "EnelSpA",            "Enel SpA",                      "power",
  "Engie",              "ENGIE",                         "power",
  "Iberdrola",          "Iberdrola SA",                  "power",
  # Cement
  "Ambuja Cements Ltd", "Ambuja Cements Ltd",            "cement",
  "Buzzi SpA",          "Buzzi SpA",                     "cement",
  "Imerys SA",          "Imerys SA",                     "cement",
  "Shree Cement Ltd",   "Shree Cement Ltd.",             "cement",
  "CRH PLC",            "CRH plc",                      "cement"
)

# Join SBTi data to our match table
sbti_matched <- sbti_match_table %>%
  left_join(
    sbti_raw %>% select(
      company_name,
      near_term_target_classification,
      near_term_target_year,
      long_term_target_classification,
      long_term_target_year,
      net_zero_year
    ),
    by = c("sbti_company_name" = "company_name")
  )

# Parse year columns (handle "FY2030" format)
parse_sbti_year <- function(x) {
  x <- as.character(x)
  x <- str_remove(x, "^FY")
  x <- str_remove(x, "\\.0$")
  as.integer(x)
}

sbti_matched <- sbti_matched %>%
  mutate(
    nt_year  = parse_sbti_year(near_term_target_year),
    lt_year  = parse_sbti_year(long_term_target_year),
    nz_year  = parse_sbti_year(net_zero_year),
    nt_class = near_term_target_classification,
    lt_class = long_term_target_classification
  )

cat("  Matched", nrow(sbti_matched), "companies to SBTi targets:\n")
for (i in 1:nrow(sbti_matched)) {
  cat("    ", sbti_matched$pipeline_company[i], " (", sbti_matched$pipeline_sector[i],
      "): NT=", sbti_matched$nt_class[i], " ", sbti_matched$nt_year[i],
      ", LT=", sbti_matched$lt_class[i], " ", sbti_matched$lt_year[i],
      ", NZ=", sbti_matched$nz_year[i], "\n", sep = "")
}

# =============================================================================
# EXPORT matched targets (trajectory generation happens in CBT)
# =============================================================================

export_csv(sbti_matched, "sbti_matched_targets.csv", dir = processed_sbti_dir)

cat("\n>> SBTi import complete\n")
cat("   NOTE: Trajectory generation (using base-year emissions) runs in CBT\n\n")

} # end file.exists check
