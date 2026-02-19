# =============================================================================
# FILE: r_pipeline/process/nger_forward.R
# Project Australian power companies forward with renewable replacement
# Creates BOTH BAU and TP scenarios
# Migrated from CarbonBudgetTracker/02.Scripts/02_process/05_nger_forward.R
# =============================================================================

library(readr)
library(dplyr)
library(tidyr)

cat(">> Projecting Australian power companies forward (2024-2050)...\n\n")

# =============================================================================
# LOAD DATA (from CSVs produced by earlier pipeline steps)
# =============================================================================

nger_facilities_file <- file.path(outputs_power_dir, "nger_facilities.csv")
nger_corporate_file  <- file.path(outputs_power_dir, "nger_corporate.csv")
closure_file         <- file.path(processed_power_dir, "facility_closure_matching_complete.csv")

if (!file.exists(nger_facilities_file)) {
  stop("nger_facilities.csv not found. Run r_pipeline/import/nger_power.R first.")
}
if (!file.exists(nger_corporate_file)) {
  stop("nger_corporate.csv not found. Run r_pipeline/import/nger_power.R first.")
}
if (!file.exists(closure_file)) {
  stop("facility_closure_matching_complete.csv not found. Run r_pipeline/process/nger_scenarios.R first.")
}

nger_facilities <- read_csv(nger_facilities_file, show_col_types = FALSE)
nger_corporate  <- read_csv(nger_corporate_file, show_col_types = FALSE)
facility_closures_raw <- read_csv(closure_file, show_col_types = FALSE)

# =============================================================================
# HANDLE MULTIPLE CLOSURE DATES
# =============================================================================

# "min"     = Use earliest year (facility closes when first unit closes)
# "max"     = Use latest year (facility operates until last unit closes)
# "median"  = Use middle year
closure_strategy <- "max"

cat(">> Loaded data:\n")
cat("   Closure strategy:", closure_strategy, "\n")

# Apply strategy
facility_closures <- facility_closures_raw %>%
  group_by(company, facility) %>%
  summarise(
    closure_year = case_when(
      closure_strategy == "min" ~ min(closure_year),
      closure_strategy == "max" ~ max(closure_year),
      closure_strategy == "median" ~ as.integer(median(closure_year)),
      TRUE ~ max(closure_year)
    ),
    n_units = n(),
    .groups = "drop"
  )

cat("   Facilities (deduplicated):", nrow(facility_closures), "\n")
cat("   (", sum(facility_closures$n_units > 1), "facilities with multiple units )\n\n")

# =============================================================================
# CALCULATE FACILITY BASELINES (2020-2023)
# =============================================================================

cat(">> Calculating facility baselines (2020-2023)...\n")

facility_baseline <- nger_facilities %>%
  filter(year >= 2020, year <= 2023) %>%
  group_by(company, facility) %>%
  summarise(
    baseline_generation_twh = mean(generation_twh, na.rm = TRUE),
    baseline_emissions_mt = mean(emissions_mt, na.rm = TRUE),
    n_years = n(),
    .groups = "drop"
  )

# Add closure dates
facility_baseline <- facility_baseline %>%
  left_join(facility_closures %>% select(company, facility, closure_year, n_units),
            by = c("company", "facility"))

cat("   Baselines for", nrow(facility_baseline), "facilities\n\n")

# =============================================================================
# CALCULATE COMPANY BASELINES
# =============================================================================

cat(">> Calculating company baselines...\n")

company_baseline <- facility_baseline %>%
  group_by(company) %>%
  summarise(
    baseline_generation_twh = sum(baseline_generation_twh, na.rm = TRUE),
    baseline_emissions_mt = sum(baseline_emissions_mt, na.rm = TRUE),
    n_facilities = n(),
    .groups = "drop"
  )

cat("   Company baselines:\n")
cat("   Total generation:", round(sum(company_baseline$baseline_generation_twh), 1), "TWh\n")
cat("   Total emissions:", round(sum(company_baseline$baseline_emissions_mt), 1), "Mt CO2\n\n")

# =============================================================================
# PROJECT BAU SCENARIO (2024-2050) - No closures
# =============================================================================

cat(">> Projecting BAU scenario (no facility closures)...\n")

projection_years <- 2024:2050

company_projections_bau <- facility_baseline %>%
  crossing(year = projection_years) %>%
  mutate(
    # Generation stays at baseline
    generation_twh = baseline_generation_twh,

    # Emissions stay at baseline (no closures, no renewable replacement)
    emissions_mt = baseline_emissions_mt
  ) %>%
  # Aggregate to company level
  group_by(company, year) %>%
  summarise(
    total_generation_twh = sum(generation_twh, na.rm = TRUE),
    total_emissions_mt = sum(emissions_mt, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    scenario = "bau",
    country = "Australia",
    sector = "Power"
  )

cat("   Projected", nrow(company_projections_bau), "company-years (BAU)\n\n")

# =============================================================================
# PROJECT TP SCENARIO (2024-2050) - With closures and renewable replacement
# =============================================================================

cat(">> Projecting TP scenario (with facility closures)...\n")

company_projections_tp <- facility_baseline %>%
  crossing(year = projection_years) %>%
  mutate(
    # Generation stays at baseline (capacity replaced if closed)
    generation_twh = baseline_generation_twh,

    # Emissions = baseline if operating, 0 if closed (replaced by renewable)
    emissions_mt = if_else(year <= closure_year, baseline_emissions_mt, 0)
  ) %>%
  # Aggregate to company level
  group_by(company, year) %>%
  summarise(
    total_generation_twh = sum(generation_twh, na.rm = TRUE),
    total_emissions_mt = sum(emissions_mt, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    scenario = "tp",
    country = "Australia",
    sector = "Power"
  )

cat("   Projected", nrow(company_projections_tp), "company-years (TP)\n\n")

# =============================================================================
# COMBINE HISTORICAL + PROJECTIONS
# =============================================================================

cat(">> Combining historical + projections...\n")

# Historical (from corporate totals)
historical <- nger_corporate %>%
  transmute(
    company,
    year,
    total_generation_twh = generation_twh,
    total_emissions_mt = emissions_mt,
    scenario = "historical",
    country = "Australia",
    sector = "Power"
  )

# Combined (now includes both BAU and TP)
company_scenarios <- bind_rows(
  historical,
  company_projections_bau,
  company_projections_tp
) %>%
  arrange(company, scenario, year)

cat("   Total records:", nrow(company_scenarios), "\n")
cat("   Scenarios:", paste(unique(company_scenarios$scenario), collapse = ", "), "\n\n")

# =============================================================================
# CREATE ALD FORMAT
# =============================================================================

cat(">> Creating ALD format...\n")

nger_ald_scenarios <- bind_rows(
  # Emissions
  company_scenarios %>%
    mutate(
      Variable = case_when(
        scenario == "historical" ~ "Emissions (Historical)",
        scenario == "bau" ~ "Emissions (BAU)",
        scenario == "tp" ~ "Emissions (TP)",
        TRUE ~ "Emissions"
      ),
      Value = total_emissions_mt,
      Unit = "MtCO2"
    ),
  # Generation
  company_scenarios %>%
    mutate(
      Variable = case_when(
        scenario == "historical" ~ "Generation (Historical)",
        scenario == "bau" ~ "Generation (BAU)",
        scenario == "tp" ~ "Generation (TP)",
        TRUE ~ "Generation"
      ),
      Value = total_generation_twh,
      Unit = "TWh"
    )
) %>%
  transmute(
    `Company Name` = company,
    Country = country,
    Year = year,
    Variable,
    Value,
    Unit
  ) %>%
  filter(!is.na(`Company Name`), !is.na(Value))

cat("   ALD records:", nrow(nger_ald_scenarios), "\n\n")

# =============================================================================
# SUMMARY STATISTICS
# =============================================================================

cat(">> PROJECTION SUMMARY:\n")

summary_years <- c(2023, 2030, 2040, 2050)

# TP summary
transition_summary_tp <- company_scenarios %>%
  filter(year %in% summary_years, scenario == "tp") %>%
  group_by(year) %>%
  summarise(
    scenario = "TP",
    total_generation = sum(total_generation_twh, na.rm = TRUE),
    total_emissions = sum(total_emissions_mt, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(avg_intensity = total_emissions / total_generation)

# BAU summary
transition_summary_bau <- company_scenarios %>%
  filter(year %in% summary_years, scenario == "bau") %>%
  group_by(year) %>%
  summarise(
    scenario = "BAU",
    total_generation = sum(total_generation_twh, na.rm = TRUE),
    total_emissions = sum(total_emissions_mt, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(avg_intensity = total_emissions / total_generation)

# Combined summary
transition_summary <- bind_rows(transition_summary_tp, transition_summary_bau) %>%
  arrange(year, scenario)

print(transition_summary)

baseline_emissions <- company_scenarios %>%
  filter(year == 2023, scenario == "historical") %>%
  summarise(total = sum(total_emissions_mt, na.rm = TRUE)) %>%
  pull(total)

tp_2050_emissions <- transition_summary_tp %>% filter(year == 2050) %>% pull(total_emissions)
bau_2050_emissions <- transition_summary_bau %>% filter(year == 2050) %>% pull(total_emissions)

cat("\n>> Key Results:\n")
cat("   Closure strategy:", closure_strategy, "\n")
cat("   2023 baseline emissions:", round(baseline_emissions, 1), "Mt CO2\n")
cat("\n   BAU Scenario:\n")
cat("     2050 emissions:", round(bau_2050_emissions, 1), "Mt CO2\n")
cat("     Change from 2023:", round(100 * (bau_2050_emissions / baseline_emissions - 1), 1), "%\n")
cat("\n   TP Scenario:\n")
cat("     2050 emissions:", round(tp_2050_emissions, 1), "Mt CO2\n")
cat("     Reduction from 2023:", round(100 * (1 - tp_2050_emissions / baseline_emissions), 1), "%\n")
cat("     Reduction vs BAU:", round(100 * (1 - tp_2050_emissions / bau_2050_emissions), 1), "%\n")
cat("\n   Generation maintained throughout in both scenarios\n\n")

# =============================================================================
# EXPORT
# =============================================================================

export_csv(company_scenarios, "nger_company_scenarios_2013_2050.csv", dir = outputs_power_dir)
export_csv(nger_ald_scenarios, "nger_ald_scenarios_complete.csv", dir = outputs_power_dir)
export_csv(transition_summary, "nger_transition_summary.csv", dir = outputs_power_dir)

cat("\n>> NGER forward projections complete!\n")
cat("   Scenarios created: BAU and TP\n")
cat("   Output: nger_ald_scenarios_complete.csv\n")
cat("   Ready to integrate with PowerALD analysis!\n\n")
