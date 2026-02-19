# =============================================================================
# FILE: r_pipeline/import/gem_reserves.R
# Import GEM reserves data and match to InfluenceMap companies
# Migrated from CarbonBudgetTracker/02.Scripts/01_import/08_gem_reserves.R
# =============================================================================

library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(readr)

cat(">> Importing GEM reserves data (Oil, Gas, Coal)...\n\n")

# =============================================================================
# CONFIGURATION
# =============================================================================

goget_file <- gem_goget_file
gcmt_file  <- gem_gcmt_file

has_goget <- file.exists(goget_file)
has_gcmt  <- file.exists(gcmt_file)

if (!has_goget && !has_gcmt) {
  stop("No GEM files found in: ", GEM_path)
}

cat(">> GEM files found:\n")
if (has_goget) cat("  + GOGET (Oil & Gas):", basename(goget_file), "\n")
if (has_gcmt)  cat("  + GCMT (Coal):", basename(gcmt_file), "\n")
cat("\n")

# =============================================================================
# HELPER: Parse ownership strings
# =============================================================================

parse_ownership <- function(ownership_str) {
  if (is.na(ownership_str) || ownership_str == "") {
    return(tibble(company = character(), pct = numeric()))
  }
  parts <- str_split(ownership_str, ";")[[1]]
  results <- lapply(parts, function(part) {
    part <- str_trim(part)
    if (part == "" || part == "Other" || part == "other") return(NULL)
    match <- str_match(part, "^(.+?)\\s*\\((\\d+(?:\\.\\d+)?)\\s*%?\\)$")
    if (!is.na(match[1, 1])) {
      return(tibble(company = str_trim(match[1, 2]), pct = as.numeric(match[1, 3]) / 100))
    } else {
      match2 <- str_match(part, "^(.+?)\\s*\\(.*\\)$")
      if (!is.na(match2[1, 1])) {
        return(tibble(company = str_trim(match2[1, 2]), pct = NA_real_))
      }
    }
    return(NULL)
  })
  bind_rows(results)
}

# =============================================================================
# PART 1: PROCESS GOGET (Oil & Gas)
# =============================================================================

if (has_goget) {
  cat(strrep("=", 60), "\n")
  cat("PROCESSING GOGET (Oil & Gas)\n")
  cat(strrep("=", 60), "\n\n")

  goget_main <- read_excel(goget_file, sheet = "Main data")
  goget_prod <- read_excel(goget_file, sheet = "Production & reserves")
  cat("  Main data:", nrow(goget_main), "units\n")
  cat("  Production & reserves:", nrow(goget_prod), "records\n\n")

  cat("  Parsing ownership...\n")
  goget_main$parsed_owners <- lapply(goget_main$Parent, parse_ownership)
  has_ownership <- sapply(goget_main$parsed_owners, nrow) > 0
  cat("  Units with ownership:", sum(has_ownership), "/", nrow(goget_main), "\n\n")

  goget_merged <- goget_prod %>%
    left_join(goget_main %>% select(`Unit ID`, parsed_owners, Status), by = "Unit ID")

  cat("  Expanding to company level...\n")
  goget_rows <- list()
  for (i in 1:nrow(goget_merged)) {
    row <- goget_merged[i, ]
    owners <- row$parsed_owners[[1]]
    if (nrow(owners) == 0) next
    known_pcts <- owners$pct[!is.na(owners$pct)]
    total_known <- sum(known_pcts)
    n_unknown <- sum(is.na(owners$pct))
    for (j in 1:nrow(owners)) {
      pct <- owners$pct[j]
      if (is.na(pct)) {
        pct <- if (n_unknown > 0 && total_known < 1) (1 - total_known) / n_unknown else 0
      }
      quantity <- if_else(!is.na(row$`Quantity (converted)`), row$`Quantity (converted)`, 0)
      goget_rows[[length(goget_rows) + 1]] <- tibble(
        company = owners$company[j],
        type = row$`Production/reserves`,
        fuel_desc = row$`Fuel description`,
        quantity_attributed = quantity * pct,
        units = row$`Units (converted)`
      )
    }
  }

  goget_company <- bind_rows(goget_rows) %>%
    mutate(
      fuel = case_when(
        str_detect(tolower(fuel_desc), "oil|crude|condensate|ngl|lpg|liquid") ~ "Oil",
        str_detect(tolower(fuel_desc), "gas") ~ "Gas",
        TRUE ~ "Other"
      ),
      quantity_boe = case_when(
        str_detect(tolower(units), "boe") ~ quantity_attributed,
        str_detect(tolower(units), "bbl") ~ quantity_attributed,
        str_detect(tolower(units), "m\u00b3") ~ quantity_attributed * 0.006289,
        TRUE ~ quantity_attributed
      )
    ) %>%
    filter(fuel %in% c("Oil", "Gas"))

  goget_production <- goget_company %>%
    filter(type == "production") %>%
    group_by(company, fuel) %>%
    summarise(production_boe_annual = sum(quantity_boe, na.rm = TRUE), .groups = "drop")

  goget_reserves <- goget_company %>%
    filter(type == "reserves") %>%
    group_by(company, fuel) %>%
    summarise(reserves_boe = sum(quantity_boe, na.rm = TRUE), .groups = "drop")

  goget_summary <- goget_production %>%
    full_join(goget_reserves, by = c("company", "fuel")) %>%
    mutate(years_of_production = reserves_boe / production_boe_annual)

  cat("  Oil/Gas companies:", n_distinct(goget_summary$company), "\n\n")
} else {
  goget_summary <- tibble()
}

# =============================================================================
# PART 2: PROCESS GCMT (Coal)
# =============================================================================

if (has_gcmt) {
  cat(strrep("=", 60), "\n")
  cat("PROCESSING GCMT (Coal)\n")
  cat(strrep("=", 60), "\n\n")

  gcmt_sheets <- excel_sheets(gcmt_file)
  main_sheet <- gcmt_sheets[str_detect(gcmt_sheets, "Non-closed|Main")][1]
  if (is.na(main_sheet)) main_sheet <- gcmt_sheets[2]

  gcmt_main <- read_excel(gcmt_file, sheet = main_sheet)
  cat("  Loaded sheet '", main_sheet, "':", nrow(gcmt_main), "mines\n\n")

  gcmt_main$reserves_mt  <- as.numeric(gcmt_main$`Total Reserves (Proven and Probable, Mt)`)
  gcmt_main$capacity_mtpa <- as.numeric(gcmt_main$`Capacity (Mtpa)`)

  parse_coal_ownership <- function(owner_str) {
    if (is.na(owner_str) || owner_str == "-" || owner_str == "") {
      return(tibble(company = character(), pct = numeric()))
    }
    parts <- str_split(as.character(owner_str), ";")[[1]]
    results <- list()
    for (part in parts) {
      part <- str_trim(part)
      if (part == "" || str_detect(tolower(part), "^others|^unknown|^small shareholder")) next
      pct_match <- str_match(part, "\\[(\\d+(?:\\.\\d+)?)\\s*%?\\]")
      if (!is.na(pct_match[1, 1])) {
        pct <- as.numeric(pct_match[1, 2]) / 100
        company <- str_trim(str_replace_all(part, "\\s*\\[.*?\\]", ""))
      } else {
        pct <- NA_real_
        company <- str_trim(part)
      }
      if (company != "") results[[length(results) + 1]] <- tibble(company = company, pct = pct)
    }
    bind_rows(results)
  }

  cat("  Parsing ownership...\n")
  gcmt_main$parsed_owners <- lapply(gcmt_main$`Parent Company`, parse_coal_ownership)
  has_ownership <- sapply(gcmt_main$parsed_owners, nrow) > 0
  cat("  Mines with ownership:", sum(has_ownership), "/", nrow(gcmt_main), "\n")

  cat("  Expanding to company level...\n")
  gcmt_rows <- list()
  for (i in 1:nrow(gcmt_main)) {
    row <- gcmt_main[i, ]
    owners <- row$parsed_owners[[1]]
    if (nrow(owners) == 0) next
    known_pcts <- owners$pct[!is.na(owners$pct)]
    total_known <- sum(known_pcts)
    n_unknown <- sum(is.na(owners$pct))
    for (j in 1:nrow(owners)) {
      pct <- owners$pct[j]
      if (is.na(pct)) pct <- if (n_unknown > 0 && total_known < 1) (1 - total_known) / n_unknown else 1.0
      gcmt_rows[[length(gcmt_rows) + 1]] <- tibble(
        company = owners$company[j],
        reserves_mt = if_else(!is.na(row$reserves_mt), row$reserves_mt * pct, NA_real_),
        capacity_mtpa = if_else(!is.na(row$capacity_mtpa), row$capacity_mtpa * pct, NA_real_)
      )
    }
  }

  gcmt_summary <- bind_rows(gcmt_rows) %>%
    group_by(company) %>%
    summarise(reserves_mt = sum(reserves_mt, na.rm = TRUE),
              capacity_mtpa = sum(capacity_mtpa, na.rm = TRUE), .groups = "drop") %>%
    filter(reserves_mt > 0 | capacity_mtpa > 0) %>%
    mutate(fuel = "Coal",
           reserves_boe = reserves_mt * 4.5,
           production_boe_annual = capacity_mtpa * 4.5,
           years_of_production = reserves_mt / capacity_mtpa) %>%
    select(company, fuel, production_boe_annual, reserves_boe, years_of_production,
           reserves_mt, capacity_mtpa)

  cat("  Coal companies:", n_distinct(gcmt_summary$company), "\n\n")
} else {
  gcmt_summary <- tibble()
}

# =============================================================================
# PART 3: COMBINE ALL FUELS
# =============================================================================

cat(strrep("=", 60), "\n")
cat("COMBINING ALL FUELS\n")
cat(strrep("=", 60), "\n\n")

if (nrow(goget_summary) > 0) {
  goget_summary <- goget_summary %>% mutate(reserves_mt = NA_real_, capacity_mtpa = NA_real_)
}
if (nrow(gcmt_summary) > 0) {
  gcmt_summary <- gcmt_summary %>%
    select(company, fuel, production_boe_annual, reserves_boe, years_of_production, reserves_mt, capacity_mtpa)
}

gem_summary <- bind_rows(goget_summary, gcmt_summary)
cat("  Total company-fuel combinations:", nrow(gem_summary), "\n\n")

# =============================================================================
# PART 4: MATCH TO INFLUENCEMAP COMPANIES
# =============================================================================

cat(strrep("=", 60), "\n")
cat("MATCHING TO INFLUENCEMAP\n")
cat(strrep("=", 60), "\n\n")

# Load InfluenceMap data (CSV from r_pipeline/import/influencemap.R)
im_clean_file <- file.path(outputs_fossil_dir, "influencemap_emissions_clean.csv")
if (!file.exists(im_clean_file)) {
  stop("influencemap_emissions_clean.csv not found. Run r_pipeline/import/influencemap.R first.")
}
im_data <- read_csv(im_clean_file, show_col_types = FALSE)
im_companies <- unique(im_data$parent_entity)

cat("  InfluenceMap companies:", length(im_companies), "\n")

# Manual matching table (verified matches)
manual_matches <- tribble(
  ~influencemap_name, ~gem_name,
  "ExxonMobil", "ExxonMobil",
  "Shell", "Shell plc",
  "BP", "BP P.L.C.",
  "BP", "BP",
  "Chevron", "Chevron",
  "TotalEnergies", "TotalEnergies",
  "ConocoPhillips", "ConocoPhillips",
  "Eni", "Eni S.P.A.",
  "Saudi Aramco", "Saudi Aramco",
  "Abu Dhabi National Oil Company", "Abu Dhabi National Oil Company",
  "Kuwait Petroleum Corp.", "Kuwait Petroleum Corporation",
  "QatarEnergy", "QatarEnergy",
  "National Iranian Oil Company", "National Iranian Oil Company",
  "Iraq National Oil Company", "Basra Oil Company",
  "Gazprom", "Gazprom",
  "Rosneft", "Rosneft",
  "Lukoil", "PJSC LUKOIL",
  "Surgutneftegas", "Surgutneftegas PJSC",
  "Tatneft", "PJSC Tatneft",
  "Novatek", "Novatek",
  "Petrobras", "Petr\u00f3leo Brasileiro S.A.",
  "Pemex", "Petroleos Mexicanos",
  "Petroleos de Venezuela", "PDVSA",
  "Ecopetrol", "Ecopetrol S.A.",
  "CNPC", "China National Petroleum Corporation",
  "CNPC", "PetroChina",
  "CNOOC", "CNOOC Limited",
  "Sinopec", "SINOPEC",
  "Petronas", "Petroliam Nasional Berhad (Petronas)",
  "ONGC", "Oil and Natural Gas Corporation (ONGC)",
  "Sonatrach", "Sonatrach SPA",
  "Sonangol", "Sociedade Nacional de Combust\u00edveis de Angola E.P",
  "Nigerian National Petroleum Corp.", "Nigerian National Petroleum Corporation",
  "Libya National Oil Corp.", "National Oil Corporation (Libya)",
  "PTTEP", "PTT PLC",
  "Pertamina", "Pertamina",
  "Egyptian General Petroleum", "Egyptian General Petroleum Corporation",
  "INPEX", "INPEX Corporation",
  "Equinor", "Equinor ASA",
  "Repsol", "Repsol SA",
  "OMV", "OMV Aktiengesellschaft",
  "Petoro", "Petoro AS",
  "Occidental Petroleum", "Occidental Petroleum Corporation",
  "EOG Resources", "EOG Resources",
  "Devon Energy", "Devon Energy Corporation",
  "APA Corporation", "APA Corporation",
  "Marathon Oil", "Marathon Oil",
  "Hess Corporation", "Hess Corporation",
  "Antero Resources", "Antero Resources",
  "Murphy Oil", "Murphy Oil Corporation",
  "Ovintiv", "Ovintiv Inc.",
  "Canadian Natural Resources", "Canadian Natural Resources",
  "Suncor Energy", "Suncor Energy Inc.",
  "Cenovus Energy", "Cenovus Energy",
  "CNX Resources", "CNX Resources Corporation",
  "EQT Corporation", "EQT Corporation",
  "Coterra Energy", "Coterra Energy Inc.",
  "Woodside Energy", "Woodside Energy Group",
  "Santos", "Santos Limited",
  "Sasol", "Sasol",
  "Coal India", "Coal India Ltd",
  "Peabody Energy", "Peabody Energy Corp",
  "Glencore", "Glencore PLC",
  "BHP", "BHP Group Ltd",
  "Anglo American", "Anglo American PLC",
  "Alpha Metallurgical Resources", "Alpha Metallurgical Resources Inc",
  "Whitehaven Coal", "Whitehaven Coal Ltd",
  "Adani Group", "Adani Enterprises Ltd",
  "Bumi Resources", "PT Bumi Resources Tbk",
  "Banpu", "Banpu PCL",
  "Banpu", "Banpu Power PCL",
  "Exxaro Resources Ltd", "Exxaro Resources Ltd",
  "Shandong Energy", "Shandong Energy",
  "Shanxi Coking Coal Group", "Shanxi Coking Coal Group",
  "China National Coal Group", "China National Coal Group Corporation",
  "Shaanxi Coal and Chemical Industry Group", "Shaanxi Coal and Chemical Industry Group",
  "CHN Energy", "China Energy",
  "Huayang New Material Technology Group", "Huayang New Material Technology Group",
  "Jinneng Group", "Jinneng Group",
  "China Huaneng Group", "China Huaneng"
)

valid_matches <- manual_matches %>%
  filter(influencemap_name %in% im_companies, gem_name %in% gem_summary$company)

cat("  Valid matches:", n_distinct(valid_matches$influencemap_name), "companies\n\n")

# =============================================================================
# MERGE RESERVES
# =============================================================================

influencemap_reserves <- valid_matches %>%
  left_join(gem_summary, by = c("gem_name" = "company")) %>%
  group_by(parent_entity = influencemap_name, Fuel = fuel) %>%
  summarise(
    gem_companies_matched = paste(unique(gem_name), collapse = "; "),
    production_boe_annual = sum(production_boe_annual, na.rm = TRUE),
    reserves_boe = sum(reserves_boe, na.rm = TRUE),
    reserves_mt = sum(reserves_mt, na.rm = TRUE),
    capacity_mtpa = sum(capacity_mtpa, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(years_of_production = round(reserves_boe / production_boe_annual, 1)) %>%
  filter(reserves_boe > 0 | production_boe_annual > 0)

cat("  Companies with reserves:", n_distinct(influencemap_reserves$parent_entity), "\n\n")

# Unmatched companies
all_ff_im <- im_data %>%
  filter(commodity %in% c("Oil & NGL", "Natural Gas", "Bituminous Coal",
                          "Sub-Bituminous Coal", "Thermal Coal", "Anthracite Coal",
                          "Lignite Coal", "Metallurgical Coal")) %>%
  pull(parent_entity) %>% unique()
unmatched <- setdiff(all_ff_im, valid_matches$influencemap_name)

# =============================================================================
# EXPORT
# =============================================================================

export_csv(influencemap_reserves, "influencemap_reserves_final.csv", dir = outputs_fossil_dir)
export_csv(gem_summary,           "gem_company_summary.csv",         dir = outputs_fossil_dir)
export_csv(valid_matches,         "gem_influencemap_matches.csv",    dir = outputs_fossil_dir)
export_csv(tibble(parent_entity = unmatched, matched = FALSE),
           "gem_unmatched_companies.csv", dir = outputs_fossil_dir)

cat("\n>> GEM import complete!\n")
cat("   Oil:", sum(influencemap_reserves$Fuel == "Oil"), "companies\n")
cat("   Gas:", sum(influencemap_reserves$Fuel == "Gas"), "companies\n")
cat("   Coal:", sum(influencemap_reserves$Fuel == "Coal"), "companies\n")
cat("   Unmatched:", length(unmatched), "\n\n")
