#!/bin/bash
# Download verified steel company reports - URLs confirmed via web scraping
# Created: 2026-02-13
# All URLs verified working (HTTP 200) unless noted

set -e
BASE="/Users/uqsrekke/Library/CloudStorage/OneDrive-TheUniversityofQueensland/Documents/GitHub/open-asset-data/data/raw/AnnualReports/steel"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
LOG_FILE="$(dirname "$0")/download_verified_log.txt"

download() {
  local dir="$1" file="$2" url="$3"
  local target_dir="$BASE/$dir"
  mkdir -p "$target_dir"
  local target="$target_dir/$file"

  # Skip if already exists and > 10KB
  if [ -f "$target" ] && [ $(stat -f%z "$target" 2>/dev/null || echo 0) -gt 10240 ]; then
    echo "  SKIP (exists): $file" | tee -a "$LOG_FILE"
    return 0
  fi

  local http_code
  http_code=$(curl -s -L -A "$UA" -o "$target" -w "%{http_code}" --connect-timeout 30 --max-time 300 "$url" 2>/dev/null)
  local size=$(stat -f%z "$target" 2>/dev/null || echo 0)

  if [ "$http_code" = "200" ] && [ "$size" -gt 10240 ]; then
    local mb=$(echo "scale=1; $size/1048576" | bc)
    echo "  OK (${mb} MB): $file" | tee -a "$LOG_FILE"
  else
    echo "  FAIL (HTTP $http_code, ${size}B): $file <- $url" | tee -a "$LOG_FILE"
    # Remove failed downloads
    [ "$size" -lt 10240 ] && rm -f "$target"
  fi
  sleep 1  # Be polite
}

echo "============================================" | tee "$LOG_FILE"
echo "Verified Report Downloader" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "============================================" | tee -a "$LOG_FILE"

# === NUCOR ===
echo "" | tee -a "$LOG_FILE"
echo "=== NUCOR ===" | tee -a "$LOG_FILE"
download "nucor" "nucor-sustainability-report-2024.pdf" \
  "https://downloads.ctfassets.net/aax1cfbwhqog/1sjMqwvmB35fkCkKEzKFpi/f0daf98fed1096251b31909841f8022c/2024_Sustainability_Report.pdf"
download "nucor" "nucor-net-zero-2050-info.pdf" \
  "https://assets.ctfassets.net/aax1cfbwhqog/2XqlpNgcZ8X78M74kL9yDj/0bb50b9c3f83f4aa9ab7152e68833c5e/Net_Zero_by_2050_Additional_Information.pdf"

# === CLEVELAND-CLIFFS ===
echo "" | tee -a "$LOG_FILE"
echo "=== CLEVELAND-CLIFFS ===" | tee -a "$LOG_FILE"
download "cleveland_cliffs" "clf-sustainability-report-2024.pdf" \
  "https://d1io3yog0oux5.cloudfront.net/_a8588941e01003fc1297d48ea77754c9/clevelandcliffs/files/pages/clevelandcliffs/db/1149/description/2025+Documents/CLF_SustainabilityReport_2024.pdf"
download "cleveland_cliffs" "clf-sustainability-report-2023.pdf" \
  "https://d1io3yog0oux5.cloudfront.net/_d01051743e0d62e2ce7b37056f2a07b3/clevelandcliffs/db/1188/11744/file/CLF_SustainabilityReport_Spreads_042023.pdf"
download "cleveland_cliffs" "clf-tcfd-report-2022.pdf" \
  "https://d1io3yog0oux5.cloudfront.net/_d01051743e0d62e2ce7b37056f2a07b3/clevelandcliffs/db/1188/11541/file/CLF_Sustainability_TFCDReport_2022.pdf"
download "cleveland_cliffs" "clf-annual-report-2024.pdf" \
  "https://d1io3yog0oux5.cloudfront.net/_d01051743e0d62e2ce7b37056f2a07b3/clevelandcliffs/db/1188/11948/file/Cleveland_Cliffs+Annual+Report+2024.pdf"
download "cleveland_cliffs" "clf-annual-report-2023.pdf" \
  "https://d1io3yog0oux5.cloudfront.net/_d01051743e0d62e2ce7b37056f2a07b3/clevelandcliffs/db/1188/11748/file/CLF_AnnualReport_2023.pdf"
download "cleveland_cliffs" "clf-sustainability-report-2021.pdf" \
  "https://d1io3yog0oux5.cloudfront.net/_d01051743e0d62e2ce7b37056f2a07b3/clevelandcliffs/db/1188/11273/file/CLF_Report_Sustainability_2021_SinglePages.pdf"
download "cleveland_cliffs" "clf-sustainability-report-2020.pdf" \
  "https://d1io3yog0oux5.cloudfront.net/_d01051743e0d62e2ce7b37056f2a07b3/clevelandcliffs/db/1188/11066/file/ClevelandCliffs_Sustainability+Report+2020+FINAL.pdf"
download "cleveland_cliffs" "clf-ghg-reduction-commitment.pdf" \
  "https://d1io3yog0oux5.cloudfront.net/_d01051743e0d62e2ce7b37056f2a07b3/clevelandcliffs/db/1188/10898/file/CLF_20210128_Cliffs-Commitment_to-Reduce-GHG-Emissions_FINAL.pdf"

# === US STEEL ===
echo "" | tee -a "$LOG_FILE"
echo "=== US STEEL ===" | tee -a "$LOG_FILE"
download "us_steel" "us-steel-sustainability-report-2024.pdf" \
  "https://www.ussteel.com/documents/d/home/uss_2024-sustainability-report_final"
download "us_steel" "us-steel-climate-strategy-report.pdf" \
  "https://www.ussteel.com/documents/40705/43725/USS+Climate+Strategy+Report+Final.pdf/0b293e0b-899a-4d24-c91f-45c8c0ff9ad7?t=1649454242508"

# === BLUESCOPE STEEL ===
echo "" | tee -a "$LOG_FILE"
echo "=== BLUESCOPE STEEL ===" | tee -a "$LOG_FILE"
download "bluescope_steel" "bluescope-sustainability-report-fy2025.pdf" \
  "https://www.bluescope.com/content/dam/bluescope/corporate/bluescope-com/sustainability/documents/fy2025/FY2025_BlueScope_Sustainability_Report.pdf"
download "bluescope_steel" "bluescope-sustainability-data-supplement-fy2025.pdf" \
  "https://www.bluescope.com/content/dam/bluescope/corporate/bluescope-com/sustainability/documents/fy2025/FY2025_Sustainability_Data_Supplement.pdf"
download "bluescope_steel" "bluescope-climate-action-report-fy2024.pdf" \
  "https://www.bluescope.com/content/dam/bluescope/corporate/bluescope-com/sustainability/documents/FY2024-Climate_Action_Reportv2.pdf"

# === KOBE STEEL (KOBELCO) ===
echo "" | tee -a "$LOG_FILE"
echo "=== KOBE STEEL ===" | tee -a "$LOG_FILE"
download "kobe_steel" "kobelco-esg-databook-2024.pdf" \
  "https://www.kobelco.co.jp/english/sustainability/pdf/esg-databook2024.pdf"
download "kobe_steel" "kobelco-esg-databook-2023.pdf" \
  "https://www.kobelco.co.jp/english/sustainability/pdf/esg-databook2023.pdf"
download "kobe_steel" "kobelco-esg-databook-2022.pdf" \
  "https://www.kobelco.co.jp/english/sustainability/pdf/esg-databook2022.pdf"

# === VOESTALPINE ===
echo "" | tee -a "$LOG_FILE"
echo "=== VOESTALPINE ===" | tee -a "$LOG_FILE"
download "voestalpine" "voestalpine-annual-report-2024-25.pdf" \
  "https://www.voestalpine.com/group/static/sites/group/.downloads/en/publications-2024-25/2024-25-annual-report.pdf"

# === SSAB ===
echo "" | tee -a "$LOG_FILE"
echo "=== SSAB ===" | tee -a "$LOG_FILE"
download "ssab" "ssab-annual-report-2024.pdf" \
  "https://www.ssab.com/-/media/files/company/investors/annual-reports/2024/ssab_annual_report_2024.pdf?m=20250320120634"
download "ssab" "ssab-annual-report-2023.pdf" \
  "https://www.ssab.com/-/media/files/company/investors/annual-reports/2023/ssab-annual-report-2023.pdf?m=20240318093306"

# === NIPPON STEEL ===
echo "" | tee -a "$LOG_FILE"
echo "=== NIPPON STEEL ===" | tee -a "$LOG_FILE"
# Already have the integrated reports, add sustainability reports from found URLs
download "nippon_steel" "nippon-steel-sustainability-report-2024.pdf" \
  "https://www.nipponsteel.com/en/sustainability/report/pdf/report2024en.pdf"
download "nippon_steel" "nippon-steel-sustainability-report-2023.pdf" \
  "https://www.nipponsteel.com/en/sustainability/report/pdf/report2023en.pdf"
download "nippon_steel" "nippon-steel-integrated-report-2024.pdf" \
  "https://www.nipponsteel.com/en/ir/library/pdf/nsc_en_ir_2024_all.pdf"

echo "" | tee -a "$LOG_FILE"
echo "============================================" | tee -a "$LOG_FILE"
echo "Completed: $(date)" | tee -a "$LOG_FILE"
echo "============================================" | tee -a "$LOG_FILE"
