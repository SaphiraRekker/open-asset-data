#!/bin/bash
# =============================================================================
# OVERNIGHT REPORT DOWNLOAD SCRIPT
# Downloads all additional steel company reports (factbooks, sustainability,
# SEC filings, investor presentations, climate reports, country reports)
#
# Run with: caffeinate -s bash pipeline/download_all_reports.sh 2>&1 | tee pipeline/download_log.txt
# =============================================================================

set -e
BASE_DIR="/Users/uqsrekke/Library/CloudStorage/OneDrive-TheUniversityofQueensland/Documents/GitHub/open-asset-data"
REPORTS_DIR="$BASE_DIR/data/raw/AnnualReports/steel"
LOG_FILE="$BASE_DIR/pipeline/download_log.txt"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

SUCCESS=0
FAILED=0
SKIPPED=0

download_pdf() {
    local company_dir="$1"
    local filename="$2"
    local url="$3"

    local target_dir="$REPORTS_DIR/$company_dir"
    mkdir -p "$target_dir"
    local target="$target_dir/$filename"

    # Skip if already exists and is > 10KB (not an error page)
    if [ -f "$target" ] && [ $(stat -f%z "$target" 2>/dev/null || echo 0) -gt 10000 ]; then
        echo "  SKIP (exists): $filename"
        SKIPPED=$((SKIPPED + 1))
        return 0
    fi

    # Download with timeout
    local http_code
    http_code=$(curl -L -o "$target" -w "%{http_code}" \
        --connect-timeout 30 --max-time 180 \
        -s -H "User-Agent: $UA" \
        "$url" 2>/dev/null || echo "000")

    local filesize=$(stat -f%z "$target" 2>/dev/null || echo 0)

    if [ "$http_code" = "200" ] && [ "$filesize" -gt 10000 ]; then
        local sizeMB=$(echo "scale=1; $filesize/1048576" | bc)
        echo "  OK ($sizeMB MB): $filename"
        SUCCESS=$((SUCCESS + 1))
    else
        echo "  FAIL (HTTP $http_code, ${filesize}B): $filename <- $url"
        rm -f "$target" 2>/dev/null
        FAILED=$((FAILED + 1))
    fi

    # Be polite - wait between downloads
    sleep 1
}

echo "=============================================="
echo "Steel Company Report Downloader"
echo "Started: $(date)"
echo "=============================================="

# =============================================================================
# TATA STEEL
# =============================================================================
echo ""
echo "=== TATA STEEL ==="

# Sustainability reports
download_pdf "tata_steel" "tata-steel-sustainability-report-fy2024.pdf" \
    "https://www.tatasteel.com/media/21243/sustainability-report-fy2024.pdf"
download_pdf "tata_steel" "tata-steel-sustainability-report-fy2023.pdf" \
    "https://www.tatasteel.com/media/18372/sustainability-report-fy2023.pdf"

# Tata Steel Europe reports
download_pdf "tata_steel" "tata-steel-europe-sustainability-2024.pdf" \
    "https://www.tatasteeleurope.com/sites/default/files/tata-steel-europe-sustainability-report-2024.pdf"
download_pdf "tata_steel" "tata-steel-europe-sustainability-2023.pdf" \
    "https://www.tatasteeleurope.com/sites/default/files/tata-steel-europe-sustainability-report-2023.pdf"

# =============================================================================
# POSCO
# =============================================================================
echo ""
echo "=== POSCO ==="

download_pdf "posco" "posco-sustainability-2024.pdf" \
    "https://sustainability.posco.com/assets_eng/file/POSCO_Sustainability_Report_2024_eng.pdf"
download_pdf "posco" "posco-annual-report-2023.pdf" \
    "https://www.posco-holdings.com/assets/file/ir/Annual_Report_2023_eng.pdf"
download_pdf "posco" "posco-annual-report-2022.pdf" \
    "https://www.posco-holdings.com/assets/file/ir/Annual_Report_2022_eng.pdf"
download_pdf "posco" "posco-tcfd-2023.pdf" \
    "https://sustainability.posco.com/assets_eng/file/POSCO_TCFD_Report_2023_eng.pdf"

# =============================================================================
# NIPPON STEEL
# =============================================================================
echo ""
echo "=== NIPPON STEEL ==="

download_pdf "nippon_steel" "nippon-steel-esg-databook-2024.pdf" \
    "https://www.nipponsteel.com/en/csr/report/pdf/report2024_databook_e.pdf"
download_pdf "nippon_steel" "nippon-steel-esg-databook-2023.pdf" \
    "https://www.nipponsteel.com/en/csr/report/pdf/report2023_databook_e.pdf"
download_pdf "nippon_steel" "nippon-steel-factbook-2024.pdf" \
    "https://www.nipponsteel.com/en/ir/library/pdf/factbook2024_e.pdf"
download_pdf "nippon_steel" "nippon-steel-factbook-2023.pdf" \
    "https://www.nipponsteel.com/en/ir/library/pdf/factbook2023_e.pdf"
download_pdf "nippon_steel" "nippon-steel-carbon-neutral-report-2024.pdf" \
    "https://www.nipponsteel.com/en/csr/env/pdf/carbon_neutral_report2024_e.pdf"

# =============================================================================
# JSW STEEL
# =============================================================================
echo ""
echo "=== JSW STEEL ==="

download_pdf "jsw_steel" "jsw-steel-sustainability-report-2024.pdf" \
    "https://www.jsw.in/sites/default/files/assets/downloads/steel/Sustainability/JSW-Steel-Sustainability-Report-2023-24.pdf"
download_pdf "jsw_steel" "jsw-steel-sustainability-report-2023.pdf" \
    "https://www.jsw.in/sites/default/files/assets/downloads/steel/Sustainability/JSW-Steel-Sustainability-Report-2022-23.pdf"

# =============================================================================
# THYSSENKRUPP
# =============================================================================
echo ""
echo "=== THYSSENKRUPP ==="

download_pdf "thyssenkrupp" "thyssenkrupp-sustainability-report-2024.pdf" \
    "https://www.thyssenkrupp.com/media/content_1/company/sustainability_3/2024_sustainability_report/thyssenkrupp-sustainability-report-2023-2024-en.pdf"
download_pdf "thyssenkrupp" "thyssenkrupp-sustainability-report-2023.pdf" \
    "https://www.thyssenkrupp.com/media/content_1/company/sustainability_3/2023_sustainability_report/thyssenkrupp-sustainability-report-2022-2023-en.pdf"
download_pdf "thyssenkrupp" "thyssenkrupp-steel-climate-strategy.pdf" \
    "https://www.thyssenkrupp-steel.com/media/content_1/company_3/sustainability_3/thyssenkrupp_steel_climate_strategy_en.pdf"

# =============================================================================
# SSAB
# =============================================================================
echo ""
echo "=== SSAB ==="

download_pdf "ssab" "ssab-sustainability-report-2024.pdf" \
    "https://www.ssab.com/en/investors/reports-and-presentations/annual-report-2024/sustainability-report-2024"
download_pdf "ssab" "ssab-annual-report-2024.pdf" \
    "https://www.ssab.com/-/media/files/company/investors/annual-reports/2024/ssab-annual-report-2024.pdf"
download_pdf "ssab" "ssab-fossil-free-steel-factsheet.pdf" \
    "https://www.ssab.com/-/media/files/company/sustainability/ssab-fossil-free-steel.pdf"

# =============================================================================
# BLUESCOPE STEEL
# =============================================================================
echo ""
echo "=== BLUESCOPE STEEL ==="

download_pdf "bluescope" "bluescope-sustainability-report-2024.pdf" \
    "https://www.bluescope.com/-/media/project/bluescopewebsite/files/sustainability/sustainability-report-2024.pdf"
download_pdf "bluescope-steel" "bluescope-sustainability-report-2023.pdf" \
    "https://www.bluescope.com/-/media/project/bluescopewebsite/files/sustainability/sustainability-report-2023.pdf"
download_pdf "bluescope-steel" "bluescope-climate-action-report-2024.pdf" \
    "https://www.bluescope.com/-/media/project/bluescopewebsite/files/sustainability/climate-action-report-2024.pdf"

# =============================================================================
# NUCOR
# =============================================================================
echo ""
echo "=== NUCOR ==="

download_pdf "nucor" "nucor-sustainability-report-2024.pdf" \
    "https://nucor.com/-/media/Files/N/Nucor-Corp/sustainability/Nucor-Sustainability-Report-2024.pdf"
download_pdf "nucor" "nucor-10k-2024.pdf" \
    "https://nucor.com/-/media/Files/N/Nucor-Corp/annual-reports/2024-annual-report.pdf"
download_pdf "nucor" "nucor-10k-2023.pdf" \
    "https://nucor.com/-/media/Files/N/Nucor-Corp/annual-reports/2023-annual-report.pdf"

# =============================================================================
# CLEVELAND-CLIFFS
# =============================================================================
echo ""
echo "=== CLEVELAND-CLIFFS ==="

download_pdf "cleveland_cliffs" "cleveland-cliffs-sustainability-report-2024.pdf" \
    "https://www.clevelandcliffs.com/-/media/files/sustainability/2024-sustainability-report.pdf"
download_pdf "cleveland_cliffs" "cleveland-cliffs-sustainability-report-2023.pdf" \
    "https://www.clevelandcliffs.com/-/media/files/sustainability/2023-sustainability-report.pdf"
download_pdf "cleveland_cliffs" "cleveland-cliffs-10k-2024.pdf" \
    "https://www.clevelandcliffs.com/-/media/files/annual-reports/2024-annual-report.pdf"

# =============================================================================
# GERDAU
# =============================================================================
echo ""
echo "=== GERDAU ==="

download_pdf "gerdau" "gerdau-integrated-report-2024.pdf" \
    "https://www.gerdau.com/media/2024/gerdau-integrated-report-2024.pdf"
download_pdf "gerdau" "gerdau-sustainability-report-2024.pdf" \
    "https://www.gerdau.com/media/2024/gerdau-esg-report-2024-en.pdf"
download_pdf "gerdau" "gerdau-sustainability-report-2023.pdf" \
    "https://www.gerdau.com/media/2023/gerdau-esg-report-2023-en.pdf"

# =============================================================================
# JFE HOLDINGS
# =============================================================================
echo ""
echo "=== JFE HOLDINGS ==="

download_pdf "jfe_holdings" "jfe-esg-databook-2024.pdf" \
    "https://www.jfe-holdings.co.jp/en/csr/report/pdf/2024/esg_databook2024_e.pdf"
download_pdf "jfe_holdings" "jfe-esg-databook-2023.pdf" \
    "https://www.jfe-holdings.co.jp/en/csr/report/pdf/2023/esg_databook2023_e.pdf"
download_pdf "jfe_holdings" "jfe-factbook-2024.pdf" \
    "https://www.jfe-holdings.co.jp/en/investor/library/factbook/pdf/factbook2024_e.pdf"
download_pdf "jfe_holdings" "jfe-carbon-neutral-report-2024.pdf" \
    "https://www.jfe-holdings.co.jp/en/csr/environment/pdf/carbon_neutral_2024_e.pdf"

# =============================================================================
# US STEEL
# =============================================================================
echo ""
echo "=== US STEEL ==="

download_pdf "us_steel" "us-steel-esg-report-2024.pdf" \
    "https://www.ussteel.com/-/media/project/ussteel/files/sustainability/2024-esg-report.pdf"
download_pdf "us_steel" "us-steel-esg-report-2023.pdf" \
    "https://www.ussteel.com/-/media/project/ussteel/files/sustainability/2023-esg-report.pdf"
download_pdf "us_steel" "us-steel-annual-report-2024.pdf" \
    "https://www.ussteel.com/-/media/project/ussteel/files/investors/annual-reports/2024-annual-report.pdf"

# =============================================================================
# HYUNDAI STEEL
# =============================================================================
echo ""
echo "=== HYUNDAI STEEL ==="

download_pdf "hyundai_steel" "hyundai-steel-sustainability-report-2024.pdf" \
    "https://www.hyundai-steel.com/en/common/file/sustDown.do?file=Hyundai_Steel_Sustainability_Report_2024_ENG.pdf"
download_pdf "hyundai_steel" "hyundai-steel-carbon-neutrality-report-2024.pdf" \
    "https://www.hyundai-steel.com/en/common/file/sustDown.do?file=Hyundai_Steel_Carbon_Neutrality_Report_2024_ENG.pdf"

# =============================================================================
# KOBE STEEL (KOBELCO)
# =============================================================================
echo ""
echo "=== KOBE STEEL ==="

download_pdf "kobe_steel" "kobelco-esg-databook-2024.pdf" \
    "https://www.kobelco.co.jp/english/about_kobelco/csr/files/esg_databook_2024.pdf"
download_pdf "kobe_steel" "kobelco-esg-databook-2023.pdf" \
    "https://www.kobelco.co.jp/english/about_kobelco/csr/files/esg_databook_2023.pdf"
download_pdf "kobe_steel" "kobelco-integrated-report-2024.pdf" \
    "https://www.kobelco.co.jp/english/ir/integrated_report/files/2024_integrated_report_e.pdf"

# =============================================================================
# VOESTALPINE
# =============================================================================
echo ""
echo "=== VOESTALPINE ==="

download_pdf "voestalpine" "voestalpine-sustainability-report-2024.pdf" \
    "https://www.voestalpine.com/group/static/sites/group/.downloads/en/publications/2023-24-sustainability-report-en.pdf"
download_pdf "voestalpine" "voestalpine-sustainability-report-2023.pdf" \
    "https://www.voestalpine.com/group/static/sites/group/.downloads/en/publications/2022-23-sustainability-report-en.pdf"
download_pdf "voestalpine" "voestalpine-corporate-responsibility-2024.pdf" \
    "https://www.voestalpine.com/group/static/sites/group/.downloads/en/publications/2023-24-cr-report-en.pdf"

# =============================================================================
# SAIL
# =============================================================================
echo ""
echo "=== SAIL ==="

download_pdf "sail" "sail-sustainability-report-2024.pdf" \
    "https://sail.co.in/sites/default/files/SAIL_Sustainability_Report_2023-24.pdf"
download_pdf "sail" "sail-sustainability-report-2023.pdf" \
    "https://sail.co.in/sites/default/files/SAIL_Sustainability_Report_2022-23.pdf"

# =============================================================================
# STEEL DYNAMICS
# =============================================================================
echo ""
echo "=== STEEL DYNAMICS ==="

download_pdf "steel_dynamics" "steel-dynamics-sustainability-report-2024.pdf" \
    "https://www.stld.com/-/media/files/sustainability/2024-sustainability-report.pdf"
download_pdf "steel_dynamics" "steel-dynamics-sustainability-report-2023.pdf" \
    "https://www.stld.com/-/media/files/sustainability/2023-sustainability-report.pdf"
download_pdf "steel_dynamics" "steel-dynamics-annual-report-2024.pdf" \
    "https://www.stld.com/-/media/files/annual-reports/2024-annual-report.pdf"

# =============================================================================
# SALZGITTER
# =============================================================================
echo ""
echo "=== SALZGITTER ==="

download_pdf "salzgitter" "salzgitter-sustainability-report-2024.pdf" \
    "https://www.salzgitter-ag.com/fileadmin/salzgitter-ag/06_MediaRelations/04_NHB/NHB_2024_E.pdf"
download_pdf "salzgitter" "salzgitter-sustainability-report-2023.pdf" \
    "https://www.salzgitter-ag.com/fileadmin/salzgitter-ag/06_MediaRelations/04_NHB/NHB_2023_E.pdf"
download_pdf "salzgitter" "salzgitter-factsheet-2024.pdf" \
    "https://www.salzgitter-ag.com/fileadmin/salzgitter-ag/06_MediaRelations/01_Factsheet/Factsheet_2024_E.pdf"

# =============================================================================
# TERNIUM
# =============================================================================
echo ""
echo "=== TERNIUM ==="

download_pdf "ternium" "ternium-sustainability-report-2024.pdf" \
    "https://www.ternium.com/-/media/Files/T/Ternium/sustainability/Ternium-Sustainability-Report-2024.pdf"
download_pdf "ternium" "ternium-sustainability-report-2023.pdf" \
    "https://www.ternium.com/-/media/Files/T/Ternium/sustainability/Ternium-Sustainability-Report-2023.pdf"
download_pdf "ternium" "ternium-annual-report-2024.pdf" \
    "https://www.ternium.com/-/media/Files/T/Ternium/annual-report/Ternium-Annual-Report-2024.pdf"

# =============================================================================
# NLMK (Russia - may be limited)
# =============================================================================
echo ""
echo "=== NLMK ==="

download_pdf "nlmk" "nlmk-sustainability-report-2021.pdf" \
    "https://nlmk.com/upload/iblock/nlmk-sustainability-report-2021-eng.pdf"
download_pdf "nlmk" "nlmk-sustainability-report-2020.pdf" \
    "https://nlmk.com/upload/iblock/nlmk-sustainability-report-2020-eng.pdf"

# =============================================================================
# EVRAZ (Russia - may be limited)
# =============================================================================
echo ""
echo "=== EVRAZ ==="

download_pdf "evraz" "evraz-sustainability-report-2021.pdf" \
    "https://www.evraz.com/upload/iblock/evraz-sustainability-report-2021.pdf"
download_pdf "evraz" "evraz-sustainability-report-2020.pdf" \
    "https://www.evraz.com/upload/iblock/evraz-sustainability-report-2020.pdf"

# =============================================================================
# SEVERSTAL (Russia - may be limited)
# =============================================================================
echo ""
echo "=== SEVERSTAL ==="

download_pdf "severstal" "severstal-sustainability-report-2021.pdf" \
    "https://severstal.com/upload/iblock/severstal-sustainability-report-2021-eng.pdf"

# =============================================================================
# CHINA STEEL CORPORATION
# =============================================================================
echo ""
echo "=== CHINA STEEL CORPORATION ==="

download_pdf "china_steel" "china-steel-csr-report-2023.pdf" \
    "https://www.csc.com.tw/csc/hr/csr/pdf/CSC_CSR_Report_2023_EN.pdf"
download_pdf "china_steel" "china-steel-csr-report-2022.pdf" \
    "https://www.csc.com.tw/csc/hr/csr/pdf/CSC_CSR_Report_2022_EN.pdf"
download_pdf "china_steel" "china-steel-annual-report-2023.pdf" \
    "https://www.csc.com.tw/csc/hr/aReport/pdf/CSC_Annual_Report_2023_EN.pdf"

# =============================================================================
# BAOSHAN / BAOSTEEL
# =============================================================================
echo ""
echo "=== BAOSHAN / BAOSTEEL ==="

download_pdf "baoshan" "baosteel-environmental-report-2024.pdf" \
    "https://www.baosteel.com/group/contents/5091/21.html"
download_pdf "baoshan" "baosteel-environmental-report-2023.pdf" \
    "https://www.baosteel.com/group/contents/5091/20.html"

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "=============================================="
echo "Download Complete: $(date)"
echo "=============================================="
echo "  Successful: $SUCCESS"
echo "  Failed:     $FAILED"
echo "  Skipped:    $SKIPPED"
echo "  Total:      $((SUCCESS + FAILED + SKIPPED))"
echo "=============================================="
