"""
Fetch SEC EDGAR 10-K and 20-F filings for US-listed steel companies.

This script uses the official SEC EDGAR API to find actual filing URLs.
No web scraping needed - these are stable government API endpoints.

Usage:
    python pipeline/fetch_sec_filings.py

Requires: requests
    pip install requests
"""

import json
import time
import requests
from pathlib import Path

# SEC requires a User-Agent with a real contact email
# See: https://www.sec.gov/os/accessing-edgar-data
SEC_HEADERS = {
    "User-Agent": "OpenAssetData/1.0 (Academic Research; uq.srekke@uq.edu.au)",
    "Accept": "application/json",
}

# Steel companies with SEC filings
COMPANIES = [
    {
        "company_key": "nucor",
        "company_name": "Nucor",
        "cik": "73309",
        "ticker": "NUE",
        "filing_type": "10-K",
    },
    {
        "company_key": "cleveland_cliffs",
        "company_name": "Cleveland-Cliffs",
        "cik": "764065",
        "ticker": "CLF",
        "filing_type": "10-K",
    },
    {
        "company_key": "us_steel",
        "company_name": "US Steel",
        "cik": "1163302",
        "ticker": "X",
        "filing_type": "10-K",
    },
    {
        "company_key": "steel_dynamics",
        "company_name": "Steel Dynamics",
        "cik": "1022671",
        "ticker": "STLD",
        "filing_type": "10-K",
    },
    {
        "company_key": "ternium",
        "company_name": "Ternium",
        "cik": "1342874",
        "ticker": "TX",
        "filing_type": "20-F",
    },
    {
        "company_key": "gerdau",
        "company_name": "Gerdau",
        "cik": "1073404",
        "ticker": "GGB",
        "filing_type": "20-F",
    },
    {
        "company_key": "posco",
        "company_name": "POSCO Holdings",
        "cik": "889132",
        "ticker": "PKX",
        "filing_type": "20-F",
    },
    {
        "company_key": "arcelormittal",
        "company_name": "ArcelorMittal",
        "cik": "1243429",
        "ticker": "MT",
        "filing_type": "20-F",
    },
]


def fetch_company_filings(cik, filing_type, start_year=2019, end_year=2025):
    """Fetch filings from SEC EDGAR submissions API."""
    cik_padded = str(cik).zfill(10)
    api_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    try:
        resp = requests.get(api_url, headers=SEC_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Error fetching CIK {cik}: {e}")
        return []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    descriptions = recent.get("primaryDocDescription", [])

    filings = []
    for i, form in enumerate(forms):
        if form != filing_type:
            continue

        filing_date = dates[i]
        year = int(filing_date[:4])

        # For 10-K, the report year is typically the year before the filing
        # For 20-F, same logic applies
        # But we use the filing year for the record
        if year < start_year or year > end_year:
            continue

        accession_raw = accessions[i]
        accession_clean = accession_raw.replace("-", "")
        primary_doc = primary_docs[i]
        description = descriptions[i] if i < len(descriptions) else ""

        # Construct the filing URL
        doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_clean}/{primary_doc}"

        # Also construct the filing index page URL
        index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_clean}/"

        filings.append({
            "year": year,
            "filing_date": filing_date,
            "url": doc_url,
            "index_url": index_url,
            "accession_number": accession_raw,
            "primary_document": primary_doc,
            "description": description,
        })

    return filings


def main():
    script_dir = Path(__file__).parent
    output_file = script_dir / "sec_steel_filings.json"

    all_filings = {}
    report_url_entries = {}

    for company in COMPANIES:
        key = company["company_key"]
        name = company["company_name"]
        cik = company["cik"]
        ftype = company["filing_type"]
        ticker = company["ticker"]

        print(f"Fetching {ftype} filings for {name} (CIK: {cik}, Ticker: {ticker})...")

        filings = fetch_company_filings(cik, ftype)
        time.sleep(0.5)  # Be nice to SEC servers

        all_filings[key] = {
            "company_name": name,
            "cik": cik,
            "ticker": ticker,
            "filing_type": ftype,
            "filings": filings,
        }

        # Convert to report_urls.json format
        doc_type = "form_10k" if ftype == "10-K" else "form_20f"
        report_url_entries[key] = []
        for f in filings:
            report_url_entries[key].append({
                "year": f["year"],
                "url": f["url"],
                "doc_type": doc_type,
                "company_name": name,
                "notes": f"SEC {ftype} filed {f['filing_date']}. Accession: {f['accession_number']}",
            })

        print(f"  Found {len(filings)} {ftype} filings")
        for f in filings:
            print(f"    {f['filing_date']}: {f['primary_document']}")

    # Save detailed output
    with open(output_file, "w") as f:
        json.dump(all_filings, f, indent=2)
    print(f"\nDetailed filings saved to {output_file}")

    # Save in report_urls.json compatible format
    compatible_output = script_dir / "sec_filings_for_report_urls.json"
    with open(compatible_output, "w") as f:
        json.dump(report_url_entries, f, indent=2)
    print(f"report_urls.json format saved to {compatible_output}")

    # Print summary
    print(f"\n{'='*60}")
    print("Summary of SEC filings found:")
    print(f"{'='*60}")
    total = 0
    for key, data in all_filings.items():
        count = len(data["filings"])
        total += count
        print(f"  {data['company_name']:25s} ({data['ticker']:5s}): {count} {data['filing_type']} filings")
    print(f"  {'TOTAL':25s}        : {total} filings")


if __name__ == "__main__":
    main()
