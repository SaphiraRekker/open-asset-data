"""
Script to verify additional report URLs and fetch SEC EDGAR filing URLs.

This script:
1. Checks if estimated PDF URLs are accessible (HTTP HEAD request)
2. Fetches actual 10-K and 20-F filing URLs from SEC EDGAR API
3. Scrapes corporate IR pages for report download links
4. Outputs verified URLs in the same JSON format as report_urls.json

Usage:
    python pipeline/verify_and_fetch_report_urls.py

Requires: requests, beautifulsoup4
    pip install requests beautifulsoup4
"""

import json
import requests
import time
import os
import re
from pathlib import Path
from urllib.parse import urljoin

# Rate limiting
REQUEST_DELAY = 0.5  # seconds between requests

HEADERS = {
    "User-Agent": "OpenAssetData/1.0 (Academic Research; contact@example.com)",
    "Accept": "application/pdf,text/html,application/xhtml+xml",
}

SEC_HEADERS = {
    "User-Agent": "OpenAssetData/1.0 (Academic Research; contact@example.com)",
    "Accept": "application/json",
}


def check_url(url, timeout=15):
    """Check if a URL is accessible. Returns (status_code, content_type, final_url)."""
    try:
        resp = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return resp.status_code, resp.headers.get("Content-Type", ""), resp.url
    except requests.RequestException as e:
        return None, str(e), url


def fetch_sec_filings(cik, filing_type="10-K", start_year=2019, end_year=2025):
    """
    Fetch SEC EDGAR filings for a company.
    Returns list of {year, url, filing_date, accession_number}.
    """
    filings = []
    # Use EDGAR full-text search API
    url = f"https://efts.sec.gov/LATEST/search-index?q=*&dateRange=custom&startdt={start_year}-01-01&enddt={end_year}-12-31&forms={filing_type}&from=0&size=20"

    # Better: use the company filings API
    cik_padded = str(cik).zfill(10)
    api_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    try:
        resp = requests.get(api_url, headers=SEC_HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"  EDGAR API returned {resp.status_code} for CIK {cik}")
            return filings

        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form == filing_type:
                filing_date = dates[i]
                year = int(filing_date[:4])
                if start_year <= year <= end_year:
                    accession = accessions[i].replace("-", "")
                    primary_doc = primary_docs[i]
                    doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{primary_doc}"
                    filings.append({
                        "year": year,
                        "url": doc_url,
                        "filing_date": filing_date,
                        "accession_number": accessions[i],
                    })

    except Exception as e:
        print(f"  Error fetching EDGAR for CIK {cik}: {e}")

    return filings


# SEC-listed steel companies
SEC_COMPANIES = {
    "nucor": {"cik": "73309", "ticker": "NUE", "filing_type": "10-K", "company_name": "Nucor"},
    "cleveland_cliffs": {"cik": "764065", "ticker": "CLF", "filing_type": "10-K", "company_name": "Cleveland-Cliffs"},
    "us_steel": {"cik": "100885", "ticker": "X", "filing_type": "10-K", "company_name": "US Steel"},
    "steel_dynamics": {"cik": "1022671", "ticker": "STLD", "filing_type": "10-K", "company_name": "Steel Dynamics"},
    "ternium": {"cik": "1342874", "ticker": "TX", "filing_type": "20-F", "company_name": "Ternium"},
    "gerdau": {"cik": "1073404", "ticker": "GGB", "filing_type": "20-F", "company_name": "Gerdau"},
    "posco": {"cik": "1137789", "ticker": "PKX", "filing_type": "20-F", "company_name": "POSCO Holdings"},
}


def fetch_all_sec_filings():
    """Fetch all SEC filings for steel companies."""
    results = {}
    for company_key, info in SEC_COMPANIES.items():
        print(f"Fetching SEC {info['filing_type']} filings for {info['company_name']}...")
        filings = fetch_sec_filings(
            info["cik"], info["filing_type"], start_year=2019, end_year=2025
        )
        time.sleep(REQUEST_DELAY)

        doc_type = "form_10k" if info["filing_type"] == "10-K" else "form_20f"
        results[company_key] = []
        for f in filings:
            results[company_key].append({
                "year": f["year"],
                "url": f["url"],
                "doc_type": doc_type,
                "company_name": info["company_name"],
                "notes": f"SEC {info['filing_type']} filed {f['filing_date']}. Accession: {f['accession_number']}",
            })
        print(f"  Found {len(filings)} filings")

    return results


def verify_additional_urls():
    """Verify URLs in additional_report_urls.json."""
    script_dir = Path(__file__).parent
    additional_file = script_dir / "additional_report_urls.json"

    with open(additional_file) as f:
        data = json.load(f)

    verified = {}
    failed = {}

    for company_key, entries in data.items():
        if company_key.startswith("_"):
            continue
        if not isinstance(entries, list):
            continue

        print(f"\nVerifying URLs for {company_key}...")
        verified[company_key] = []
        failed[company_key] = []

        for entry in entries:
            url = entry.get("url", "")
            if not url or "sec.gov/cgi-bin" in url:
                # Skip EDGAR search page URLs
                continue

            status, content_type, final_url = check_url(url)
            time.sleep(REQUEST_DELAY)

            if status == 200:
                is_pdf = "pdf" in content_type.lower() or url.lower().endswith(".pdf")
                entry["verified"] = True
                entry["is_pdf"] = is_pdf
                entry["final_url"] = final_url
                verified[company_key].append(entry)
                print(f"  OK: {url[:80]}...")
            else:
                entry["verified"] = False
                entry["http_status"] = status
                entry["error"] = content_type if status is None else f"HTTP {status}"
                failed[company_key].append(entry)
                print(f"  FAIL ({entry['error']}): {url[:80]}...")

    return verified, failed


def main():
    script_dir = Path(__file__).parent

    print("=" * 70)
    print("Step 1: Fetching SEC EDGAR filings")
    print("=" * 70)
    sec_filings = fetch_all_sec_filings()

    # Save SEC filings
    sec_output = script_dir / "sec_filings_verified.json"
    with open(sec_output, "w") as f:
        json.dump(sec_filings, f, indent=2)
    print(f"\nSEC filings saved to {sec_output}")

    print("\n" + "=" * 70)
    print("Step 2: Verifying additional report URLs")
    print("=" * 70)
    verified, failed = verify_additional_urls()

    # Save results
    verified_output = script_dir / "additional_urls_verified.json"
    failed_output = script_dir / "additional_urls_failed.json"

    with open(verified_output, "w") as f:
        json.dump(verified, f, indent=2)
    with open(failed_output, "w") as f:
        json.dump(failed, f, indent=2)

    print(f"\nVerified URLs saved to {verified_output}")
    print(f"Failed URLs saved to {failed_output}")

    # Summary
    total_verified = sum(len(v) for v in verified.values())
    total_failed = sum(len(v) for v in failed.values())
    total_sec = sum(len(v) for v in sec_filings.values())

    print(f"\n{'=' * 70}")
    print(f"Summary:")
    print(f"  SEC filings found: {total_sec}")
    print(f"  Additional URLs verified: {total_verified}")
    print(f"  Additional URLs failed: {total_failed}")
    print(f"{'=' * 70}")

    # Merge verified additional URLs with SEC filings for final output
    merged = {}
    for key in set(list(verified.keys()) + list(sec_filings.keys())):
        merged[key] = []
        if key in sec_filings:
            merged[key].extend(sec_filings[key])
        if key in verified:
            merged[key].extend(verified[key])

    merged_output = script_dir / "additional_report_urls_verified.json"
    with open(merged_output, "w") as f:
        json.dump(merged, f, indent=2)
    print(f"\nMerged output saved to {merged_output}")


if __name__ == "__main__":
    main()
