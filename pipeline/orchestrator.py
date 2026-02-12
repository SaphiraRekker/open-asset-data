"""
Orchestrator - end-to-end pipeline runner.
Downloads reports, extracts data, and outputs CSV files with full provenance.

Usage:
    cd open-asset-data
    python -m pipeline.orchestrator
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

from .config import (
    EXTRACTED_PRODUCTION_FILE,
    EXTRACTED_EMISSIONS_FILE,
    EXTRACTION_REPORT_FILE,
    PROCESSED_DATA_DIR,
    OUTPUTS_DIR,
)
from .registry import ReportRegistry
from .downloader import ReportDownloader
from .models import SourceInfo, DataPoint
from .extractors.generic import GenericExtractor
from .extractors.arcelormittal import ArcelorMittalExtractor
from .extractors.tata_steel import TataSteelExtractor
from .extractors.jswsteel import JSWSteelExtractor
from .extractors.nippon_steel import NipponSteelExtractor
from .extractors.posco import POSCOExtractor
from .extractors.ssab import SSABExtractor
from .extractors.thyssenkrupp import ThyssenKruppExtractor
from .extractors.nucor import NucorExtractor
from .extractors.bluescope import BlueScopeExtractor
from .extractors.cleveland_cliffs import ClevelandCliffsExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Map company slugs to their specialized extractors
EXTRACTOR_MAP = {
    "arcelormittal": ArcelorMittalExtractor,
    "tata_steel": TataSteelExtractor,
    "jsw_steel": JSWSteelExtractor,
    "nippon_steel": NipponSteelExtractor,
    "posco": POSCOExtractor,
    "ssab": SSABExtractor,
    "thyssenkrupp": ThyssenKruppExtractor,
    "nucor": NucorExtractor,
    "bluescope_steel": BlueScopeExtractor,
    "cleveland_cliffs": ClevelandCliffsExtractor,
}


def get_extractor(company_slug: str):
    """Get the appropriate extractor for a company."""
    if company_slug in EXTRACTOR_MAP:
        return EXTRACTOR_MAP[company_slug]()
    return GenericExtractor(company_slug=company_slug)


def run_pipeline(sector: str = "steel", companies: list = None):
    """Run the full download + extraction pipeline."""
    logger.info("=" * 60)
    logger.info("REPORT PIPELINE - Download & Extract")
    logger.info("=" * 60)

    # Step 1: Load registry
    registry = ReportRegistry()
    all_companies = companies or registry.get_all_companies()
    logger.info(f"Registry has {len(all_companies)} companies: {all_companies}")

    # Step 2: Download all reports
    downloader = ReportDownloader()
    all_data_points = []
    extraction_report = []

    for company_slug in all_companies:
        reports = registry.get_reports(company_slug)
        if not reports:
            logger.warning(f"No reports in registry for {company_slug}")
            extraction_report.append({
                "company_slug": company_slug,
                "status": "no_reports_in_registry",
                "production_found": False,
                "emissions_found": False,
            })
            continue

        for report_entry in reports:
            url = report_entry["url"]
            year = report_entry["year"]
            doc_type = report_entry["doc_type"]
            company_name = report_entry["company_name"]

            logger.info(f"\n--- {company_name} ({year}, {doc_type}) ---")

            # Download
            source = downloader.download(
                url=url,
                company_slug=company_slug,
                year=year,
                doc_type=doc_type,
                company_name=company_name,
                sector=sector,
            )

            if source is None:
                logger.error(f"Failed to download: {url}")
                extraction_report.append({
                    "company_slug": company_slug,
                    "company_name": company_name,
                    "year": year,
                    "doc_type": doc_type,
                    "url": url,
                    "status": "download_failed",
                    "production_found": False,
                    "emissions_found": False,
                })
                continue

            # Find the absolute path to the downloaded PDF
            # source.local_path is relative to project root
            pdf_path = _resolve_pdf_path(source)
            if pdf_path is None or not pdf_path.exists():
                logger.error(f"PDF not found at resolved path for {company_name}")
                extraction_report.append({
                    "company_slug": company_slug,
                    "company_name": company_name,
                    "year": year,
                    "doc_type": doc_type,
                    "url": url,
                    "status": "pdf_not_found",
                    "production_found": False,
                    "emissions_found": False,
                })
                continue

            # Extract
            extractor = get_extractor(company_slug)
            logger.info(f"Using extractor: {extractor.__class__.__name__}")

            try:
                result = extractor.extract(pdf_path, source)
                points = result.data_points
                all_data_points.extend(points)

                has_production = any(dp.metric == "production_mt" for dp in points)
                has_emissions = any(dp.metric.startswith("emissions") for dp in points)

                logger.info(
                    f"Extracted {len(points)} data points "
                    f"(production: {has_production}, emissions: {has_emissions})"
                )

                extraction_report.append({
                    "company_slug": company_slug,
                    "company_name": company_name,
                    "year": year,
                    "doc_type": doc_type,
                    "url": url,
                    "status": "success" if points else "no_data_found",
                    "num_data_points": len(points),
                    "production_found": has_production,
                    "emissions_found": has_emissions,
                    "extraction_method": points[0].extraction_method if points else "",
                    "pdf_path": str(pdf_path),
                    "sha256": source.sha256,
                })

            except Exception as e:
                logger.error(f"Extraction failed for {company_name}: {e}")
                extraction_report.append({
                    "company_slug": company_slug,
                    "company_name": company_name,
                    "year": year,
                    "doc_type": doc_type,
                    "url": url,
                    "status": f"extraction_error: {str(e)[:100]}",
                    "production_found": False,
                    "emissions_found": False,
                })

    # Step 3: Save outputs
    _save_outputs(all_data_points, extraction_report)

    # Step 4: Print summary
    _print_summary(all_data_points, extraction_report)


def _resolve_pdf_path(source: SourceInfo) -> Path:
    """Resolve the PDF path from SourceInfo."""
    from .config import PROJECT_ROOT, ANNUAL_REPORTS_DIR

    # Try relative path from project root
    candidate = PROJECT_ROOT / source.local_path
    if candidate.exists():
        return candidate

    # Try with data/ prefix (handles off-by-one in relative path calc)
    candidate = PROJECT_ROOT / "data" / source.local_path
    if candidate.exists():
        return candidate

    # Try absolute path
    candidate = Path(source.local_path)
    if candidate.exists():
        return candidate

    # Search in AnnualReports directory (case-insensitive for .PDF/.pdf)
    filename = Path(source.local_path).name
    for pdf in ANNUAL_REPORTS_DIR.rglob("*.[pP][dD][fF]"):
        if pdf.name == filename:
            return pdf

    return candidate  # return even if doesn't exist, caller checks


def _save_outputs(data_points: list, extraction_report: list):
    """Save extracted data to CSV files."""
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    if data_points:
        df = pd.DataFrame([dp.to_dict() for dp in data_points])

        # Split into production and emissions
        prod_df = df[df["metric"] == "production_mt"].copy()
        emis_df = df[df["metric"].str.startswith("emissions")].copy()

        if not prod_df.empty:
            prod_df.to_csv(EXTRACTED_PRODUCTION_FILE, index=False)
            logger.info(f"Production data saved: {EXTRACTED_PRODUCTION_FILE} ({len(prod_df)} rows)")

        if not emis_df.empty:
            emis_df.to_csv(EXTRACTED_EMISSIONS_FILE, index=False)
            logger.info(f"Emissions data saved: {EXTRACTED_EMISSIONS_FILE} ({len(emis_df)} rows)")

        # Also save combined
        combined_path = PROCESSED_DATA_DIR / "steel_all_extracted.csv"
        df.to_csv(combined_path, index=False)
        logger.info(f"Combined data saved: {combined_path} ({len(df)} rows)")

    # Save extraction quality report
    if extraction_report:
        report_df = pd.DataFrame(extraction_report)
        report_df.to_csv(EXTRACTION_REPORT_FILE, index=False)
        logger.info(f"Extraction report saved: {EXTRACTION_REPORT_FILE}")


def _print_summary(data_points: list, extraction_report: list):
    """Print a summary of the pipeline run."""
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)

    total_reports = len(extraction_report)
    successful = sum(1 for r in extraction_report if r.get("status") == "success")
    failed_download = sum(1 for r in extraction_report if r.get("status") == "download_failed")
    no_data = sum(1 for r in extraction_report if r.get("status") == "no_data_found")
    errors = sum(1 for r in extraction_report if "error" in str(r.get("status", "")))

    logger.info(f"Total reports attempted: {total_reports}")
    logger.info(f"  Successful extractions: {successful}")
    logger.info(f"  Download failures: {failed_download}")
    logger.info(f"  No data found: {no_data}")
    logger.info(f"  Extraction errors: {errors}")
    logger.info(f"Total data points extracted: {len(data_points)}")

    if data_points:
        companies_with_production = set(
            dp.company for dp in data_points if dp.metric == "production_mt"
        )
        companies_with_emissions = set(
            dp.company for dp in data_points if dp.metric.startswith("emissions")
        )
        logger.info(f"Companies with production data: {len(companies_with_production)}")
        logger.info(f"Companies with emissions data: {len(companies_with_emissions)}")

        for dp in data_points:
            logger.info(
                f"  {dp.company} ({dp.year}): {dp.metric} = {dp.value:.2f} {dp.unit} "
                f"[{dp.extraction_method}, p.{dp.source_page}, {dp.confidence}]"
            )

    logger.info("=" * 60)


def main():
    """Entry point."""
    run_pipeline(sector="steel")


if __name__ == "__main__":
    main()
