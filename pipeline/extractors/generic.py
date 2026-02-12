"""
Generic extractor - keyword-based fallback for any company.
Searches all pages for production and emissions patterns.
Enhanced to handle various report formats including flowing text,
cross-line numbers, kt/thousand tonnes units, and tCO2e values.
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class GenericExtractor(BaseExtractor):
    """Fallback extractor that uses regex patterns across all pages."""

    company_name = "Generic"
    company_slug = "generic"

    def __init__(self, company_name: str = "Generic", company_slug: str = "generic"):
        self.company_name = company_name
        self.company_slug = company_slug

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running generic extraction on {pdf_path.name}")
        data = self.try_generic_extraction(pdf_path, source)

        # If base generic didn't find anything, try enhanced patterns
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)

        if not has_production or not has_emissions:
            all_text = self.extract_text(pdf_path)
            for page_num, text in all_text:
                # Rejoin hyphenated line breaks
                clean_text = re.sub(r"-\n\s*", "", text)
                self._enhanced_search(clean_text, page_num, data)

        # Also try table search
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)
        if not has_production or not has_emissions:
            tables = self.extract_tables(pdf_path)
            for page_num, table in tables:
                self._search_tables_enhanced(table, page_num, data)

        return data

    def _enhanced_search(self, text: str, page_num: int, data: CompanyYearData):
        """Enhanced text search with broader patterns."""
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)

        if not has_production:
            # "X million tons of crude steel" or "crude steel production of X million tons"
            patterns = [
                r"(\d+\.?\d*)\s*million\s*tons?\s+(?:of\s+)?crude\s+steel",
                r"crude\s+steel[^.]*?(\d+\.?\d*)\s*million\s*tons?",
                r"steel\s+production[^.]*?(\d+\.?\d*)\s*million\s*tons?",
                r"produced\s+(\d+\.?\d*)\s*million\s*tons?",
                r"production\s+of\s+(\d+\.?\d*)\s*million\s*tons?",
                # kt format: "35,682 kt"
                r"crude\s+steel[^.]*?([\d,]+)\s*kt",
                # "tons of crude steel" with number before
                r"([\d,.]+)\s*(?:thousand\s+)?tons?\s+(?:of\s+)?crude\s+steel",
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    val = self.parse_number(match.group(1))
                    if val is not None:
                        if "kt" in pattern or "thousand" in pattern:
                            val = val / 1000.0
                        if 1 < val < 200:
                            data.add(
                                metric="production_mt",
                                value=val,
                                unit="Mt",
                                page=page_num,
                                method="regex_text",
                                confidence="medium",
                                notes=f"Generic enhanced: {match.group(0)[:80]}",
                            )
                            break

        if not has_emissions:
            # Various emission formats
            patterns = [
                # "Scope 1 and Scope 2 emissions ... X million tons"
                r"scope\s+1\s+and\s+scope\s+2[^.]*?(\d+\.?\d*)\s*million\s*tons?",
                # "GHG emissions ... X tCO2e/t of steel" - skip intensity
                # "GHG emissions of X Mt CO2"
                r"ghg\s+emissions?[^.]*?(\d+\.?\d*)\s*(mt|million\s*t)",
                # tCO2e in large numbers: "71,971,900"
                r"(?:scope\s+1\s*(?:&|and)\s*2)[^.]*?([\d,]+)\s*tco2",
                # "X thousand tonnes CO2"
                r"(?:co2|carbon\s+dioxide)\s+emissions?[^.]*?([\d,]+)\s*thousand\s*tonn",
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    val = self.parse_number(match.group(1))
                    if val is not None:
                        if "thousand" in pattern:
                            val = val / 1000.0
                        elif "tco2" in pattern and val > 1000000:
                            val = val / 1000000.0
                        if 1 < val < 500:
                            data.add(
                                metric="emissions_scope12_mt_co2",
                                value=val,
                                unit="Mt CO2",
                                page=page_num,
                                method="regex_text",
                                confidence="low",
                                notes=f"Generic enhanced: {match.group(0)[:80]}",
                            )
                            break

    def _search_tables_enhanced(self, table: list, page_num: int, data: CompanyYearData):
        """Enhanced table search with broader matching."""
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)

        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            if not has_production:
                if any(kw in row_text for kw in ["crude steel", "steel production", "production volume"]):
                    if "intensity" in row_text or "per tonne" in row_text:
                        continue
                    for cell in row[1:]:
                        val = self.parse_number(str(cell)) if cell else None
                        if val is not None:
                            # Handle kt
                            if 1000 < val < 100000:
                                data.add(
                                    metric="production_mt",
                                    value=val / 1000.0,
                                    unit="Mt",
                                    page=page_num,
                                    table=row_idx,
                                    method="table_pdfplumber",
                                    confidence="medium",
                                    notes=f"Generic table (kt): {row_text[:80]}",
                                )
                                has_production = True
                                break
                            elif 1 < val < 200:
                                data.add(
                                    metric="production_mt",
                                    value=val,
                                    unit="Mt",
                                    page=page_num,
                                    table=row_idx,
                                    method="table_pdfplumber",
                                    confidence="medium",
                                    notes=f"Generic table: {row_text[:80]}",
                                )
                                has_production = True
                                break

            if not has_emissions:
                if any(kw in row_text for kw in ["co2 emission", "scope 1", "ghg emission", "carbon dioxide"]):
                    if "intensity" in row_text or "per tonne" in row_text:
                        continue
                    for cell in row[1:]:
                        val = self.parse_number(str(cell)) if cell else None
                        if val is not None:
                            if val > 1000000:  # tCO2e
                                data.add(
                                    metric="emissions_scope12_mt_co2",
                                    value=val / 1000000.0,
                                    unit="Mt CO2",
                                    page=page_num,
                                    table=row_idx,
                                    method="table_pdfplumber",
                                    confidence="medium",
                                    notes=f"Generic table (tCO2e): {row_text[:80]}",
                                )
                                has_emissions = True
                                break
                            elif 1000 < val < 100000:  # thousand tonnes
                                data.add(
                                    metric="emissions_scope12_mt_co2",
                                    value=val / 1000.0,
                                    unit="Mt CO2",
                                    page=page_num,
                                    table=row_idx,
                                    method="table_pdfplumber",
                                    confidence="medium",
                                    notes=f"Generic table (kt): {row_text[:80]}",
                                )
                                has_emissions = True
                                break
                            elif 1 < val < 500:
                                data.add(
                                    metric="emissions_scope12_mt_co2",
                                    value=val,
                                    unit="Mt CO2",
                                    page=page_num,
                                    table=row_idx,
                                    method="table_pdfplumber",
                                    confidence="medium",
                                    notes=f"Generic table: {row_text[:80]}",
                                )
                                has_emissions = True
                                break
