"""
Base extractor with common PDF table/text extraction methods.
Company-specific extractors inherit from this.
"""

import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Tuple

import pdfplumber

from .models import CompanyYearData, DataPoint, SourceInfo

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for PDF data extraction."""

    # Subclasses should set these
    company_name: str = ""
    company_slug: str = ""

    # Common patterns for steel production and emissions
    # These are designed to match real report formats across companies
    PRODUCTION_PATTERNS = [
        # "Crude steel production 26.43 MnT" (JSW format)
        r"(?:crude\s+)?steel\s+production[:\s]*(?:consolidated\s+)?([0-9.,]+)\s*(mnt|mtpa|mt|million\s*t|mmt|kt)",
        # "Consolidated 26.43 MnT" preceded by crude steel
        r"consolidated\s+([0-9.,]+)\s*(mnt|mt|million\s*t)",
        r"production\s+(?:of\s+)?(?:crude\s+)?steel[:\s]*([0-9.,]+)\s*(mt|million\s*t|mmt|kt|mnt|mtpa)",
        r"(?:crude\s+)?steel\s+output[:\s]*([0-9.,]+)\s*(mt|million\s*t|mmt|kt|mnt)",
        r"total\s+(?:steel\s+)?production[:\s]*([0-9.,]+)\s*(mt|million\s*t|mmt|kt|mnt)",
        r"(?:deliveries|shipments)[:\s]*([0-9.,]+)\s*(mt|million\s*t|mmt|kt|mnt)",
    ]

    EMISSIONS_PATTERNS = [
        r"(?:scope\s+1\s*(?:\+|and)\s*2|scope\s+1&2)\s*(?:emissions?)?[:\s]*([0-9.,]+)\s*(mt?\s*co2|million\s*t(?:onnes?)?\s*(?:of\s+)?co2|'000\s*t)",
        r"co2\s*e?\s+(?:footprint|emissions?)[^\n]*?([0-9.,]+)\s*(mt?\s*co2|million\s*t(?:onnes?)?\s*(?:of\s+)?co2|kt\s*co2)",
        r"co2\s+emissions?[:\s]*([0-9.,]+)\s*(mt?\s*co2|million\s*t(?:onnes?)?\s*(?:of\s+)?co2|kt\s*co2)",
        r"ghg\s+emissions?[:\s]*([0-9.,]+)\s*(mt?\s*co2e?|million\s*t(?:onnes?)?\s*(?:of\s+)?co2e?)",
        r"total\s+(?:direct\s+)?emissions?[:\s]*([0-9.,]+)\s*(mt?\s*co2|million\s*t)",
    ]

    def extract_tables(self, pdf_path: Path, pages: Optional[List[int]] = None) -> List[Tuple[int, list]]:
        """Extract tables from PDF pages. Returns list of (page_num, table_data)."""
        results = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                page_range = pages if pages else range(len(pdf.pages))
                for page_num in page_range:
                    if page_num >= len(pdf.pages):
                        continue
                    page = pdf.pages[page_num]
                    tables = page.extract_tables()
                    for table in tables:
                        if table:
                            results.append((page_num, table))
        except Exception as e:
            logger.error(f"Error extracting tables from {pdf_path}: {e}")
        return results

    def extract_text(self, pdf_path: Path, pages: Optional[List[int]] = None) -> List[Tuple[int, str]]:
        """Extract text from PDF pages. Returns list of (page_num, text)."""
        results = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                page_range = pages if pages else range(len(pdf.pages))
                for page_num in page_range:
                    if page_num >= len(pdf.pages):
                        continue
                    page = pdf.pages[page_num]
                    text = page.extract_text()
                    if text:
                        results.append((page_num, text))
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {e}")
        return results

    def search_patterns(self, text: str, patterns: List[str]) -> Optional[Tuple[str, str]]:
        """Search text for patterns. Returns (value_str, unit_str) or None."""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1), match.group(2)
        return None

    def parse_number(self, value_str: str) -> Optional[float]:
        """Parse a number string, handling commas and various formats."""
        try:
            cleaned = value_str.replace(",", "").replace(" ", "").strip()
            return float(cleaned)
        except (ValueError, AttributeError):
            return None

    def normalize_to_mt(self, value: float, unit: str) -> float:
        """Convert production/emissions values to million tonnes (Mt)."""
        unit_lower = unit.lower().strip().replace("'", "").replace("\u2019", "")
        # '000 tCO2 or '000 tonne -> value is in thousands of tonnes
        if "000 t" in unit_lower or "000t" in unit_lower:
            return value / 1000.0
        if "kt" in unit_lower:
            return value / 1000.0
        if any(u in unit_lower for u in ["mnt", "mmt", "million", "mt", "mtpa"]):
            return value
        # If the value is very large (>500), it might be in thousands of tonnes
        if value > 500:
            logger.warning(f"Large value {value} with unit '{unit}', assuming '000 tonnes")
            return value / 1000.0
        return value

    def find_year_in_context(self, text: str, position: int, default_year: int) -> int:
        """Try to find a year near the matched position in text."""
        context = text[max(0, position - 200):position + 200]
        years = re.findall(r"20[12]\d", context)
        if years:
            return int(years[-1])  # most recent year found
        return default_year

    @abstractmethod
    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        """Extract data from a PDF. Must be implemented by subclasses."""
        pass

    def try_generic_extraction(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        """Fallback: search all pages for production and emissions patterns."""
        data = CompanyYearData(company=source.company, year=source.year, source=source)

        all_text = self.extract_text(pdf_path)
        full_text = "\n".join(text for _, text in all_text)

        # Search for production
        for page_num, text in all_text:
            result = self.search_patterns(text, self.PRODUCTION_PATTERNS)
            if result:
                val = self.parse_number(result[0])
                if val is not None:
                    val_mt = self.normalize_to_mt(val, result[1])
                    data.add(
                        metric="production_mt",
                        value=val_mt,
                        unit="Mt",
                        page=page_num,
                        method="regex_text",
                        confidence="medium",
                        notes=f"Matched pattern in text, raw: {result[0]} {result[1]}",
                    )
                    break  # take first match

        # Search for emissions
        for page_num, text in all_text:
            result = self.search_patterns(text, self.EMISSIONS_PATTERNS)
            if result:
                val = self.parse_number(result[0])
                if val is not None:
                    val_mt = self.normalize_to_mt(val, result[1])
                    data.add(
                        metric="emissions_scope12_mt_co2",
                        value=val_mt,
                        unit="Mt CO2",
                        page=page_num,
                        method="regex_text",
                        confidence="medium",
                        notes=f"Matched pattern in text, raw: {result[0]} {result[1]}",
                    )
                    break

        return data
