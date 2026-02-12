"""
Tata Steel-specific extractor.
Handles Indian Integrated Report format (FY notation, e.g., FY2023-24).
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class TataSteelExtractor(BaseExtractor):
    company_name = "Tata Steel"
    company_slug = "tata_steel"

    # Tata reports in MTPA (million tonnes per annum) or Mt
    # India operations ~20 Mt, UK ~3-5 Mt, Netherlands ~7 Mt
    TATA_PRODUCTION_PATTERNS = [
        r"(?:crude\s+)?steel\s+production[:\s]*([0-9.,]+)\s*(mt(?:pa)?|million\s*t|mmt)",
        r"total\s+(?:steel\s+)?production[:\s]*([0-9.,]+)\s*(mt(?:pa)?|million\s*t)",
        r"(?:deliveries|saleable\s+steel)[:\s]*([0-9.,]+)\s*(mt(?:pa)?|million\s*t)",
        r"production\s+volume[:\s]*([0-9.,]+)\s*(mt(?:pa)?|million\s*t)",
    ]

    TATA_EMISSIONS_PATTERNS = [
        r"(?:scope\s+1\s*(?:\+|and|&)\s*2)[:\s]*([0-9.,]+)\s*(mt?\s*(?:of\s+)?co2|million\s*t(?:onnes?)?\s*co2)",
        r"(?:total\s+)?co2\s+emissions?[:\s]*([0-9.,]+)\s*(mt?\s*co2|million\s*t(?:onnes?)?\s*co2)",
        r"ghg\s+emissions?[:\s]*([0-9.,]+)\s*(mt?\s*co2e?|million\s*t(?:onnes?)?\s*co2e?)",
        r"(?:tco2e?|co2\s+equivalent)[:\s]*([0-9.,]+)\s*(mt|million\s*t|crore\s*t)",
    ]

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running Tata Steel extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        # Strategy 1: Table extraction — look for performance summary tables
        tables = self.extract_tables(pdf_path)
        for page_num, table in tables:
            self._search_table(table, page_num, data)

        # Strategy 2: Text-based extraction for emissions
        # Tata's emissions tables often don't parse well, so also search text
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)
        if not has_emissions:
            text_pages = self.extract_text(pdf_path)
            for page_num, text in text_pages:
                self._search_emissions_text(text, page_num, data)

        # Strategy 3: Text regex for production if not found
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        if not has_production:
            if not text_pages:
                text_pages = self.extract_text(pdf_path)
            for page_num, text in text_pages:
                self._search_text(text, page_num, data)

        # Strategy 4: Generic fallback
        if not data.data_points:
            return self.try_generic_extraction(pdf_path, source)

        return data

    def _search_table(self, table: list, page_num: int, data: CompanyYearData):
        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            # Tata reports crude steel production
            if any(term in row_text for term in ["crude steel", "steel production", "hot metal"]):
                if "intensity" in row_text or "per tonne" in row_text:
                    continue
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 1 < val < 50:  # Tata ~20-30 Mt total
                        data.add(
                            metric="production_mt",
                            value=val,
                            unit="Mt",
                            page=page_num,
                            table=row_idx,
                            method="table_pdfplumber",
                            confidence="high",
                            notes=f"Table row: {row_text[:80]}",
                        )
                        return  # take first match

            # Emissions
            if any(term in row_text for term in ["co2 emission", "scope 1", "ghg emission", "tco2"]):
                if "intensity" in row_text or "per tonne" in row_text:
                    continue
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 1 < val < 200:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val,
                            unit="Mt CO2",
                            page=page_num,
                            table=row_idx,
                            method="table_pdfplumber",
                            confidence="high",
                            notes=f"Table row: {row_text[:80]}",
                        )
                        return

    def _search_emissions_text(self, text: str, page_num: int, data: CompanyYearData):
        """Search for Tata Steel's consolidated emissions in text.

        Tata reports emissions in tables like:
          'Absolute emissions - Scope 1 for all sites MT - 33 49 50 56'
          'Total Scope 1 emissions Million tonnes COe 56 55 77 76'
        The last or second-to-last number is typically the most recent FY.
        """
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)
        if has_emissions:
            return

        lines = text.split("\n")
        scope1_val = None
        scope2_val = None

        for line in lines:
            ll = line.lower()

            # Match "Total Scope 1 emissions Million tonnes COe 56 55 77 76"
            if "total scope 1 emission" in ll and ("million" in ll or " mt " in ll.replace("mt", " mt ")):
                numbers = re.findall(r"(\d+\.?\d*)", line)
                # Filter to reasonable Scope 1 values (20-100 Mt for Tata consolidated)
                vals = [float(n) for n in numbers if 20 < float(n) < 120]
                if vals:
                    scope1_val = vals[0]  # most recent year (first reasonable value)

            # Match "Total Scope 2 emissions Million tonnes COe 7 6 5 6"
            if "total scope 2 emission" in ll and ("million" in ll or " mt " in ll.replace("mt", " mt ")):
                numbers = re.findall(r"(\d+\.?\d*)", line)
                vals = [float(n) for n in numbers if 1 < float(n) < 30]
                if vals:
                    scope2_val = vals[0]

            # Also match consolidated format:
            # "Absolute emissions -Scope 1 for all sites MT - 33 49 50 56"
            if "absolute emissions" in ll and "scope 1" in ll and "all sites" in ll:
                numbers = re.findall(r"(\d+\.?\d*)", line)
                vals = [float(n) for n in numbers if 20 < float(n) < 120]
                if vals:
                    scope1_val = vals[-1]  # last value = most recent

            if "absolute emissions" in ll and "scope 2" in ll and "all sites" in ll:
                numbers = re.findall(r"(\d+\.?\d*)", line)
                vals = [float(n) for n in numbers if 1 < float(n) < 30]
                if vals:
                    scope2_val = vals[-1]

        if scope1_val is not None:
            total = scope1_val + (scope2_val or 0)
            data.add(
                metric="emissions_scope12_mt_co2",
                value=total,
                unit="Mt CO2",
                page=page_num,
                method="regex_text",
                confidence="high" if scope2_val else "medium",
                notes=f"Tata consolidated Scope 1={scope1_val}, Scope 2={scope2_val}",
            )

    def _search_text(self, text: str, page_num: int, data: CompanyYearData):
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)

        if not has_production:
            result = self.search_patterns(text, self.TATA_PRODUCTION_PATTERNS)
            if result:
                val = self.parse_number(result[0])
                if val is not None:
                    val_mt = self.normalize_to_mt(val, result[1])
                    if 1 < val_mt < 50:
                        data.add(
                            metric="production_mt",
                            value=val_mt,
                            unit="Mt",
                            page=page_num,
                            method="regex_text",
                            confidence="medium",
                        )
