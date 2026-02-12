"""
Nippon Steel-specific extractor.
Japanese company with detailed English integrated reports.
"""

import logging
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class NipponSteelExtractor(BaseExtractor):
    company_name = "Nippon Steel"
    company_slug = "nippon_steel"

    NIPPON_PRODUCTION_PATTERNS = [
        r"(?:crude\s+)?steel\s+(?:production|output)[:\s]*([0-9.,]+)\s*(million\s*t(?:on(?:ne)?s?)?|mt|mmt|kt)",
        r"(?:total\s+)?production\s+volume[:\s]*([0-9.,]+)\s*(million\s*t|mt)",
        r"steel\s+products[:\s]*([0-9.,]+)\s*(million\s*t|mt|kt)",
    ]

    NIPPON_EMISSIONS_PATTERNS = [
        r"co2\s+emissions?[:\s]*([0-9.,]+)\s*(million\s*t(?:on(?:ne)?s?)?\s*(?:of\s+)?co2|mt\s*co2)",
        r"(?:scope\s+1\s*(?:\+|and|&)\s*2)[:\s]*([0-9.,]+)\s*(million\s*t|mt)\s*co2",
        r"(?:total\s+)?ghg\s+emissions?[:\s]*([0-9.,]+)\s*(million\s*t|mt)\s*co2",
        # Nippon sometimes reports in 10,000 tonnes (万トン)
        r"co2\s+emissions?[:\s]*([0-9.,]+)\s*(10,?000\s*t|ten\s*thousand)",
        # "energy-derived CO2 emissions ... 79 million tons"
        r"(?:energy.derived\s+)?co2\s+emissions?\s+.*?(\d+\.?\d*)\s*(million\s*t)",
    ]

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running Nippon Steel extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        # Strategy 1: Table extraction
        tables = self.extract_tables(pdf_path)
        for page_num, table in tables:
            self._search_table(table, page_num, data)

        # Strategy 2: Text search (always run for emissions even if tables found production)
        text_pages = self.extract_text(pdf_path)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        if not has_emissions or not has_production:
            for page_num, text in text_pages:
                self._search_text(text, page_num, data)
                self._search_emissions_detailed(text, page_num, data)

        # Strategy 3: Generic fallback
        if not data.data_points:
            return self.try_generic_extraction(pdf_path, source)

        return data

    def _search_table(self, table: list, page_num: int, data: CompanyYearData):
        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            # Only add production if we don't have it yet
            has_production = any(dp.metric == "production_mt" for dp in data.data_points)
            if not has_production and any(term in row_text for term in ["crude steel", "steel production", "production volume"]):
                if "intensity" in row_text or "per" in row_text:
                    continue
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 30 < val < 80:  # Nippon ~40-50 Mt
                        data.add(
                            metric="production_mt",
                            value=val,
                            unit="Mt",
                            page=page_num,
                            table=row_idx,
                            method="table_pdfplumber",
                            confidence="high",
                            notes=f"Table: {row_text[:80]}",
                        )
                        return

            has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)
            if not has_emissions and any(term in row_text for term in ["co2 emission", "scope 1", "ghg", "greenhouse"]):
                if "intensity" in row_text or "per" in row_text:
                    continue
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 30 < val < 300:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val,
                            unit="Mt CO2",
                            page=page_num,
                            table=row_idx,
                            method="table_pdfplumber",
                            confidence="high",
                            notes=f"Table: {row_text[:80]}",
                        )
                        return

    def _search_text(self, text: str, page_num: int, data: CompanyYearData):
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)

        if not has_production:
            result = self.search_patterns(text, self.NIPPON_PRODUCTION_PATTERNS)
            if result:
                val = self.parse_number(result[0])
                if val is not None:
                    val_mt = self.normalize_to_mt(val, result[1])
                    if 10 < val_mt < 100:
                        data.add(metric="production_mt", value=val_mt, unit="Mt",
                                 page=page_num, method="regex_text", confidence="medium")

            # Also match "produced 34.99 million tons of crude steel"
            import re
            match = re.search(r"produced\s+([0-9.,]+)\s*million\s*tons?\s+of\s+crude\s+steel", text, re.IGNORECASE)
            if match and not any(dp.metric == "production_mt" for dp in data.data_points):
                val = self.parse_number(match.group(1))
                if val and 10 < val < 100:
                    data.add(metric="production_mt", value=val, unit="Mt",
                             page=page_num, method="regex_text", confidence="high",
                             notes=f"Nippon 'produced X million tons of crude steel': {val}")

    def _search_emissions_detailed(self, text: str, page_num: int, data: CompanyYearData):
        """Search for Nippon Steel emissions in detailed text and table formats.

        Nippon Steel reports in thousand t-CO2 in their Scope 1+2 tables:
          Scope1: ~92,000 thousand t-CO2
          Scope2: ~12,000 thousand t-CO2
        And in text: "energy-derived CO2 emissions ... 79 million tons"
        """
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)
        if has_emissions:
            return

        import re

        # Pattern: "energy-derived CO2 emissions were 936 PJ and 79 million tons"
        match = re.search(
            r"energy.derived\s+co2\s+emissions\s+.*?(\d+\.?\d*)\s*million\s*t",
            text, re.IGNORECASE
        )
        if match:
            val = float(match.group(1))
            if 30 < val < 200:
                data.add(
                    metric="emissions_scope12_mt_co2",
                    value=val,
                    unit="Mt CO2",
                    page=page_num,
                    method="regex_text",
                    confidence="medium",
                    notes=f"Nippon energy-derived CO2: {val} million tons (preliminary)",
                )
                return

        # Table format in thousand t-CO2: look for Scope1 row
        lines = text.split("\n")
        scope1_val = None
        scope2_val = None
        for line in lines:
            ll = line.lower()
            # "Scope1 Direct emissions ... 92,XXX" (thousand t-CO2)
            if "scope1" in ll.replace(" ", "") and "direct" in ll:
                numbers = re.findall(r"(\d[\d,]+)", line)
                for n in numbers:
                    v = self.parse_number(n)
                    if v and 50000 < v < 200000:  # thousand t-CO2
                        scope1_val = v / 1000.0  # convert to Mt
            if "scope2" in ll.replace(" ", "") and "indirect" in ll:
                numbers = re.findall(r"(\d[\d,]+)", line)
                for n in numbers:
                    v = self.parse_number(n)
                    if v and 5000 < v < 50000:
                        scope2_val = v / 1000.0

        if scope1_val:
            total = scope1_val + (scope2_val or 0)
            data.add(
                metric="emissions_scope12_mt_co2",
                value=total,
                unit="Mt CO2",
                page=page_num,
                method="regex_text",
                confidence="high" if scope2_val else "medium",
                notes=f"Nippon Scope1={scope1_val:.1f}Mt, Scope2={scope2_val}Mt",
            )
