"""
ArcelorMittal-specific extractor.
Handles Integrated Annual Review and Sustainability Report formats.

Known report format (2023 IAR):
- Page 33 has performance table as flowing text:
  "Adjusted crude steel production1 Mt ArcelorMittal 77.4 73.3 60.9 70.7 61.8 58.1 – –"
  "Adjusted absolute COe footprint1 Million tonnes ArcelorMittal 158.8 151.8 130.5 148.1 125.7 114.3 – –"
  Numbers are chronological (oldest to newest), last numeric value = most recent year.
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class ArcelorMittalExtractor(BaseExtractor):
    company_name = "ArcelorMittal"
    company_slug = "arcelormittal"

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running ArcelorMittal extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        # Strategy 1: Search text for the specific ArcelorMittal table format
        all_text = self.extract_text(pdf_path)
        for page_num, text in all_text:
            self._extract_am_performance_table(text, page_num, data, source.year)

        # Strategy 2: Search tables
        if not data.data_points:
            tables = self.extract_tables(pdf_path)
            for page_num, table in tables:
                self._search_table_for_data(table, page_num, data)

        # Strategy 3: Generic fallback with better patterns
        if not data.data_points:
            return self.try_generic_extraction(pdf_path, source)

        return data

    def _extract_am_performance_table(self, text: str, page_num: int,
                                       data: CompanyYearData, report_year: int):
        """Extract from ArcelorMittal's flowing-text performance table format."""
        lines = text.split("\n")

        for line in lines:
            line_lower = line.lower()

            # Production: "Adjusted crude steel production1 Mt ArcelorMittal 77.4 73.3 60.9 70.7 61.8 58.1"
            if "crude steel production" in line_lower and "arcelormittal" in line_lower:
                if "intensity" in line_lower:
                    continue
                numbers = re.findall(r"(\d+\.?\d*)", line)
                # Filter to reasonable production values (>10 Mt, <200 Mt)
                prod_values = [float(n) for n in numbers if 10 < float(n) < 200]
                if prod_values:
                    # Emit data point for each year in the series
                    # Series is oldest-to-newest, last value = report_year
                    n = len(prod_values)
                    for i, val in enumerate(prod_values):
                        year_for_val = report_year - (n - 1 - i)
                        conf = "high" if i == n - 1 else "medium"
                        data.add(
                            metric="production_mt",
                            value=val,
                            unit="Mt",
                            page=page_num,
                            method="regex_text",
                            confidence=conf,
                            notes=f"AM performance table, value {i+1}/{n} from series: {prod_values}",
                            year_override=year_for_val,
                        )

            # Emissions: "Adjusted absolute COe footprint1 Million tonnes ArcelorMittal 158.8 151.8 ..."
            if ("co" in line_lower and "footprint" in line_lower and
                    "arcelormittal" in line_lower and "intensity" not in line_lower):
                if "europe" in line_lower:
                    continue  # skip Europe-only line
                numbers = re.findall(r"(\d+\.?\d*)", line)
                emis_values = [float(n) for n in numbers if 50 < float(n) < 500]
                if emis_values:
                    # Emit data point for each year in the series
                    n = len(emis_values)
                    for i, val in enumerate(emis_values):
                        year_for_val = report_year - (n - 1 - i)
                        conf = "high" if i == n - 1 else "medium"
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val,
                            unit="Mt CO2",
                            page=page_num,
                            method="regex_text",
                            confidence=conf,
                            notes=f"AM CO2 footprint table, value {i+1}/{n} from series: {emis_values}",
                            year_override=year_for_val,
                        )

            # Also look for "98.5 million tonnes" absolute footprint (2023 98.5 Mt is scope 1+2)
            match = re.search(r"(?:absolute|total)\s+co2?\s*e?\s+(?:footprint|emissions?)[^\n]*?(\d+\.?\d*)\s*million\s*tonnes", line_lower)
            if match:
                val = float(match.group(1))
                if 50 < val < 500:
                    has_emis = any(dp.metric.startswith("emissions") for dp in data.data_points)
                    if not has_emis:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val,
                            unit="Mt CO2",
                            page=page_num,
                            method="regex_text",
                            confidence="medium",
                            notes=f"AM absolute emissions text match: {val}",
                        )

    def _search_table_for_data(self, table: list, page_num: int,
                                data: CompanyYearData):
        """Search structured table for production and emissions data."""
        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            if any(kw in row_text for kw in ["crude steel", "steel production", "steel shipments"]):
                if "intensity" in row_text:
                    continue
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 10 < val < 200:
                        data.add(
                            metric="production_mt", value=val, unit="Mt",
                            page=page_num, table=row_idx,
                            method="table_pdfplumber", confidence="high",
                            notes=f"Table row: {row_text[:80]}",
                        )
                        break

            if any(kw in row_text for kw in ["co2 emission", "scope 1", "ghg emission", "co2e footprint"]):
                if "intensity" in row_text:
                    continue
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 10 < val < 500:
                        data.add(
                            metric="emissions_scope12_mt_co2", value=val, unit="Mt CO2",
                            page=page_num, table=row_idx,
                            method="table_pdfplumber", confidence="high",
                            notes=f"Table row: {row_text[:80]}",
                        )
                        break
