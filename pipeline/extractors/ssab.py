"""
SSAB-specific extractor.
Handles SSAB Annual Report format (Nordic/Swedish company).

Known report format (2023 Annual Report):
- Page 5: "annual crude steel production capacity of 8.8 million tonnes"
- Page 10: "Carbon dioxide emissions, thousand tonnes 9,915 9,844"
  (Scope 1 only, but primary figure reported)
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class SSABExtractor(BaseExtractor):
    company_name = "SSAB"
    company_slug = "ssab"

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running SSAB extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        all_text = self.extract_text(pdf_path)
        for page_num, text in all_text:
            self._search_ssab_data(text, page_num, data)

        if not data.data_points:
            tables = self.extract_tables(pdf_path)
            for page_num, table in tables:
                self._search_table(table, page_num, data)

        if not data.data_points:
            return self.try_generic_extraction(pdf_path, source)

        return data

    def _search_ssab_data(self, text: str, page_num: int, data: CompanyYearData):
        lines = text.split("\n")
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)

        for line in lines:
            ll = line.lower()

            # Production: "annual crude steel production capacity of 8.8 million tonnes"
            if not has_production and "crude steel production" in ll:
                if "intensity" in ll:
                    continue
                match = re.search(r"(\d+\.?\d*)\s*million\s*tonn", line, re.IGNORECASE)
                if match:
                    val = float(match.group(1))
                    if 3 < val < 20:  # SSAB ~8-9 Mt
                        data.add(
                            metric="production_mt",
                            value=val,
                            unit="Mt",
                            page=page_num,
                            method="regex_text",
                            confidence="high",
                            notes=f"SSAB crude steel production capacity: {val} Mt",
                        )
                        has_production = True

            # Emissions: "Carbon dioxide emissions, thousand tonnes 9,915 9,844"
            # or "Carbon dioxide emissions2), thousand tonnes 9,915 9,844"
            if not has_emissions and "carbon dioxide emission" in ll and "thousand tonn" in ll:
                numbers = re.findall(r"([\d,]+\.?\d*)", line)
                for n in numbers:
                    val = self.parse_number(n)
                    if val is not None and 5000 < val < 20000:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val / 1000.0,
                            unit="Mt CO2",
                            page=page_num,
                            method="regex_text",
                            confidence="high",
                            notes=f"SSAB CO2 emissions: {val:,.0f} thousand tonnes = {val/1000:.1f} Mt (Scope 1)",
                        )
                        has_emissions = True
                        break

            # Also look for "COe emissions (Scope 1)" with values
            if not has_emissions and "co" in ll and "emission" in ll and "scope 1" in ll:
                numbers = re.findall(r"([\d,]+\.?\d*)", line)
                for n in numbers:
                    val = self.parse_number(n)
                    if val is not None and 5000 < val < 20000:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val / 1000.0,
                            unit="Mt CO2",
                            page=page_num,
                            method="regex_text",
                            confidence="medium",
                            notes=f"SSAB Scope 1: {val:,.0f} thousand tonnes",
                        )
                        has_emissions = True
                        break

    def _search_table(self, table: list, page_num: int, data: CompanyYearData):
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)

        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            if not has_production and "crude steel" in row_text and "intensity" not in row_text:
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 3 < val < 20:
                        data.add(
                            metric="production_mt", value=val, unit="Mt",
                            page=page_num, table=row_idx,
                            method="table_pdfplumber", confidence="high",
                            notes=f"SSAB table: {row_text[:80]}",
                        )
                        has_production = True
                        break

            if not has_emissions and "carbon dioxide" in row_text and "thousand" in row_text:
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 5000 < val < 20000:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val / 1000.0,
                            unit="Mt CO2",
                            page=page_num, table=row_idx,
                            method="table_pdfplumber", confidence="high",
                            notes=f"SSAB table: {val:,.0f} thousand tonnes",
                        )
                        has_emissions = True
                        break
