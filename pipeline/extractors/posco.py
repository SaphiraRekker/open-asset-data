"""
POSCO Holdings-specific extractor.
Handles Sustainability Report format.

Known report format (2023 Sustainability Report):
- Page 124: Financial and Production Information table
  "Crude steel produced kt 38,263 34,219 35,682"
- Page 125: GHG Emissions table
  "Direct/indirect emissions (Scope 1&2) tCO2e 78,490,212 70,185,623 71,971,900"
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class POSCOExtractor(BaseExtractor):
    company_name = "POSCO Holdings"
    company_slug = "posco"

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running POSCO extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        # Strategy 1: Text search — POSCO tables often parse as text
        all_text = self.extract_text(pdf_path)
        for page_num, text in all_text:
            self._search_posco_data(text, page_num, data)

        # Strategy 2: Table extraction
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)
        if not has_production or not has_emissions:
            tables = self.extract_tables(pdf_path)
            for page_num, table in tables:
                self._search_table(table, page_num, data)

        # Strategy 3: Generic fallback
        if not data.data_points:
            return self.try_generic_extraction(pdf_path, source)

        return data

    def _search_posco_data(self, text: str, page_num: int, data: CompanyYearData):
        """Search for POSCO production and emissions in text."""
        lines = text.split("\n")
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)

        for line in lines:
            ll = line.lower()

            # Production: "Crude steel produced kt 38,263 34,219 35,682"
            if not has_production and "crude steel" in ll and ("produced" in ll or "production" in ll):
                if "intensity" in ll:
                    continue
                numbers = re.findall(r"([\d,]+\.?\d*)", line)
                for n in numbers:
                    val = self.parse_number(n)
                    if val is not None:
                        # POSCO reports in kt (~35,000 kt = 35 Mt)
                        if 20000 < val < 50000:
                            data.add(
                                metric="production_mt",
                                value=val / 1000.0,
                                unit="Mt",
                                page=page_num,
                                method="regex_text",
                                confidence="high",
                                notes=f"POSCO crude steel produced: {val} kt = {val/1000:.1f} Mt",
                            )
                            has_production = True
                            break
                        # Also handle if already in Mt
                        elif 20 < val < 50:
                            data.add(
                                metric="production_mt",
                                value=val,
                                unit="Mt",
                                page=page_num,
                                method="regex_text",
                                confidence="high",
                                notes=f"POSCO crude steel produced: {val} Mt",
                            )
                            has_production = True
                            break

            # Emissions: "Direct/indirect emissions (Scope 1&2) tCO2e 78,490,212 70,185,623 71,971,900"
            if not has_emissions and "scope 1" in ll and "2" in ll and "emission" in ll:
                if "intensity" in ll or "indirect emissions (scope 3" in ll:
                    continue
                # Look for tCO2e values (tens of millions)
                numbers = re.findall(r"([\d,]+)", line)
                for n in numbers:
                    val = self.parse_number(n)
                    if val is not None and 30000000 < val < 200000000:
                        # Convert tCO2e to Mt CO2
                        val_mt = val / 1000000.0
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val_mt,
                            unit="Mt CO2",
                            page=page_num,
                            method="regex_text",
                            confidence="high",
                            notes=f"POSCO Scope 1&2: {val:,.0f} tCO2e = {val_mt:.1f} Mt",
                        )
                        has_emissions = True
                        break

    def _search_table(self, table: list, page_num: int, data: CompanyYearData):
        """Search structured tables for POSCO data."""
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)

        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            if not has_production and "crude steel" in row_text and "intensity" not in row_text:
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 20000 < val < 50000:
                        data.add(
                            metric="production_mt",
                            value=val / 1000.0,
                            unit="Mt",
                            page=page_num,
                            table=row_idx,
                            method="table_pdfplumber",
                            confidence="high",
                            notes=f"POSCO table: {val} kt",
                        )
                        has_production = True
                        break

            if not has_emissions and "scope 1" in row_text and "2" in row_text:
                if "intensity" in row_text or "scope 3" in row_text:
                    continue
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 30000000 < val < 200000000:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val / 1000000.0,
                            unit="Mt CO2",
                            page=page_num,
                            table=row_idx,
                            method="table_pdfplumber",
                            confidence="high",
                            notes=f"POSCO table Scope 1&2: {val:,.0f} tCO2e",
                        )
                        has_emissions = True
                        break
