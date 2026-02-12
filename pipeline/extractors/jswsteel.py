"""
JSW Steel-specific extractor.
Indian company reporting in Indian FY (April-March), units in MTPA/MnT.

Known report format (2023-24 IR):
- Page 12: "Crude steel production\nConsolidated 26.43 MnT"
- Page 46: Table with "CO emissions\n2\n(Scope 1 and 2)" | "'000 tCO\n2" | "53,167.64"
  The unit is '000 tCO2, so divide by 1000 to get Mt.
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class JSWSteelExtractor(BaseExtractor):
    company_name = "JSW Steel"
    company_slug = "jsw_steel"

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running JSW Steel extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        # Strategy 1: Search text for "Consolidated XX.XX MnT" near crude steel
        all_text = self.extract_text(pdf_path)
        for page_num, text in all_text:
            self._extract_jsw_text(text, page_num, data)

        # Strategy 2: Table extraction for emissions (CO2 in '000 tCO2)
        tables = self.extract_tables(pdf_path)
        for page_num, table in tables:
            self._extract_jsw_table(table, page_num, data)

        # Strategy 3: Generic fallback
        if not data.data_points:
            return self.try_generic_extraction(pdf_path, source)

        return data

    def _extract_jsw_text(self, text: str, page_num: int, data: CompanyYearData):
        """Extract from JSW's text format: 'Crude steel production\\n\\nConsolidated 26.43 MnT'"""
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        if has_production:
            return

        lines = text.split("\n")
        crude_steel_line_idx = -10  # track how many lines since "Crude steel production"

        for i, line in enumerate(lines):
            line_lower = line.lower().strip()

            # Look for "Crude steel production" header
            if "crude steel production" in line_lower:
                crude_steel_line_idx = i
                # Check same line: "Crude steel production Consolidated 26.43 MnT"
                match = re.search(r"(\d+\.?\d*)\s*MnT", line)
                if match:
                    val = float(match.group(1))
                    if 5 < val < 100:
                        data.add(
                            metric="production_mt", value=val, unit="Mt",
                            page=page_num, method="regex_text", confidence="high",
                            notes=f"JSW production (same line): {val} MnT",
                        )
                        return
                continue

            # Within 3 lines of "Crude steel production", look for "Consolidated 26.43 MnT"
            if 0 < (i - crude_steel_line_idx) <= 3:
                match = re.search(r"(?:[Cc]onsolidated\s+)?(\d+\.?\d*)\s*MnT", line)
                if match:
                    val = float(match.group(1))
                    if 5 < val < 100:
                        data.add(
                            metric="production_mt",
                            value=val,
                            unit="Mt",
                            page=page_num,
                            method="regex_text",
                            confidence="high",
                            notes=f"JSW consolidated production: {val} MnT",
                        )
                        return

    def _extract_jsw_table(self, table: list, page_num: int, data: CompanyYearData):
        """Extract from JSW's ESG performance table."""
        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            # CO2 emissions (Scope 1 and 2) in '000 tCO2
            if ("co" in row_text and ("scope 1" in row_text or "scope 1 and 2" in row_text)):
                if "intensity" in row_text:
                    continue
                has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)
                if has_emissions:
                    continue

                # Check unit column for '000 tCO2
                unit_is_thousands = any("000" in str(cell) for cell in row if cell)

                # Get the first numeric cell after the label (FY column)
                for cell in row:
                    if cell is None:
                        continue
                    val = self.parse_number(str(cell))
                    if val is not None and val > 1000:  # in '000 tCO2, values like 53,167
                        val_mt = val / 1000.0 if unit_is_thousands else val
                        if 1 < val_mt < 200:
                            data.add(
                                metric="emissions_scope12_mt_co2",
                                value=val_mt,
                                unit="Mt CO2",
                                page=page_num,
                                table=row_idx,
                                method="table_pdfplumber",
                                confidence="high",
                                notes=f"JSW CO2 Scope 1+2, raw: {val} {'000 tCO2' if unit_is_thousands else 'Mt CO2'}",
                            )
                            return

            # Also check for crude steel production in table
            if "crude steel" in row_text and "production" in row_text:
                if "intensity" in row_text:
                    continue
                has_production = any(dp.metric == "production_mt" for dp in data.data_points)
                if has_production:
                    continue
                for cell in row[1:]:
                    val = self.parse_number(str(cell)) if cell else None
                    if val is not None and 1 < val < 100:
                        data.add(
                            metric="production_mt", value=val, unit="Mt",
                            page=page_num, table=row_idx,
                            method="table_pdfplumber", confidence="high",
                            notes=f"JSW table crude steel: {val}",
                        )
                        return
