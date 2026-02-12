"""
Nucor-specific extractor.
Handles two report types:
  1. GHG Verification Statement (short 2-page document, emissions only)
  2. Annual/sustainability reports (production + emissions)

Known formats:
- Verification Statement (2023): "Scope 1  4,344,072", "Scope 2 - Market  5,143,384" (tCO2e)
- Sustainability Report (2023): "Steel Shipped 25.7M Tons" (short tons)
- Emissions in SR: Scope 1 = 5.7M tCO2e, Scope 2 = 5.1M tCO2e (steel mills)

Conversion: 1 US short ton = 0.907185 metric tonnes
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)

SHORT_TON_TO_METRIC = 0.907185


class NucorExtractor(BaseExtractor):
    company_name = "Nucor"
    company_slug = "nucor"

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running Nucor extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        all_text = self.extract_text(pdf_path)
        first_text = " ".join(t for _, t in all_text[:3]).lower()

        # Route based on report type
        if "verification statement" in first_text or "verification opinion" in first_text:
            self._extract_verification(all_text, data)
        else:
            self._extract_report(all_text, data)
            # Also try tables for reports
            if not self._has_production(data) or not self._has_emissions(data):
                tables = self.extract_tables(pdf_path)
                for page_num, table in tables:
                    self._search_table(table, page_num, data)

        if not data.data_points:
            return self.try_generic_extraction(pdf_path, source)

        return data

    def _has_production(self, data: CompanyYearData) -> bool:
        return any(dp.metric == "production_mt" for dp in data.data_points)

    def _has_emissions(self, data: CompanyYearData) -> bool:
        return any(dp.metric.startswith("emissions") for dp in data.data_points)

    def _extract_verification(self, all_text: list, data: CompanyYearData):
        """Extract from GHG Verification Statement (existing logic)."""
        scope1 = None
        scope2 = None

        for page_num, text in all_text:
            for line in text.split("\n"):
                ll = line.lower().strip()

                # "Scope 1  4,344,072"
                if ll.startswith("scope 1") and "scope 2" not in ll:
                    for n in re.findall(r"([\d,]+)", line):
                        val = self.parse_number(n)
                        if val and 1000000 < val < 50000000:
                            scope1 = val

                # "Scope 2 - Market  5,143,384"
                if "scope 2" in ll:
                    for n in re.findall(r"([\d,]+)", line):
                        val = self.parse_number(n)
                        if val and 1000000 < val < 50000000:
                            scope2 = val

        if scope1 is not None:
            total_mt = (scope1 + (scope2 or 0)) / 1000000.0
            data.add(
                metric="emissions_scope12_mt_co2",
                value=round(total_mt, 3),
                unit="Mt CO2",
                page=1,
                method="regex_text",
                confidence="high",
                notes=f"Nucor verification: Scope1={scope1:,.0f}, Scope2={scope2 or 0:,.0f} tCO2e, total={total_mt:.2f} Mt",
            )

    def _extract_report(self, all_text: list, data: CompanyYearData):
        """Extract from annual/sustainability reports."""
        for page_num, text in all_text:
            clean_text = re.sub(r"-\n\s*", "", text)

            if not self._has_production(data):
                self._search_production(clean_text, page_num, data)

            if not self._has_emissions(data):
                self._search_emissions(clean_text, page_num, data)

    def _search_production(self, text: str, page_num: int, data: CompanyYearData):
        # Pattern 1: "Steel Shipped 25.7M Tons" or "25.7M Tons steel shipped"
        patterns = [
            r"(?:steel\s+)?shipped\s+([\d,.]+)\s*[Mm]\s*(?:short\s+)?[Tt]ons?",
            r"([\d,.]+)\s*[Mm]\s*(?:short\s+)?[Tt]ons?\s+(?:of\s+)?(?:steel\s+)?(?:shipped|produced)",
            r"(?:steel\s+)?(?:production|shipments?)\s*[:\s]*([\d,.]+)\s*[Mm]\s*(?:short\s+)?[Tt]ons?",
            r"([\d,.]+)\s*million\s+(?:short\s+)?tons?\s+(?:of\s+)?steel\s+(?:shipped|produced)",
            r"(?:total\s+)?(?:steel\s+)?(?:shipments?|tons?\s+shipped)\s*[:\s]*([\d,.]+)\s*[Mm](?:illion)?\s*[Tt]",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = self.parse_number(match.group(1))
                if val is None:
                    continue
                # Nucor reports in short tons; convert to metric
                context = match.group(0).lower()
                if "metric" in context:
                    val_mt = val
                else:
                    val_mt = round(val * SHORT_TON_TO_METRIC, 3)
                if 15 < val_mt < 30:
                    data.add(
                        metric="production_mt",
                        value=val_mt,
                        unit="Mt",
                        page=page_num,
                        method="regex_text",
                        confidence="high",
                        notes=f"Nucor shipped: {match.group(0)[:80]} → {val_mt} Mt",
                    )
                    return

    def _search_emissions(self, text: str, page_num: int, data: CompanyYearData):
        # Pattern 1: Scope 1 + Scope 2 separately in "M tCO2e" or similar
        scope1 = None
        scope2 = None

        # "Scope 1: 5.7M tCO2e" or "Scope 1 ... 5,700,000 tCO2e"
        s1_patterns = [
            r"scope\s+1[^.]*?([\d,.]+)\s*[Mm]\s*(?:metric\s+)?(?:t(?:ons?)?\s*)?co2",
            r"scope\s+1[^.]*?([\d,]+)\s*tco2",
        ]
        for pattern in s1_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = self.parse_number(match.group(1))
                if val is not None:
                    if val > 1000000:  # tCO2e
                        scope1 = val / 1000000.0
                    elif val < 50:  # Already in M/Mt
                        scope1 = val
                    break

        s2_patterns = [
            r"scope\s+2[^.]*?([\d,.]+)\s*[Mm]\s*(?:metric\s+)?(?:t(?:ons?)?\s*)?co2",
            r"scope\s+2[^.]*?([\d,]+)\s*tco2",
        ]
        for pattern in s2_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = self.parse_number(match.group(1))
                if val is not None:
                    if val > 1000000:
                        scope2 = val / 1000000.0
                    elif val < 50:
                        scope2 = val
                    break

        if scope1 is not None:
            total = round(scope1 + (scope2 or 0), 3)
            if 3 < total < 20:
                data.add(
                    metric="emissions_scope12_mt_co2",
                    value=total,
                    unit="Mt CO2",
                    page=page_num,
                    method="regex_text",
                    confidence="high",
                    notes=f"Nucor report: Scope1={scope1:.1f}, Scope2={scope2 or 0:.1f} Mt, total={total:.1f} Mt",
                )
                return

        # Pattern 2: Combined Scope 1+2
        combined_patterns = [
            r"(?:scope\s+1\s*(?:\+|and|&)\s*(?:scope\s+)?2)[^.]*?([\d,.]+)\s*(?:[Mm]\s*)?(?:metric\s+)?t(?:ons?)?\s*co2",
            r"(?:total\s+)?(?:ghg|greenhouse)\s+emissions?[^.]*?([\d,.]+)\s*(?:[Mm]\s*)?(?:metric\s+)?t",
        ]
        for pattern in combined_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = self.parse_number(match.group(1))
                if val is not None:
                    if val > 1000000:
                        val = val / 1000000.0
                    if 3 < val < 20:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=round(val, 3),
                            unit="Mt CO2",
                            page=page_num,
                            method="regex_text",
                            confidence="medium",
                            notes=f"Nucor combined: {match.group(0)[:80]}",
                        )
                        return

    def _search_table(self, table: list, page_num: int, data: CompanyYearData):
        has_production = self._has_production(data)
        has_emissions = self._has_emissions(data)

        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            # Production from tables
            if not has_production:
                if any(kw in row_text for kw in [
                    "steel shipped", "tons shipped", "shipments", "total production",
                ]):
                    if "intensity" in row_text or "per ton" in row_text:
                        continue
                    for cell in row[1:]:
                        val = self.parse_number(str(cell)) if cell else None
                        if val is None:
                            continue
                        # Million short tons range
                        if 15 < val < 35:
                            val_mt = round(val * SHORT_TON_TO_METRIC, 3)
                            if 15 < val_mt < 30:
                                data.add(
                                    metric="production_mt",
                                    value=val_mt,
                                    unit="Mt",
                                    page=page_num,
                                    table=row_idx,
                                    method="table_pdfplumber",
                                    confidence="high",
                                    notes=f"Nucor table: {row_text[:80]} → {val_mt} Mt",
                                )
                                has_production = True
                                break

            # Emissions from tables
            if not has_emissions:
                if any(kw in row_text for kw in ["scope 1", "scope", "co2", "ghg"]):
                    if "intensity" in row_text or "per ton" in row_text:
                        continue
                    for cell in row[1:]:
                        val = self.parse_number(str(cell)) if cell else None
                        if val is None:
                            continue
                        if val > 1000000:  # tCO2e
                            val_mt = round(val / 1000000.0, 3)
                            if 3 < val_mt < 20:
                                data.add(
                                    metric="emissions_scope12_mt_co2",
                                    value=val_mt,
                                    unit="Mt CO2",
                                    page=page_num,
                                    table=row_idx,
                                    method="table_pdfplumber",
                                    confidence="medium",
                                    notes=f"Nucor table: {val:,.0f} tCO2e = {val_mt} Mt",
                                )
                                has_emissions = True
                                break
