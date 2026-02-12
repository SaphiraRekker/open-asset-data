"""
BlueScope Steel extractor.
Handles Australian/US steelmaker reports (FY Jul-Jun).

Known report format:
- Production reported as "despatch volumes" in kt (1,000 tonnes)
- Emissions often reported as intensity (tCO2e per tonne raw steel)
- Key assets: Port Kembla (BF-BOF, ~3 Mt/yr), North Star (EAF, ~2.5 Mt/yr)
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class BlueScopeExtractor(BaseExtractor):
    company_name = "BlueScope Steel"
    company_slug = "bluescope_steel"

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running BlueScope extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        all_text = self.extract_text(pdf_path)
        for page_num, text in all_text:
            self._search_text(text, page_num, data)

        if not self._has_production(data):
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

    def _search_text(self, text: str, page_num: int, data: CompanyYearData):
        lines = text.split("\n")
        clean_text = re.sub(r"-\n\s*", "", text)

        if not self._has_production(data):
            self._search_production_text(clean_text, lines, page_num, data)

        if not self._has_emissions(data):
            self._search_emissions_text(clean_text, lines, page_num, data)

    def _search_production_text(self, text: str, lines: list, page_num: int,
                                data: CompanyYearData):
        # Pattern 1: "despatch volumes" or "despatches" with kt values
        patterns = [
            r"(?:total\s+)?despatch(?:es|ed)?\s*(?:volumes?)?\s*[:\s]*([\d,]+\.?\d*)\s*(?:kt|thousand\s*t)",
            r"(?:raw\s+)?steel\s+(?:slab\s+)?production\s*[:\s]*([\d,]+\.?\d*)\s*(?:kt|thousand\s*t)",
            r"steelmaking\s+production\s*[:\s]*([\d,]+\.?\d*)\s*(?:kt|thousand\s*t)",
            # Mt format
            r"(?:total\s+)?despatch(?:es|ed)?\s*(?:volumes?)?\s*[:\s]*([\d,]+\.?\d*)\s*(?:mt|million\s*t)",
            r"(?:raw\s+)?steel\s+(?:slab\s+)?production\s*[:\s]*([\d,]+\.?\d*)\s*(?:mt|million\s*t)",
            # "X,XXX kt" or "X,XXX thousand tonnes" near despatch/production context
            r"(?:despatch|production|output)[^\n]*?([\d,]+\.?\d*)\s*(?:kt|thousand\s*tonn)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = self.parse_number(match.group(1))
                if val is None:
                    continue
                # Determine if kt or Mt
                unit_part = match.group(0).lower()
                if "kt" in unit_part or "thousand" in unit_part:
                    val_mt = val / 1000.0
                else:
                    val_mt = val
                if 2 < val_mt < 8:
                    data.add(
                        metric="production_mt",
                        value=round(val_mt, 3),
                        unit="Mt",
                        page=page_num,
                        method="regex_text",
                        confidence="high",
                        notes=f"BlueScope despatch/production: {match.group(0)[:80]}",
                    )
                    return

        # Pattern 2: "X million tonnes" near steel/production context
        patterns_mt = [
            r"(\d+\.?\d*)\s*million\s*tonn\w*\s+(?:of\s+)?(?:crude\s+)?(?:raw\s+)?steel",
            r"(?:crude|raw)\s+steel[^.]*?(\d+\.?\d*)\s*million\s*tonn",
            r"(?:production|output)[^.]*?(\d+\.?\d*)\s*million\s*tonn",
        ]
        for pattern in patterns_mt:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Skip capacity mentions
                start = max(0, match.start() - 60)
                surrounding = text[start:match.end()].lower()
                if any(kw in surrounding for kw in [
                    "capacity", "capable", "annual production capacity",
                ]):
                    continue
                val = float(match.group(1))
                if 2 < val < 8:
                    data.add(
                        metric="production_mt",
                        value=round(val, 3),
                        unit="Mt",
                        page=page_num,
                        method="regex_text",
                        confidence="medium",
                        notes=f"BlueScope million tonnes: {match.group(0)[:80]}",
                    )
                    return

    def _search_emissions_text(self, text: str, lines: list, page_num: int,
                               data: CompanyYearData):
        # Pattern 1: Absolute Scope 1+2 emissions
        abs_patterns = [
            r"(?:scope\s+1\s*(?:\+|and|&)\s*(?:scope\s+)?2)[^.]*?([\d,.]+)\s*(?:mt|million\s*t)",
            r"(?:total\s+)?(?:ghg|greenhouse)\s+emissions?[^.]*?([\d,.]+)\s*(?:mt|million\s*t)",
            r"(?:co2|carbon)\s+emissions?[^.]*?([\d,.]+)\s*(?:mt|million\s*t)",
        ]
        for pattern in abs_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Skip historical reduction context: "reduced...down to X Mt"
                start = max(0, match.start() - 100)
                surrounding = text[start:match.end()].lower()
                if any(kw in surrounding for kw in [
                    "reduced", "down to", "down from", "since fy",
                    "since 20", "from fy", "compared to",
                ]):
                    continue
                val = self.parse_number(match.group(1))
                if val is not None and 2 < val < 20:
                    data.add(
                        metric="emissions_scope12_mt_co2",
                        value=round(val, 3),
                        unit="Mt CO2",
                        page=page_num,
                        method="regex_text",
                        confidence="high",
                        notes=f"BlueScope absolute emissions: {match.group(0)[:80]}",
                    )
                    return

        # Pattern 2: Absolute in kt (thousand tonnes)
        kt_patterns = [
            r"(?:scope\s+1\s*(?:\+|and|&)\s*(?:scope\s+)?2)[^.]*?([\d,]+)\s*(?:kt|thousand\s*t)",
            r"(?:total\s+)?(?:ghg|greenhouse)\s+emissions?[^.]*?([\d,]+)\s*(?:kt|thousand\s*t)",
        ]
        for pattern in kt_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = self.parse_number(match.group(1))
                if val is not None and 2000 < val < 20000:
                    data.add(
                        metric="emissions_scope12_mt_co2",
                        value=round(val / 1000.0, 3),
                        unit="Mt CO2",
                        page=page_num,
                        method="regex_text",
                        confidence="high",
                        notes=f"BlueScope emissions (kt): {match.group(0)[:80]}",
                    )
                    return

        # Pattern 3: Intensity (tCO2e per tonne) — derive absolute if production available
        intensity_patterns = [
            r"([\d,.]+)\s*(?:t\s*co2e?\s*(?:per|/)\s*(?:tonne|t)\s+(?:of\s+)?(?:raw|crude)\s+steel)",
            r"(?:emissions?\s+)?intensity[:\s]*([\d,.]+)\s*(?:tco2e?\s*(?:per|/)\s*t)",
            r"([\d,.]+)\s*tonn\w*\s+(?:of\s+)?co2[^.]*?(?:per|/)\s*(?:tonne|t)",
        ]
        for pattern in intensity_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                intensity = self.parse_number(match.group(1))
                if intensity is not None and 0.5 < intensity < 3.0:
                    # Try to compute absolute from production
                    production_dp = next(
                        (dp for dp in data.data_points if dp.metric == "production_mt"),
                        None,
                    )
                    if production_dp:
                        absolute = round(intensity * production_dp.value, 3)
                        if 2 < absolute < 20:
                            data.add(
                                metric="emissions_scope12_mt_co2",
                                value=absolute,
                                unit="Mt CO2",
                                page=page_num,
                                method="regex_text",
                                confidence="medium",
                                notes=f"BlueScope derived: {intensity} tCO2e/t × {production_dp.value} Mt = {absolute} Mt",
                            )
                            return

    def _search_table(self, table: list, page_num: int, data: CompanyYearData):
        has_production = self._has_production(data)
        has_emissions = self._has_emissions(data)

        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            # Production: despatch, production, output rows
            if not has_production:
                if any(kw in row_text for kw in [
                    "despatch", "dispatch", "raw steel", "steel slab",
                    "steelmaking production", "steel production",
                ]):
                    if "intensity" in row_text or "per tonne" in row_text:
                        continue
                    for cell in row[1:]:
                        val = self.parse_number(str(cell)) if cell else None
                        if val is None:
                            continue
                        # kt range (1000-6000)
                        if 1000 < val < 6000:
                            data.add(
                                metric="production_mt",
                                value=round(val / 1000.0, 3),
                                unit="Mt",
                                page=page_num,
                                table=row_idx,
                                method="table_pdfplumber",
                                confidence="high",
                                notes=f"BlueScope table (kt): {row_text[:80]}",
                            )
                            has_production = True
                            break
                        # Mt range (2-8)
                        elif 2 < val < 8:
                            data.add(
                                metric="production_mt",
                                value=round(val, 3),
                                unit="Mt",
                                page=page_num,
                                table=row_idx,
                                method="table_pdfplumber",
                                confidence="high",
                                notes=f"BlueScope table (Mt): {row_text[:80]}",
                            )
                            has_production = True
                            break

            # Emissions
            if not has_emissions:
                if any(kw in row_text for kw in [
                    "scope 1", "co2", "ghg", "greenhouse", "carbon dioxide",
                ]):
                    if "intensity" in row_text or "per tonne" in row_text:
                        continue
                    for cell in row[1:]:
                        val = self.parse_number(str(cell)) if cell else None
                        if val is None:
                            continue
                        # kt range (2000-20000)
                        if 2000 < val < 20000:
                            data.add(
                                metric="emissions_scope12_mt_co2",
                                value=round(val / 1000.0, 3),
                                unit="Mt CO2",
                                page=page_num,
                                table=row_idx,
                                method="table_pdfplumber",
                                confidence="high",
                                notes=f"BlueScope table emissions (kt): {row_text[:80]}",
                            )
                            has_emissions = True
                            break
                        # Mt range (2-20)
                        elif 2 < val < 20:
                            data.add(
                                metric="emissions_scope12_mt_co2",
                                value=round(val, 3),
                                unit="Mt CO2",
                                page=page_num,
                                table=row_idx,
                                method="table_pdfplumber",
                                confidence="high",
                                notes=f"BlueScope table emissions (Mt): {row_text[:80]}",
                            )
                            has_emissions = True
                            break
