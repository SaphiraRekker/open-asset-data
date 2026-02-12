"""
Cleveland-Cliffs extractor.
Handles US steelmaker reports (calendar year).

Known report format:
- Production reported as "steel shipments" in "million net tons" (US short tons)
- Emissions reported as intensity: metric tons CO2e per metric ton crude steel
- Conversion: 1 short/net ton = 0.907185 metric tonnes
- Only a steel company since 2020 (AK Steel + AM USA acquisitions)
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)

SHORT_TON_TO_METRIC = 0.907185


class ClevelandCliffsExtractor(BaseExtractor):
    company_name = "Cleveland-Cliffs"
    company_slug = "cleveland_cliffs"

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running Cleveland-Cliffs extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        all_text = self.extract_text(pdf_path)
        for page_num, text in all_text:
            clean_text = re.sub(r"-\n\s*", "", text)
            self._search_text(clean_text, page_num, data)

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

    def _search_text(self, text: str, page_num: int, data: CompanyYearData):
        if not self._has_production(data):
            self._search_production_text(text, page_num, data)

        if not self._has_emissions(data):
            self._search_emissions_text(text, page_num, data)

    def _normalize_text(self, text: str) -> str:
        """Insert spaces between digits and letters for poorly-extracted PDFs."""
        t = re.sub(r'([a-z])(\d)', r'\1 \2', text)
        t = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', t)
        # Also split common concatenated word patterns in 10-K filings
        # e.g. "producedatotalof" → "produced a total of"
        for word in ['produced', 'capacity', 'approximately', 'million',
                      'steel', 'total', 'raw', 'net', 'tons', 'shipped']:
            t = re.sub(rf'(?i)({word})([a-z])', r'\1 \2', t)
            t = re.sub(rf'(?i)([a-z])({word})', r'\1 \2', t)
        return t

    def _search_production_text(self, text: str, page_num: int,
                                data: CompanyYearData):
        norm_text = self._normalize_text(text)

        # Pattern 1: "steel shipments of X million net/short tons"
        patterns = [
            r"steel\s+shipments?\s+(?:of\s+)?([\d,.]+)\s*million\s+(?:net\s+)?tons?",
            r"([\d,.]+)\s*million\s+(?:net\s+)?tons?\s+(?:of\s+)?steel\s+(?:shipped|shipments?|produced|production)",
            r"shipped\s+([\d,.]+)\s*million\s+(?:net\s+)?tons?",
            r"steel\s+(?:production|output)\s+(?:of\s+)?([\d,.]+)\s*million\s+(?:net\s+)?tons?",
            r"([\d,.]+)\s*million\s+(?:net\s+)?tons?\s+(?:of\s+)?(?:crude\s+|raw\s+)?steel",
            r"produced\s+(?:a\s+total\s+of\s+)?([\d,.]+)\s*million\s+(?:(?:net|short)\s+)?tons?\s+(?:of\s+)?(?:raw\s+)?steel",
            r"produced\s+([\d,.]+)\s*million\s+(?:(?:net|short)\s+)?tons?\s+(?:of\s+)?(?:raw\s+)?steel",
            # "produced a total of 16.8 million and 18.3 million net tons of raw steel" (first value = current year)
            r"produced\s+(?:a\s+total\s+of\s+)?([\d,.]+)\s*million\s+and\s+[\d,.]+\s*million\s+(?:(?:net|short)\s+)?tons?\s+(?:of\s+)?(?:raw\s+)?steel",
        ]
        for pattern in patterns:
            match = re.search(pattern, norm_text, re.IGNORECASE)
            if match:
                # Skip capacity descriptions
                start = max(0, match.start() - 80)
                surrounding = norm_text[start:match.end() + 40].lower()
                if any(kw in surrounding for kw in [
                    "capab", "capacity", "rated", "configured",
                    "annual rated", "annually",
                ]):
                    continue
                val = self.parse_number(match.group(1))
                if val is None:
                    continue
                # Determine if short tons or metric
                context = match.group(0).lower()
                if "net ton" in context or "short ton" in context:
                    val_mt = round(val * SHORT_TON_TO_METRIC, 3)
                elif "metric" in context:
                    val_mt = val
                else:
                    # US company, assume short tons
                    val_mt = round(val * SHORT_TON_TO_METRIC, 3)
                if 3 < val_mt < 25:
                    data.add(
                        metric="production_mt",
                        value=val_mt,
                        unit="Mt",
                        page=page_num,
                        method="regex_text",
                        confidence="high",
                        notes=f"Cleveland-Cliffs shipments: {match.group(0)[:80]} → {val_mt} Mt",
                    )
                    return

        # Pattern 2: "X.X million metric tons" (some reports use metric directly)
        metric_patterns = [
            r"([\d,.]+)\s*million\s+metric\s+tons?\s+(?:of\s+)?steel",
            r"steel[^.]*?([\d,.]+)\s*million\s+metric\s+tons?",
        ]
        for pattern in metric_patterns:
            match = re.search(pattern, norm_text, re.IGNORECASE)
            if match:
                val = self.parse_number(match.group(1))
                if val is not None and 3 < val < 25:
                    data.add(
                        metric="production_mt",
                        value=round(val, 3),
                        unit="Mt",
                        page=page_num,
                        method="regex_text",
                        confidence="high",
                        notes=f"Cleveland-Cliffs metric tons: {match.group(0)[:80]}",
                    )
                    return

    def _search_emissions_text(self, text: str, page_num: int,
                               data: CompanyYearData):
        # Pattern 1: Absolute Scope 1+2 emissions
        abs_patterns = [
            r"(?:scope\s+1\s*(?:\+|and|&)\s*(?:scope\s+)?2)[^.]*?([\d,.]+)\s*(?:million\s+)?(?:metric\s+)?(?:mt|t)",
            r"(?:total\s+)?(?:ghg|greenhouse)\s+emissions?[^.]*?([\d,.]+)\s*(?:million\s+)?(?:metric\s+)?(?:mt|t)",
        ]
        for pattern in abs_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = self.parse_number(match.group(1))
                if val is not None:
                    # Could be in tCO2e (millions) or Mt
                    if val > 1000000:
                        val = val / 1000000.0
                    if 10 < val < 50:
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=round(val, 3),
                            unit="Mt CO2",
                            page=page_num,
                            method="regex_text",
                            confidence="high",
                            notes=f"Cleveland-Cliffs absolute emissions: {match.group(0)[:80]}",
                        )
                        return

        # Pattern 2: Intensity + production → derive absolute
        intensity_patterns = [
            r"([\d,.]+)\s*(?:metric\s+)?tons?\s*co2e?\s*(?:per|/)\s*(?:metric\s+)?ton",
            r"(?:emissions?\s+)?intensity[:\s]*([\d,.]+)\s*(?:mt?\s*co2|tco2)",
            r"(?:average\s+)?emissions?\s+intensity[^.]*?([\d,.]+)\s*(?:metric\s+)?ton",
        ]
        for pattern in intensity_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                intensity = self.parse_number(match.group(1))
                if intensity is not None and 1.0 < intensity < 3.0:
                    production_dp = next(
                        (dp for dp in data.data_points if dp.metric == "production_mt"),
                        None,
                    )
                    if production_dp:
                        absolute = round(intensity * production_dp.value, 3)
                        if 10 < absolute < 50:
                            data.add(
                                metric="emissions_scope12_mt_co2",
                                value=absolute,
                                unit="Mt CO2",
                                page=page_num,
                                method="regex_text",
                                confidence="medium",
                                notes=f"Cleveland-Cliffs derived: {intensity} tCO2e/t × {production_dp.value} Mt = {absolute} Mt",
                            )
                            return

    def _search_table(self, table: list, page_num: int, data: CompanyYearData):
        has_production = self._has_production(data)
        has_emissions = self._has_emissions(data)

        for row_idx, row in enumerate(table):
            if not row:
                continue
            row_text = " ".join(str(cell) for cell in row if cell).lower()

            # Production
            if not has_production:
                if any(kw in row_text for kw in [
                    "steel shipment", "steel production", "crude steel",
                    "raw steel", "net tons shipped",
                ]):
                    if "intensity" in row_text or "per ton" in row_text:
                        continue
                    for cell in row[1:]:
                        val = self.parse_number(str(cell)) if cell else None
                        if val is None:
                            continue
                        # Million net/short tons range
                        if 3 < val < 25:
                            # Determine if short or metric from row context
                            if "net" in row_text or "short" in row_text:
                                val_mt = round(val * SHORT_TON_TO_METRIC, 3)
                            elif "metric" in row_text:
                                val_mt = round(val, 3)
                            else:
                                val_mt = round(val * SHORT_TON_TO_METRIC, 3)
                            if 3 < val_mt < 25:
                                data.add(
                                    metric="production_mt",
                                    value=val_mt,
                                    unit="Mt",
                                    page=page_num,
                                    table=row_idx,
                                    method="table_pdfplumber",
                                    confidence="high",
                                    notes=f"Cleveland-Cliffs table: {row_text[:80]}",
                                )
                                has_production = True
                                break

            # Emissions
            if not has_emissions:
                if any(kw in row_text for kw in [
                    "scope 1", "co2", "ghg", "greenhouse", "carbon",
                ]):
                    if "intensity" in row_text or "per ton" in row_text:
                        continue
                    for cell in row[1:]:
                        val = self.parse_number(str(cell)) if cell else None
                        if val is None:
                            continue
                        if val > 1000000:  # tCO2e
                            val_mt = round(val / 1000000.0, 3)
                        elif 10 < val < 50:  # Already Mt
                            val_mt = round(val, 3)
                        else:
                            continue
                        if 10 < val_mt < 50:
                            data.add(
                                metric="emissions_scope12_mt_co2",
                                value=val_mt,
                                unit="Mt CO2",
                                page=page_num,
                                table=row_idx,
                                method="table_pdfplumber",
                                confidence="high",
                                notes=f"Cleveland-Cliffs table emissions: {row_text[:80]}",
                            )
                            has_emissions = True
                            break
