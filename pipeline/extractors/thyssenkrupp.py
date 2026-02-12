"""
ThyssenKrupp-specific extractor.
Handles Annual Report format.

Known report format (2023-2024 Annual Report):
- Page 65: "Crude steel production ... came to 10.3 mil-lion tons"
  "shipments were also 5% lower year-on-year at 9.0 million tons"
- Page 88: "Scope 1 and Scope 2 emissions ... came to around 23.2 million tons COe"
"""

import logging
import re
from pathlib import Path

from ..base_extractor import BaseExtractor
from ..models import CompanyYearData, SourceInfo

logger = logging.getLogger(__name__)


class ThyssenKruppExtractor(BaseExtractor):
    company_name = "ThyssenKrupp"
    company_slug = "thyssenkrupp"

    def extract(self, pdf_path: Path, source: SourceInfo) -> CompanyYearData:
        logger.info(f"Running ThyssenKrupp extraction on {pdf_path.name}")
        data = CompanyYearData(company=self.company_name, year=source.year, source=source)

        # ThyssenKrupp data is in flowing text, not tables
        all_text = self.extract_text(pdf_path)

        # Build multi-line text blocks for cross-line matching
        for page_num, text in all_text:
            # Rejoin hyphenated words split across lines (e.g., "mil-\nlion")
            clean_text = re.sub(r"-\n\s*", "", text)
            self._search_tk_data(clean_text, page_num, data)

        if not data.data_points:
            return self.try_generic_extraction(pdf_path, source)

        return data

    def _search_tk_data(self, text: str, page_num: int, data: CompanyYearData):
        has_production = any(dp.metric == "production_mt" for dp in data.data_points)
        has_emissions = any(dp.metric.startswith("emissions") for dp in data.data_points)

        # Production: "Crude steel production ... came to 10.3 million tons"
        # or "crude steel production ... 10.3 million tons"
        if not has_production:
            patterns = [
                r"crude\s+steel\s+production[^.]*?(\d+\.?\d*)\s*million\s*tons?",
                r"steel\s+production[^.]*?came\s+to\s+(\d+\.?\d*)\s*million\s*tons?",
                r"production\s+capacity\s+(?:of|for)\s+(\d+\.?\d*)\s*million\s*tons?",
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    val = float(match.group(1))
                    if 5 < val < 20:  # ThyssenKrupp ~10-11 Mt
                        data.add(
                            metric="production_mt",
                            value=val,
                            unit="Mt",
                            page=page_num,
                            method="regex_text",
                            confidence="high",
                            notes=f"ThyssenKrupp crude steel production: {val} million tons",
                        )
                        has_production = True
                        break

        # Emissions: "Scope 1 and Scope 2 emissions ... came to around 23.2 million tons COe"
        # Note: this is company-wide, not just steel segment
        if not has_emissions:
            patterns = [
                r"scope\s+1\s+and\s+scope\s+2\s+emis[^.]*?(\d+\.?\d*)\s*million\s*tons?\s*co",
                r"greenhouse\s+gas\s+emissions[^.]*?(\d+\.?\d*)\s*million\s*tons?\s*co",
                r"ghg\s+emissions[^.]*?came\s+to\s+.*?(\d+\.?\d*)\s*million\s*tons?",
            ]
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    val = float(match.group(1))
                    if 10 < val < 50:  # ThyssenKrupp ~20-25 Mt (whole company)
                        data.add(
                            metric="emissions_scope12_mt_co2",
                            value=val,
                            unit="Mt CO2",
                            page=page_num,
                            method="regex_text",
                            confidence="high",
                            notes=f"ThyssenKrupp Scope 1+2: {val} million tons CO2e (company-wide)",
                        )
                        has_emissions = True
                        break
