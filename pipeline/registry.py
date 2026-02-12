"""
URL Registry for company reports.
Maps company slugs to lists of report URLs with metadata.
Stored as JSON for easy manual editing and version control.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from .config import REPORT_URLS_FILE

logger = logging.getLogger(__name__)


def _default_registry() -> Dict[str, List[dict]]:
    """Initial registry with known working report URLs for priority companies."""
    return {
        "arcelormittal": [
            {
                "year": 2023,
                "url": "https://corporate.arcelormittal.com/media/vrqovnik/arcelor-mittal-integrated-annual-review-2023.pdf",
                "doc_type": "integrated_report",
                "company_name": "ArcelorMittal",
            },
            {
                "year": 2024,
                "url": "https://corporate.arcelormittal.com/media/3fwar2wu/2024-sustainability-report.pdf",
                "doc_type": "sustainability_report",
                "company_name": "ArcelorMittal",
            },
        ],
        "tata_steel": [
            {
                "year": 2024,
                "url": "https://www.tatasteel.com/media/21244/integrated-report-and-annual-accounts-fy2023-24.pdf",
                "doc_type": "integrated_report",
                "company_name": "Tata Steel",
            },
        ],
        "jsw_steel": [
            {
                "year": 2024,
                "url": "https://www.jswsteel.in/jsw-steel-annual-report-2023-24/JSW-Steel-IR24.pdf",
                "doc_type": "integrated_report",
                "company_name": "JSW Steel",
            },
        ],
        "nippon_steel": [
            {
                "year": 2024,
                "url": "https://www.nipponsteel.com/en/ir/library/pdf/nsc_en_ir_2024_all.pdf",
                "doc_type": "integrated_report",
                "company_name": "Nippon Steel",
            },
        ],
        "posco": [
            {
                "year": 2023,
                "url": "https://sustainability.posco.com/assets_eng/file/POSCO_Sustainability_Report_2023_eng.pdf",
                "doc_type": "sustainability_report",
                "company_name": "POSCO Holdings",
            },
        ],
        "nucor": [
            {
                "year": 2023,
                "url": "https://assets.ctfassets.net/aax1cfbwhqog/4fECDnhcDdCUAiSk6m7CD6/3d8c6dc0250564987e52425b0bac6fa8/Nucor_2023_Scopes1_2_Verification_Statement_-_SCS_Global.pdf",
                "doc_type": "sustainability_report",
                "company_name": "Nucor",
            },
        ],
        "ssab": [
            {
                "year": 2023,
                "url": "https://mb.cision.com/Public/980/3947371/89747ddd5471691f.pdf",
                "doc_type": "annual_report",
                "company_name": "SSAB",
            },
        ],
        "thyssenkrupp": [
            {
                "year": 2024,
                "url": "https://d2zo35mdb530wx.cloudfront.net/_binary/thyssenkruppAGReport/354e8b26-9f91-40f1-80cf-91207a65c9d0/Annual-report_2023-2024.pdf",
                "doc_type": "annual_report",
                "company_name": "ThyssenKrupp",
            },
        ],
        "jfe_holdings": [
            {
                "year": 2024,
                "url": "https://www.jfe-holdings.co.jp/en/common/pdf/investor/library/group-report/2024/all.pdf",
                "doc_type": "integrated_report",
                "company_name": "JFE Holdings",
            },
        ],
        "gerdau": [
            {
                "year": 2023,
                "url": "https://gsn.gerdau.com/sites/gsn_gerdau/files/PDF/annual-report-gerdau-en-2023.pdf",
                "doc_type": "integrated_report",
                "company_name": "Gerdau",
            },
        ],
    }


class ReportRegistry:
    """Manages the URL registry for company reports."""

    def __init__(self, registry_path: Optional[Path] = None):
        self.path = registry_path or REPORT_URLS_FILE
        self.data = self._load()

    def _load(self) -> Dict[str, List[dict]]:
        if self.path.exists():
            with open(self.path, "r") as f:
                return json.load(f)
        logger.info("No registry file found, using defaults")
        data = _default_registry()
        self._save(data)
        return data

    def _save(self, data: Optional[dict] = None):
        data = data or self.data
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Registry saved to {self.path}")

    def get_reports(self, company_slug: str) -> List[dict]:
        return self.data.get(company_slug, [])

    def get_all_companies(self) -> List[str]:
        return list(self.data.keys())

    def add_report(self, company_slug: str, year: int, url: str,
                   doc_type: str, company_name: str):
        if company_slug not in self.data:
            self.data[company_slug] = []
        # Avoid duplicates
        for entry in self.data[company_slug]:
            if entry["year"] == year and entry["url"] == url:
                return
        self.data[company_slug].append({
            "year": year,
            "url": url,
            "doc_type": doc_type,
            "company_name": company_name,
        })
        self._save()

    def save(self):
        self._save()
