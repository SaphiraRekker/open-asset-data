"""
Data models for the report pipeline.
Every extracted value carries full provenance information.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional


@dataclass
class SourceInfo:
    """Provenance information for a downloaded report."""
    url: str
    doc_type: str  # "integrated_report", "sustainability_report", "annual_report", "climate_report"
    company: str
    year: int
    local_path: str = ""
    sha256: str = ""
    download_date: str = ""
    file_size_bytes: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DataPoint:
    """A single extracted data value with full provenance."""
    company: str
    year: int
    metric: str  # "production_mt", "emissions_scope1_mt_co2", "emissions_scope12_mt_co2", "revenue_usd_m", etc.
    value: float
    unit: str  # "Mt", "kt", "tCO2", "Mt CO2", "USD million", etc.
    source_pdf: str  # relative path to the PDF
    source_page: Optional[int] = None
    source_table: Optional[int] = None  # table index on page if from table extraction
    extraction_method: str = "unknown"  # "table_pdfplumber", "regex_text", "manual_override"
    sha256: str = ""
    extracted_date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))
    confidence: str = "medium"  # "high", "medium", "low"
    notes: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CompanyYearData:
    """All extracted data for a company-year combination."""
    company: str
    year: int
    source: SourceInfo
    data_points: list = field(default_factory=list)  # List[DataPoint]

    def add(self, metric: str, value: float, unit: str, page: Optional[int] = None,
            table: Optional[int] = None, method: str = "unknown",
            confidence: str = "medium", notes: str = "",
            year_override: Optional[int] = None) -> DataPoint:
        dp = DataPoint(
            company=self.company,
            year=year_override if year_override is not None else self.year,
            metric=metric,
            value=value,
            unit=unit,
            source_pdf=self.source.local_path,
            source_page=page,
            source_table=table,
            extraction_method=method,
            sha256=self.source.sha256,
            confidence=confidence,
            notes=notes,
        )
        self.data_points.append(dp)
        return dp

    def to_dicts(self) -> list:
        return [dp.to_dict() for dp in self.data_points]
