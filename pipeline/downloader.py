"""
PDF Downloader with manifest tracking.
Downloads reports, computes SHA256 hashes, and maintains a manifest
so we never re-download the same file.
"""

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Optional

import requests

from .config import ANNUAL_REPORTS_DIR, DOWNLOAD_MANIFEST_FILE, REQUEST_HEADERS, REQUEST_TIMEOUT
from .models import SourceInfo

logger = logging.getLogger(__name__)


def _compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _slugify(name: str) -> str:
    return name.lower().replace(" ", "_").replace("-", "_")


class ReportDownloader:
    """Downloads PDFs and tracks them in a manifest."""

    def __init__(self, base_dir: Optional[Path] = None,
                 manifest_path: Optional[Path] = None):
        self.base_dir = base_dir or ANNUAL_REPORTS_DIR
        self.manifest_path = manifest_path or DOWNLOAD_MANIFEST_FILE
        self.manifest = self._load_manifest()

    def _load_manifest(self) -> dict:
        if self.manifest_path.exists():
            with open(self.manifest_path, "r") as f:
                return json.load(f)
        return {"downloads": []}

    def _save_manifest(self):
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, "w") as f:
            json.dump(self.manifest, f, indent=2)

    def _is_already_downloaded(self, url: str, company_slug: str, year: int) -> Optional[str]:
        """Check if this report was already downloaded. Returns local path if found."""
        for entry in self.manifest["downloads"]:
            if entry["url"] == url and entry["company_slug"] == company_slug:
                local = Path(entry["local_path"])
                if local.exists():
                    return str(local)
        return None

    def download(self, url: str, company_slug: str, year: int,
                 doc_type: str, company_name: str, sector: str = "steel") -> Optional[SourceInfo]:
        """Download a PDF report. Returns SourceInfo or None on failure."""

        # Check if already downloaded
        existing = self._is_already_downloaded(url, company_slug, year)
        if existing:
            logger.info(f"Already downloaded: {existing}")
            sha = _compute_sha256(Path(existing))
            from .config import PROJECT_ROOT
            try:
                rel_path = str(Path(existing).relative_to(PROJECT_ROOT))
            except ValueError:
                rel_path = str(existing)
            return SourceInfo(
                url=url,
                doc_type=doc_type,
                company=company_name,
                year=year,
                local_path=rel_path,
                sha256=sha,
                download_date=self._get_manifest_date(url),
                file_size_bytes=Path(existing).stat().st_size,
            )

        # Create target directory
        company_dir = self.base_dir / sector / company_slug
        company_dir.mkdir(parents=True, exist_ok=True)

        # Determine filename from URL
        url_filename = url.split("/")[-1].split("?")[0]
        if not url_filename.lower().endswith(".pdf"):
            url_filename = f"{company_slug}_{doc_type}_{year}.pdf"
        local_path = company_dir / url_filename

        # Download
        logger.info(f"Downloading: {url}")
        try:
            resp = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT,
                                stream=True, allow_redirects=True)
            resp.raise_for_status()

            # Verify we got a PDF (check content-type and magic bytes)
            content_type = resp.headers.get("Content-Type", "")
            first_chunk = None

            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if first_chunk is None:
                        first_chunk = chunk
                    f.write(chunk)

            # Verify it's actually a PDF
            if first_chunk and not first_chunk[:5] == b"%PDF-":
                logger.warning(f"Downloaded file is not a PDF (starts with {first_chunk[:20]}). Removing.")
                local_path.unlink(missing_ok=True)
                return None

            sha = _compute_sha256(local_path)
            file_size = local_path.stat().st_size
            download_date = time.strftime("%Y-%m-%d %H:%M:%S")

            # Relative path from project root
            try:
                from .config import PROJECT_ROOT
                rel_path = str(local_path.relative_to(PROJECT_ROOT))
            except ValueError:
                rel_path = str(local_path)

            # Update manifest
            self.manifest["downloads"].append({
                "url": url,
                "company_slug": company_slug,
                "company_name": company_name,
                "year": year,
                "doc_type": doc_type,
                "local_path": str(local_path),
                "relative_path": rel_path,
                "sha256": sha,
                "file_size_bytes": file_size,
                "download_date": download_date,
            })
            self._save_manifest()

            logger.info(f"Downloaded {file_size / 1024 / 1024:.1f} MB -> {local_path.name}")

            return SourceInfo(
                url=url,
                doc_type=doc_type,
                company=company_name,
                year=year,
                local_path=rel_path,
                sha256=sha,
                download_date=download_date,
                file_size_bytes=file_size,
            )

        except requests.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            local_path.unlink(missing_ok=True)
            return None

    def _get_manifest_date(self, url: str) -> str:
        for entry in self.manifest["downloads"]:
            if entry["url"] == url:
                return entry.get("download_date", "")
        return ""

    def get_all_downloaded(self) -> list:
        return self.manifest["downloads"]
