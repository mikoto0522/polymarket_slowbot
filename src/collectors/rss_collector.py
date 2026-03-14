from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from ..utils.db import Database
from ..utils.time import iso_utc


@dataclass
class SourceInfo:
    tier: int
    label: str


class SourceClassifier:
    def __init__(self, whitelist_config: dict[str, Any]) -> None:
        self.tier_1 = set(whitelist_config.get("tier_1_official", []))
        self.tier_2 = set(whitelist_config.get("tier_2_mainstream_media", []))
        self.tier_3 = set(whitelist_config.get("tier_3_social", []))
        self.blacklist = set(whitelist_config.get("blacklist", []))

    def classify(self, url: str) -> SourceInfo:
        domain = (urlparse(url).hostname or "").lower()
        if domain.startswith("www."):
            domain = domain[4:]
        if domain in self.blacklist:
            return SourceInfo(0, "untrusted")
        if domain in self.tier_1:
            return SourceInfo(3, "trusted")
        if domain in self.tier_2:
            return SourceInfo(2, "mainstream")
        if domain in self.tier_3:
            return SourceInfo(1, "social_signal")
        return SourceInfo(1, "unknown")


class RSSCollector:
    def __init__(self, rss_urls: list[str], classifier: SourceClassifier) -> None:
        self.rss_urls = rss_urls
        self.classifier = classifier

    @staticmethod
    def _request_xml(url: str) -> str:
        req = Request(url, headers={"User-Agent": "curl/8.0", "Accept": "application/xml"})
        with urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="ignore")

    @staticmethod
    def _safe_find_text(node: ET.Element, names: list[str]) -> str:
        for name in names:
            child = node.find(name)
            if child is not None and child.text:
                return child.text.strip()
        return ""

    def _parse_rss(self, xml_text: str) -> list[dict[str, str]]:
        root = ET.fromstring(xml_text)
        items: list[dict[str, str]] = []

        channel_items = root.findall(".//channel/item")
        if channel_items:
            for item in channel_items:
                items.append(
                    {
                        "title": self._safe_find_text(item, ["title"]),
                        "link": self._safe_find_text(item, ["link"]),
                        "description": self._safe_find_text(item, ["description"]),
                        "pub_date": self._safe_find_text(item, ["pubDate"]),
                        "author": self._safe_find_text(item, ["author"]),
                    }
                )
            return items

        atom_entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
        for entry in atom_entries:
            link = ""
            for link_node in entry.findall("{http://www.w3.org/2005/Atom}link"):
                href = link_node.attrib.get("href")
                if href:
                    link = href
                    break
            items.append(
                {
                    "title": self._safe_find_text(entry, ["{http://www.w3.org/2005/Atom}title"]),
                    "link": link,
                    "description": self._safe_find_text(
                        entry, ["{http://www.w3.org/2005/Atom}summary"]
                    ),
                    "pub_date": self._safe_find_text(
                        entry, ["{http://www.w3.org/2005/Atom}updated"]
                    ),
                    "author": self._safe_find_text(
                        entry, ["{http://www.w3.org/2005/Atom}author/{http://www.w3.org/2005/Atom}name"]
                    ),
                }
            )
        return items

    def ingest(self, db: Database) -> dict[str, int]:
        fetched = 0
        inserted = 0
        for feed_url in self.rss_urls:
            try:
                xml_text = self._request_xml(feed_url)
                entries = self._parse_rss(xml_text)
            except Exception:
                continue

            for entry in entries:
                title = entry.get("title", "").strip()
                body = entry.get("description", "").strip()
                url = entry.get("link", "").strip()
                if not title or not url:
                    continue
                fetched += 1
                classification = self.classifier.classify(url)
                raw_hash = hashlib.sha256(f"{title}\n{body}\n{url}".encode("utf-8")).hexdigest()

                row = {
                    "source": urlparse(url).hostname or "unknown",
                    "author": entry.get("author") or None,
                    "publisher": urlparse(url).hostname or None,
                    "title": title,
                    "body": body,
                    "url": url,
                    "publish_time": entry.get("pub_date") or None,
                    "first_seen_time": iso_utc(),
                    "tags_json": json.dumps([], ensure_ascii=False),
                    "raw_text_hash": raw_hash,
                    "source_tier": classification.tier,
                    "source_classification": classification.label,
                }
                if db.insert_external_document(row):
                    inserted += 1
        return {"fetched": fetched, "inserted": inserted}
