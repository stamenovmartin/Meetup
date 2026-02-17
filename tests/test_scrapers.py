"""
Tests for scrapers
"""
import pytest
from data_collection.scrapers.base_scraper import BaseScraper


class TestBaseScraper:
    """Tests for BaseScraper"""

    def test_generate_event_id(self):
        """Test event ID generation"""
        scraper = BaseScraper("http://example.com")

        id1 = scraper.generate_event_id("Test Event", "2025-01-01")
        id2 = scraper.generate_event_id("Test Event", "2025-01-01")
        id3 = scraper.generate_event_id("Different Event", "2025-01-01")

        # Same input should generate same ID
        assert id1 == id2
        # Different input should generate different ID
        assert id1 != id3
        # Should be MD5 hash (32 chars)
        assert len(id1) == 32

    def test_get_file_prefix(self):
        """Test file prefix generation"""
        scraper = BaseScraper("http://example.com")
        prefix = scraper.get_file_prefix()

        assert isinstance(prefix, str)
        assert 'scraper' not in prefix.lower()
