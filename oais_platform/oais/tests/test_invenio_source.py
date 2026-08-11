import datetime
from unittest.mock import MagicMock, patch

from rest_framework.test import APITestCase

from oais_platform.oais.enums import FilterType
from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.invenio import Invenio


class InvenioSourceTests(APITestCase):
    def test_invenio_initialization(self):
        invenio_client = Invenio(
            "cds-rdm-sandbox",
            "https://sandbox-cds-rdm.web.cern.ch/api",
            token="test-token",
        )
        self.assertEqual(invenio_client.source, "cds-rdm-sandbox")
        self.assertEqual(
            invenio_client.baseURL, "https://sandbox-cds-rdm.web.cern.ch/api"
        )
        self.assertIn("Authorization", invenio_client.headers)
        self.assertEqual(invenio_client.headers["Authorization"], "Bearer test-token")

    @patch("oais_platform.oais.sources.invenio.Invenio._get_session")
    def test_search_success(self, mock_get_session):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.text = '{"hits": {"total": 1, "hits": [{"id": "123", "metadata": {"title": "Test Title"}}]}}'
        mock_session.get.return_value = mock_response
        mock_get_session.return_value = mock_session

        client = Invenio("cds-rdm-sandbox", "https://sandbox-cds-rdm.web.cern.ch/api")

        result = client.search("test-query")

        self.assertEqual(result["total_num_hits"], 1)
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["recid"], "123")
        self.assertEqual(result["results"][0]["title"], "Test Title")

    @patch("oais_platform.oais.sources.invenio.Invenio._get_session")
    def test_search_service_unavailable(self, mock_get_session):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_session.get.return_value = mock_response
        mock_get_session.return_value = mock_session

        client = Invenio("cds-rdm-sandbox", "https://sandbox-cds-rdm.web.cern.ch/api")

        with self.assertRaises(ServiceUnavailable):
            client.search("test-query")

    @patch("oais_platform.oais.sources.invenio.Invenio.get_records_in_range")
    def test_get_records_to_harvest_single_chunk(self, mock_get_records_in_range):
        mock_get_records_in_range.return_value = {
            "total_num_hits": 2,
            "results": [
                {"recid": "1", "title": "Record One", "updated": "2026-06-01T10:00:00"},
                {"recid": "2", "title": "Record Two", "updated": "2026-06-01T11:00:00"},
            ],
        }

        client = Invenio("cds-rdm-sandbox", "https://sandbox-cds-rdm.web.cern.ch/api")
        start_time = datetime.datetime(
            2026, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        end_time = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        generator = client.get_records_to_harvest(
            start=start_time, end=end_time, size=10
        )
        chunks = list(generator)

        self.assertEqual(len(chunks), 1)
        records_batch, batch_end_time = chunks[0]
        self.assertEqual(len(records_batch), 2)
        self.assertEqual(records_batch[0]["recid"], "1")
        self.assertEqual(records_batch[1]["recid"], "2")
        self.assertEqual(batch_end_time, end_time)

    @patch("oais_platform.oais.sources.invenio.Invenio.get_records_in_range")
    def test_get_records_to_harvest_empty(self, mock_get_records_in_range):
        mock_get_records_in_range.return_value = {
            "total_num_hits": 0,
            "results": [],
        }

        client = Invenio("cds-rdm-sandbox", "https://sandbox-cds-rdm.web.cern.ch/api")
        start_time = datetime.datetime(
            2026, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        end_time = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        generator = client.get_records_to_harvest(start=start_time, end=end_time)
        chunks = list(generator)

        self.assertEqual(len(chunks), 1)
        records_batch, batch_end_time = chunks[0]
        self.assertEqual(records_batch, [])
        self.assertEqual(batch_end_time, end_time)

    @patch("oais_platform.oais.sources.invenio.Invenio._get_session")
    def test_search_with_extra_query_parameters(self, mock_get_session):
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.text = '{"hits": {"total": 1, "hits": [{"id": "789", "metadata": {"title": "Param Query Title"}}]}}'
        mock_session.get.return_value = mock_response
        mock_get_session.return_value = mock_session

        client = Invenio("cds-rdm-sandbox", "https://sandbox-cds-rdm.web.cern.ch/api")

        result = client.search(
            "base_query", extra_query="type=publication&subtype=article"
        )

        self.assertEqual(result["total_num_hits"], 1)

        called_url = mock_session.get.call_args[0][0]
        self.assertIn("&type=publication&subtype=article", called_url)

    @patch("oais_platform.oais.sources.invenio.Invenio.get_records_in_range")
    def test_get_records_to_harvest_with_extra_query(self, mock_get_records_in_range):
        mock_get_records_in_range.return_value = {
            "total_num_hits": 1,
            "results": [
                {
                    "recid": "999",
                    "title": "Filtered Record",
                    "updated": "2026-06-01T10:00:00",
                },
            ],
        }

        client = Invenio("cds-rdm-sandbox", "https://sandbox-cds-rdm.web.cern.ch/api")
        start_time = datetime.datetime(
            2026, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        end_time = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        extra_query = "q=restricted:false"

        generator = client.get_records_to_harvest(
            start=start_time, end=end_time, extra_query=extra_query
        )
        chunks = list(generator)

        self.assertEqual(len(chunks), 1)
        records_batch, _ = chunks[0]
        self.assertEqual(len(records_batch), 1)
        self.assertEqual(records_batch[0]["recid"], "999")

        mock_get_records_in_range.assert_called_with(
            start_time, end_time, 1, 200, FilterType.UPDATED, extra_query
        )
