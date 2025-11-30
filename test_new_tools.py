"""Tests for the new tools: list_schemas, list_unit_prices, cancel_batch_job, download_batch_files."""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import tempfile
import os
import sys


class TestListSchemas:
    """Tests for list_schemas tool."""

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_list_schemas_returns_all_schemas(self):
        """Test that list_schemas returns all expected schemas."""
        # Clear cache using public API
        from server import cache
        cache.clear()

        with patch("server.client") as mock_client:
            from server import call_tool

            # Run the tool
            result = asyncio.run(call_tool("list_schemas", {}))

            # Check result
            assert len(result) == 1
            text = result[0].text

            # Verify all schemas are present
            expected_schemas = [
                "trades", "tbbo", "mbp-1", "mbp-10", "mbo",
                "ohlcv-1s", "ohlcv-1m", "ohlcv-1h", "ohlcv-1d",
                "definition", "statistics", "status", "imbalance"
            ]
            for schema in expected_schemas:
                assert schema in text, f"Schema '{schema}' should be in output"

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_list_schemas_includes_descriptions(self):
        """Test that list_schemas includes descriptions for each schema."""
        with patch("server.client"):
            from server import call_tool

            result = asyncio.run(call_tool("list_schemas", {}))
            text = result[0].text

            # Check for section headers
            assert "Trade Data" in text
            assert "Order Book" in text
            assert "OHLCV Bars" in text
            assert "Reference Data" in text

            # Check for descriptions
            assert "Individual trades" in text
            assert "Market by price" in text


class TestListUnitPrices:
    """Tests for list_unit_prices tool."""

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_list_unit_prices_without_filter(self):
        """Test list_unit_prices without dataset filter."""
        # Clear cache
        from server import cache
        cache.clear()

        with patch("server.client") as mock_client:
            from server import call_tool

            # Mock the API response
            mock_client.metadata.list_unit_prices.return_value = [
                {"dataset": "GLBX.MDP3", "schema": "trades", "mode": "historical", "price": "0.01", "unit": "per GB"},
                {"dataset": "GLBX.MDP3", "schema": "ohlcv-1m", "mode": "historical", "price": "0.005", "unit": "per GB"},
            ]

            result = asyncio.run(call_tool("list_unit_prices", {}))

            assert len(result) == 1
            text = result[0].text
            assert "Unit Prices" in text
            assert "GLBX.MDP3" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_list_unit_prices_with_dataset_filter(self):
        """Test list_unit_prices with dataset filter."""
        from server import cache
        cache.clear()

        with patch("server.client") as mock_client:
            from server import call_tool

            mock_client.metadata.list_unit_prices.return_value = [
                {"dataset": "GLBX.MDP3", "schema": "trades", "mode": "historical", "price": "0.01", "unit": "per GB"},
            ]

            result = asyncio.run(call_tool("list_unit_prices", {"dataset": "GLBX.MDP3"}))

            assert len(result) == 1
            text = result[0].text
            assert "Filtered by dataset: GLBX.MDP3" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_list_unit_prices_invalid_dataset(self):
        """Test list_unit_prices with invalid dataset format."""
        with patch("server.client"):
            from server import call_tool

            result = asyncio.run(call_tool("list_unit_prices", {"dataset": "invalid"}))

            text = result[0].text
            assert "Validation error" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_list_unit_prices_empty_response(self):
        """Test list_unit_prices when API returns empty list."""
        from server import cache
        cache.clear()

        with patch("server.client") as mock_client:
            from server import call_tool

            mock_client.metadata.list_unit_prices.return_value = []

            result = asyncio.run(call_tool("list_unit_prices", {}))

            text = result[0].text
            assert "No pricing information available" in text


class TestCancelBatchJob:
    """Tests for cancel_batch_job tool."""

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_cancel_batch_job_success(self):
        """Test successful batch job cancellation."""
        with patch("server.client") as mock_client:
            from server import call_tool

            mock_client.batch.cancel_job.return_value = {"state": "cancelled"}

            result = asyncio.run(call_tool("cancel_batch_job", {"job_id": "JOB-12345"}))

            text = result[0].text
            assert "JOB-12345" in text
            assert "success" in text.lower() or "cancelled" in text.lower()

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_cancel_batch_job_missing_job_id(self):
        """Test cancel_batch_job with missing job_id."""
        with patch("server.client"):
            from server import call_tool

            result = asyncio.run(call_tool("cancel_batch_job", {}))

            text = result[0].text
            assert "job_id is required" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_cancel_batch_job_empty_job_id(self):
        """Test cancel_batch_job with empty job_id."""
        with patch("server.client"):
            from server import call_tool

            result = asyncio.run(call_tool("cancel_batch_job", {"job_id": ""}))

            text = result[0].text
            assert "job_id is required" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_cancel_batch_job_not_found(self):
        """Test cancel_batch_job when job is not found."""
        with patch("server.client") as mock_client:
            from server import call_tool

            mock_client.batch.cancel_job.side_effect = Exception("Job not found (404)")

            result = asyncio.run(call_tool("cancel_batch_job", {"job_id": "INVALID-JOB"}))

            text = result[0].text
            assert "not found" in text.lower()

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_cancel_batch_job_already_completed(self):
        """Test cancel_batch_job when job is already completed."""
        with patch("server.client") as mock_client:
            from server import call_tool

            mock_client.batch.cancel_job.side_effect = Exception("Job already completed")

            result = asyncio.run(call_tool("cancel_batch_job", {"job_id": "JOB-DONE"}))

            text = result[0].text
            assert "completed" in text.lower()


class TestDownloadBatchFiles:
    """Tests for download_batch_files tool."""

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_download_batch_files_missing_job_id(self):
        """Test download_batch_files with missing job_id."""
        with patch("server.client"):
            from server import call_tool

            result = asyncio.run(call_tool("download_batch_files", {"output_dir": "/tmp"}))

            text = result[0].text
            assert "job_id is required" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_download_batch_files_missing_output_dir(self):
        """Test download_batch_files with missing output_dir."""
        with patch("server.client"):
            from server import call_tool

            result = asyncio.run(call_tool("download_batch_files", {"job_id": "JOB-12345"}))

            text = result[0].text
            assert "output_dir is required" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_download_batch_files_no_files(self):
        """Test download_batch_files when job has no files."""
        with patch("server.client") as mock_client:
            from server import call_tool

            mock_client.batch.list_files.return_value = []

            with tempfile.TemporaryDirectory() as tmpdir:
                result = asyncio.run(call_tool("download_batch_files", {
                    "job_id": "JOB-EMPTY",
                    "output_dir": tmpdir
                }))

            text = result[0].text
            assert "No files available" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_download_batch_files_with_files(self):
        """Test download_batch_files with available files."""
        with patch("server.client") as mock_client:
            from server import call_tool

            mock_client.batch.list_files.return_value = [
                {
                    "filename": "data.dbn.zst",
                    "size": 1024,
                    "urls": {"https": "https://example.com/data.dbn.zst"},
                }
            ]

            with tempfile.TemporaryDirectory() as tmpdir:
                # Mock httpx
                with patch("httpx.AsyncClient") as mock_http:
                    mock_response = MagicMock()
                    mock_response.content = b"test data content"
                    mock_response.raise_for_status = MagicMock()

                    async_client = AsyncMock()
                    async_client.get.return_value = mock_response
                    async_client.__aenter__.return_value = async_client
                    async_client.__aexit__.return_value = None
                    mock_http.return_value = async_client

                    result = asyncio.run(call_tool("download_batch_files", {
                        "job_id": "JOB-WITH-FILES",
                        "output_dir": tmpdir
                    }))

            text = result[0].text
            assert "Download Results" in text

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_download_batch_files_file_exists_no_overwrite(self):
        """Test download_batch_files when file exists and overwrite is false."""
        with patch("server.client") as mock_client:
            from server import call_tool

            mock_client.batch.list_files.return_value = [
                {
                    "filename": "existing.dbn",
                    "size": 1024,
                    "urls": {"https": "https://example.com/existing.dbn"},
                }
            ]

            with tempfile.TemporaryDirectory() as tmpdir:
                # Create existing file
                existing_file = os.path.join(tmpdir, "existing.dbn")
                with open(existing_file, 'w') as f:
                    f.write("existing content")

                result = asyncio.run(call_tool("download_batch_files", {
                    "job_id": "JOB-12345",
                    "output_dir": tmpdir,
                    "overwrite": False
                }))

            text = result[0].text
            assert "already exists" in text.lower() or "overwrite" in text.lower()


class TestToolRegistration:
    """Tests to verify all new tools are registered."""

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_new_tools_registered_in_list_tools(self):
        """Test that all new tools are registered in list_tools."""
        with patch("server.client"):
            from server import list_tools

            tools = asyncio.run(list_tools())
            tool_names = [tool.name for tool in tools]

            assert "list_schemas" in tool_names
            assert "list_unit_prices" in tool_names
            assert "cancel_batch_job" in tool_names
            assert "download_batch_files" in tool_names

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_list_schemas_input_schema(self):
        """Test list_schemas has correct input schema."""
        with patch("server.client"):
            from server import list_tools

            tools = asyncio.run(list_tools())
            list_schemas_tool = next(t for t in tools if t.name == "list_schemas")

            # Should have no required parameters
            assert list_schemas_tool.inputSchema["type"] == "object"
            assert "required" not in list_schemas_tool.inputSchema or list_schemas_tool.inputSchema.get("required") == []

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_cancel_batch_job_input_schema(self):
        """Test cancel_batch_job has correct input schema."""
        with patch("server.client"):
            from server import list_tools

            tools = asyncio.run(list_tools())
            cancel_tool = next(t for t in tools if t.name == "cancel_batch_job")

            assert "job_id" in cancel_tool.inputSchema["properties"]
            assert "job_id" in cancel_tool.inputSchema["required"]

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_download_batch_files_input_schema(self):
        """Test download_batch_files has correct input schema."""
        with patch("server.client"):
            from server import list_tools

            tools = asyncio.run(list_tools())
            download_tool = next(t for t in tools if t.name == "download_batch_files")

            assert "job_id" in download_tool.inputSchema["properties"]
            assert "output_dir" in download_tool.inputSchema["properties"]
            assert "overwrite" in download_tool.inputSchema["properties"]
            assert "job_id" in download_tool.inputSchema["required"]
            assert "output_dir" in download_tool.inputSchema["required"]
