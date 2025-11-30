"""Tests for the async I/O module."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from databento_mcp.async_io import read_dbn_file_async, write_parquet_async, read_parquet_async


class TestReadDbnFileAsync:
    """Tests for read_dbn_file_async function."""

    @pytest.mark.asyncio
    @patch("databento_mcp.async_io.db.DBNStore")
    async def test_reads_dbn_file(self, mock_dbn_store):
        """Test reading a DBN file asynchronously."""
        # Setup mock
        mock_store = MagicMock()
        mock_metadata = MagicMock()
        mock_df = pd.DataFrame({"col1": [1, 2, 3, 4, 5]})
        mock_store.to_df.return_value = mock_df
        mock_store.metadata = mock_metadata
        mock_dbn_store.from_file.return_value = mock_store

        # Call function
        metadata, df = await read_dbn_file_async(Path("/test/file.dbn"))

        assert metadata is mock_metadata
        assert len(df) == 5
        mock_dbn_store.from_file.assert_called_once_with("/test/file.dbn")

    @pytest.mark.asyncio
    @patch("databento_mcp.async_io.db.DBNStore")
    async def test_applies_limit(self, mock_dbn_store):
        """Test that limit is applied to results."""
        mock_store = MagicMock()
        mock_df = pd.DataFrame({"col1": list(range(100))})
        mock_store.to_df.return_value = mock_df
        mock_store.metadata = MagicMock()
        mock_dbn_store.from_file.return_value = mock_store

        _, df = await read_dbn_file_async(Path("/test/file.dbn"), limit=10)

        assert len(df) == 10

    @pytest.mark.asyncio
    @patch("databento_mcp.async_io.db.DBNStore")
    async def test_applies_offset(self, mock_dbn_store):
        """Test that offset is applied to results."""
        mock_store = MagicMock()
        mock_df = pd.DataFrame({"col1": list(range(100))})
        mock_store.to_df.return_value = mock_df
        mock_store.metadata = MagicMock()
        mock_dbn_store.from_file.return_value = mock_store

        _, df = await read_dbn_file_async(Path("/test/file.dbn"), offset=50, limit=10)

        assert len(df) == 10
        assert df.iloc[0]["col1"] == 50


class TestWriteParquetAsync:
    """Tests for write_parquet_async function."""

    @pytest.mark.asyncio
    async def test_writes_parquet_file(self, tmp_path):
        """Test writing a Parquet file asynchronously."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        output_path = tmp_path / "test.parquet"

        size = await write_parquet_async(df, output_path)

        assert output_path.exists()
        assert size > 0

    @pytest.mark.asyncio
    async def test_writes_with_compression(self, tmp_path):
        """Test writing with different compression options."""
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Test snappy compression
        snappy_path = tmp_path / "snappy.parquet"
        await write_parquet_async(df, snappy_path, compression="snappy")
        assert snappy_path.exists()

        # Test none compression
        none_path = tmp_path / "none.parquet"
        await write_parquet_async(df, none_path, compression="none")
        assert none_path.exists()


class TestReadParquetAsync:
    """Tests for read_parquet_async function."""

    @pytest.mark.asyncio
    async def test_reads_parquet_file(self, tmp_path):
        """Test reading a Parquet file asynchronously."""
        # Create a test file
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(str(parquet_path))

        # Read it back
        result_df, schema, metadata = await read_parquet_async(parquet_path)

        assert len(result_df) == 3
        assert list(result_df.columns) == ["col1", "col2"]

    @pytest.mark.asyncio
    async def test_applies_limit(self, tmp_path):
        """Test that limit is applied to results."""
        df = pd.DataFrame({"col1": list(range(100))})
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(str(parquet_path))

        result_df, _, _ = await read_parquet_async(parquet_path, limit=10)

        assert len(result_df) == 10

    @pytest.mark.asyncio
    async def test_reads_specific_columns(self, tmp_path):
        """Test reading specific columns."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [4, 5, 6]})
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(str(parquet_path))

        result_df, _, _ = await read_parquet_async(parquet_path, columns=["col1", "col3"])

        assert list(result_df.columns) == ["col1", "col3"]
