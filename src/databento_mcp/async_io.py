"""Async file I/O operations for better concurrency.

Provides async wrappers for CPU-bound file operations using a thread pool executor.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Tuple

import databento as db
import pandas as pd
import pyarrow.parquet as pq

# Thread pool for CPU-bound operations
_executor = ThreadPoolExecutor(max_workers=4)


async def read_dbn_file_async(
    file_path: Path, limit: int = 1000, offset: int = 0
) -> Tuple[db.Metadata, pd.DataFrame]:
    """
    Read DBN file asynchronously.

    Args:
        file_path: Path to the DBN file
        limit: Maximum number of records to return
        offset: Number of records to skip

    Returns:
        Tuple of (metadata, DataFrame)
    """
    loop = asyncio.get_event_loop()

    def _read():
        store = db.DBNStore.from_file(str(file_path))
        df = store.to_df()
        metadata = store.metadata
        return metadata, df

    metadata, df = await loop.run_in_executor(_executor, _read)

    # Apply offset and limit
    if offset > 0:
        df = df.iloc[offset:]
    if limit > 0:
        df = df.head(limit)

    return metadata, df


async def write_parquet_async(
    df: pd.DataFrame, file_path: Path, compression: str = "snappy"
) -> int:
    """
    Write Parquet file asynchronously.

    Args:
        df: DataFrame to write
        file_path: Path for output file
        compression: Compression type ("snappy", "gzip", "zstd", or "none")

    Returns:
        File size in bytes
    """
    loop = asyncio.get_event_loop()

    def _write():
        parquet_compression = compression if compression != "none" else None
        df.to_parquet(str(file_path), compression=parquet_compression)
        return file_path.stat().st_size

    return await loop.run_in_executor(_executor, _write)


async def read_parquet_async(
    file_path: Path, limit: int = 1000, columns: Optional[list] = None
) -> Tuple[pd.DataFrame, pq.ParquetSchema, pq.FileMetaData]:
    """
    Read Parquet file asynchronously.

    Args:
        file_path: Path to the Parquet file
        limit: Maximum number of records to return
        columns: Optional list of columns to read

    Returns:
        Tuple of (DataFrame, schema, metadata)
    """
    loop = asyncio.get_event_loop()

    def _read():
        parquet_file = pq.read_table(str(file_path), columns=columns)
        df = parquet_file.to_pandas()
        metadata = pq.read_metadata(str(file_path))
        return df, parquet_file.schema, metadata

    df, schema, metadata = await loop.run_in_executor(_executor, _read)

    # Apply limit
    if limit > 0:
        df = df.head(limit)

    return df, schema, metadata
