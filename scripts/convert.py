#!/usr/bin/env python3
"""
convert.py – Convert a Pandas DataFrame (pickle, CSV, etc.) to Parquet for Rusty Panda.

Usage:
    python convert.py input.pkl output.parquet

The DataFrame must have columns 'x' and 'y' where each cell contains
a list/array of floats. All other columns are treated as metadata.

The output is a Parquet file with:
  - x: list<float64>
  - y: list<float64>
  - <metadata columns>: their original types
"""

import sys
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def convert(input_path: str, output_path: str) -> None:
    # Determine input format from extension
    if input_path.endswith(".csv"):
        df = pd.read_csv(input_path)
    elif input_path.endswith((".pkl", ".pickle")):
        df = pd.read_pickle(input_path)
    elif input_path.endswith((".parquet", ".pq")):
        df = pd.read_parquet(input_path)
    else:
        # Try pickle as default
        df = pd.read_pickle(input_path)

    assert "x" in df.columns, "DataFrame must have an 'x' column"
    assert "y" in df.columns, "DataFrame must have a 'y' column"

    # Ensure x and y are Python lists of floats (not numpy arrays)
    df["x"] = df["x"].apply(lambda v: v.tolist() if isinstance(v, np.ndarray) else [float(x) for x in v])
    df["y"] = df["y"].apply(lambda v: v.tolist() if isinstance(v, np.ndarray) else [float(x) for x in v])

    # Convert timestamps to ISO strings for portability
    for col in df.columns:
        if col in ("x", "y"):
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Build Arrow table explicitly to ensure list types
    arrays = {}
    for col in df.columns:
        if col in ("x", "y"):
            arrays[col] = pa.array(df[col].tolist(), type=pa.list_(pa.float64()))
        else:
            arrays[col] = pa.array(df[col].tolist())

    table = pa.table(arrays)
    pq.write_table(table, output_path)
    print(f"Converted {len(df)} rows → {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
