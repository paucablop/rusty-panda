#!/usr/bin/env python3
"""
generate_sample_data.py – Create a sample spectral dataset for testing.

Usage:
    python generate_sample_data.py

Requires:
    pip install pyarrow

Produces:
    sample_data.parquet  (Parquet file with List<Float64> x/y columns)
    sample_data.json     (JSON records format, ready for Rusty Panda)
    sample_data.csv      (CSV with semicolon-separated x/y values)
"""

import json
import csv
import math
import random

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


def gaussian(x, mu, sigma, amplitude):
    return amplitude * math.exp(-((x - mu) ** 2) / (2 * sigma ** 2))


def generate_spectrum(wavenumbers, peaks, noise_level=0.01):
    """Generate a spectrum as a sum of Gaussians + noise."""
    y = []
    for wn in wavenumbers:
        intensity = sum(gaussian(wn, mu, sigma, amp) for mu, sigma, amp in peaks)
        intensity += random.gauss(0, noise_level)
        y.append(round(intensity, 6))
    return y


def main():
    random.seed(42)

    wavenumbers = [round(4000 - i * 2, 1) for i in range(1000)]  # 4000 → 2002

    samples = ["Sample_A", "Sample_B", "Sample_C"]
    concentrations = [0.1, 0.5, 1.0, 2.0, 5.0]
    operators = ["Alice", "Bob"]

    # Define characteristic peaks for each sample type
    sample_peaks = {
        "Sample_A": [(3400, 80, 0.8), (2900, 40, 0.5), (2350, 30, 0.3)],
        "Sample_B": [(3200, 60, 0.6), (2800, 50, 0.7), (2500, 35, 0.4)],
        "Sample_C": [(3600, 70, 0.9), (3000, 45, 0.4), (2200, 25, 0.5)],
    }

    records = []
    row_id = 0
    for sample in samples:
        for conc in concentrations:
            for operator in operators:
                peaks = [
                    (mu, sigma, amp * conc)
                    for mu, sigma, amp in sample_peaks[sample]
                ]
                y = generate_spectrum(wavenumbers, peaks, noise_level=0.005 * conc)
                records.append({
                    "x": wavenumbers,
                    "y": y,
                    "sample": sample,
                    "concentration": conc,
                    "operator": operator,
                    "measurement_id": row_id,
                })
                row_id += 1

    # Write Parquet (preferred format)
    if HAS_PYARROW:
        table = pa.table({
            "x": pa.array([r["x"] for r in records], type=pa.list_(pa.float64())),
            "y": pa.array([r["y"] for r in records], type=pa.list_(pa.float64())),
            "sample": pa.array([r["sample"] for r in records], type=pa.utf8()),
            "concentration": pa.array([r["concentration"] for r in records], type=pa.float64()),
            "operator": pa.array([r["operator"] for r in records], type=pa.utf8()),
            "measurement_id": pa.array([r["measurement_id"] for r in records], type=pa.int64()),
        })
        pq.write_table(table, "sample_data.parquet")
        print(f"Wrote {len(records)} spectra to sample_data.parquet")
    else:
        print("pyarrow not installed – skipping Parquet output (pip install pyarrow)")

    # Write JSON
    with open("sample_data.json", "w") as f:
        json.dump(records, f)
    print(f"Wrote {len(records)} spectra to sample_data.json")

    # Write CSV
    with open("sample_data.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["x", "y", "sample", "concentration", "operator", "measurement_id"])
        for rec in records:
            x_str = ";".join(str(v) for v in rec["x"])
            y_str = ";".join(str(v) for v in rec["y"])
            writer.writerow([
                x_str, y_str,
                rec["sample"], rec["concentration"],
                rec["operator"], rec["measurement_id"]
            ])
    print(f"Wrote {len(records)} spectra to sample_data.csv")


if __name__ == "__main__":
    main()
