use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, AsArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeListArray, ListArray, StringArray, BooleanArray,
};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value as JsonValue;

use super::model::{MetadataValue, Spectrum, SpectralDataset};

// ---------------------------------------------------------------------------
// Public entry-point
// ---------------------------------------------------------------------------

/// Load a spectral dataset from a file.  Dispatch by extension.
///
/// Supported formats:
/// * `.parquet` – Parquet file with `x` and `y` list columns (recommended)
/// * `.json`    – `[{ "x": [...], "y": [...], ...meta }, ...]`
/// * `.csv`     – columns `x` and `y` containing semicolon-separated floats
pub fn load_file(path: &Path) -> Result<SpectralDataset> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    match ext.as_str() {
        "parquet" | "pq" => load_parquet(path),
        "json" => load_json(path),
        "csv" => load_csv(path),
        other => bail!("Unsupported file extension: .{other}"),
    }
}

// ---------------------------------------------------------------------------
// JSON loader
// ---------------------------------------------------------------------------

/// Expected JSON schema (records-oriented, the default `df.to_json(orient='records')`):
///
/// ```json
/// [
///   {
///     "x": [4000.0, 3999.0, ...],
///     "y": [0.12,   0.14,  ...],
///     "sample": "A",
///     "concentration": 1.5
///   },
///   ...
/// ]
/// ```
fn load_json(path: &Path) -> Result<SpectralDataset> {
    let text = std::fs::read_to_string(path).context("reading JSON file")?;
    let root: JsonValue = serde_json::from_str(&text).context("parsing JSON")?;

    let records = root
        .as_array()
        .context("Expected top-level JSON array")?;

    let mut spectra = Vec::with_capacity(records.len());

    for (i, rec) in records.iter().enumerate() {
        let obj = rec
            .as_object()
            .with_context(|| format!("Row {i} is not a JSON object"))?;

        let x = json_array_to_f64(obj.get("x"), i, "x")?;
        let y = json_array_to_f64(obj.get("y"), i, "y")?;

        if x.len() != y.len() {
            bail!("Row {i}: x has {} values but y has {}", x.len(), y.len());
        }

        let mut metadata = BTreeMap::new();
        for (key, val) in obj {
            if key == "x" || key == "y" {
                continue;
            }
            metadata.insert(key.clone(), json_to_metadata(val));
        }

        spectra.push(Spectrum { x, y, metadata });
    }

    Ok(SpectralDataset::from_spectra(spectra))
}

fn json_array_to_f64(val: Option<&JsonValue>, row: usize, col: &str) -> Result<Vec<f64>> {
    let arr = val
        .and_then(|v| v.as_array())
        .with_context(|| format!("Row {row}: missing or invalid '{col}' array"))?;

    arr.iter()
        .enumerate()
        .map(|(j, v)| {
            v.as_f64()
                .with_context(|| format!("Row {row}, {col}[{j}]: not a number"))
        })
        .collect()
}

fn json_to_metadata(val: &JsonValue) -> MetadataValue {
    match val {
        JsonValue::String(s) => MetadataValue::String(s.clone()),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                MetadataValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                MetadataValue::Float(f)
            } else {
                MetadataValue::String(n.to_string())
            }
        }
        JsonValue::Bool(b) => MetadataValue::Bool(*b),
        JsonValue::Null => MetadataValue::Null,
        other => MetadataValue::String(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// CSV loader
// ---------------------------------------------------------------------------

/// CSV layout:  header row with column names.
/// `x` and `y` columns contain semicolon-separated floats:
///   `"4000.0;3999.0;3998.0"`, `"0.12;0.14;0.11"`
/// All other columns are treated as metadata.
fn load_csv(path: &Path) -> Result<SpectralDataset> {
    let mut reader = csv::Reader::from_path(path).context("opening CSV")?;
    let headers: Vec<String> = reader
        .headers()
        .context("reading CSV headers")?
        .iter()
        .map(|h| h.to_string())
        .collect();

    let x_idx = headers
        .iter()
        .position(|h| h == "x")
        .context("CSV missing 'x' column")?;
    let y_idx = headers
        .iter()
        .position(|h| h == "y")
        .context("CSV missing 'y' column")?;

    let mut spectra = Vec::new();

    for (row_no, result) in reader.records().enumerate() {
        let record = result.with_context(|| format!("CSV row {row_no}"))?;

        let x = parse_semicolon_floats(record.get(x_idx).unwrap_or(""), row_no, "x")?;
        let y = parse_semicolon_floats(record.get(y_idx).unwrap_or(""), row_no, "y")?;

        if x.len() != y.len() {
            bail!(
                "CSV row {row_no}: x has {} values but y has {}",
                x.len(),
                y.len()
            );
        }

        let mut metadata = BTreeMap::new();
        for (col_idx, value) in record.iter().enumerate() {
            if col_idx == x_idx || col_idx == y_idx {
                continue;
            }
            let col_name = &headers[col_idx];
            metadata.insert(col_name.clone(), guess_metadata_type(value));
        }

        spectra.push(Spectrum { x, y, metadata });
    }

    Ok(SpectralDataset::from_spectra(spectra))
}

fn parse_semicolon_floats(s: &str, row: usize, col: &str) -> Result<Vec<f64>> {
    s.split(';')
        .enumerate()
        .map(|(j, tok)| {
            tok.trim()
                .parse::<f64>()
                .with_context(|| format!("Row {row}, {col}[{j}]: '{tok}' is not a number"))
        })
        .collect()
}

fn guess_metadata_type(s: &str) -> MetadataValue {
    if s.is_empty() {
        return MetadataValue::Null;
    }
    if let Ok(i) = s.parse::<i64>() {
        return MetadataValue::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return MetadataValue::Float(f);
    }
    if s == "true" || s == "false" {
        return MetadataValue::Bool(s == "true");
    }
    MetadataValue::String(s.to_string())
}

// ---------------------------------------------------------------------------
// Parquet loader
// ---------------------------------------------------------------------------

/// Load a Parquet file containing spectral data.
///
/// Expected schema:
/// - `x`: List<Float64> or LargeList<Float64> – wavenumber arrays
/// - `y`: List<Float64> or LargeList<Float64> – intensity arrays
/// - Any other columns are treated as metadata (strings, ints, floats, bools)
///
/// Works with files written by both **Pandas** (`df.to_parquet()`) and
/// **Polars** (`df.write_parquet()`).
fn load_parquet(path: &Path) -> Result<SpectralDataset> {
    let file = std::fs::File::open(path).context("opening parquet file")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading parquet metadata")?;
    let reader = builder.build().context("building parquet reader")?;

    let mut spectra = Vec::new();

    for batch_result in reader {
        let batch = batch_result.context("reading parquet record batch")?;
        let schema = batch.schema();
        let n_rows = batch.num_rows();

        // Locate x and y columns
        let x_idx = schema
            .index_of("x")
            .map_err(|_| anyhow::anyhow!("Parquet file missing 'x' column"))?;
        let y_idx = schema
            .index_of("y")
            .map_err(|_| anyhow::anyhow!("Parquet file missing 'y' column"))?;

        let x_col = batch.column(x_idx);
        let y_col = batch.column(y_idx);

        // Collect metadata column indices (everything except x, y)
        let meta_cols: Vec<(usize, String)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != x_idx && *i != y_idx)
            .map(|(i, f)| (i, f.name().clone()))
            .collect();

        for row in 0..n_rows {
            let x = extract_f64_list(x_col, row)
                .with_context(|| format!("Row {row}: failed to read 'x'"))?;
            let y = extract_f64_list(y_col, row)
                .with_context(|| format!("Row {row}: failed to read 'y'"))?;

            if x.len() != y.len() {
                bail!("Row {row}: x has {} values but y has {}", x.len(), y.len());
            }

            let mut metadata = BTreeMap::new();
            for (col_idx, col_name) in &meta_cols {
                let col_array = batch.column(*col_idx);
                let value = extract_metadata_value(col_array, row);
                metadata.insert(col_name.clone(), value);
            }

            spectra.push(Spectrum { x, y, metadata });
        }
    }

    Ok(SpectralDataset::from_spectra(spectra))
}

// -- Parquet / Arrow helpers --

/// Extract a `Vec<f64>` from a List or LargeList column at the given row.
fn extract_f64_list(col: &Arc<dyn Array>, row: usize) -> Result<Vec<f64>> {
    if col.is_null(row) {
        bail!("null value in list column");
    }

    let values_array = match col.data_type() {
        DataType::List(_) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .context("expected ListArray")?;
            list_arr.value(row)
        }
        DataType::LargeList(_) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .context("expected LargeListArray")?;
            list_arr.value(row)
        }
        other => bail!("Expected List or LargeList column, got {other:?}"),
    };

    // The inner array can be Float64 or Float32
    if let Some(f64_arr) = values_array.as_any().downcast_ref::<Float64Array>() {
        Ok(f64_arr.iter().map(|v| v.unwrap_or(f64::NAN)).collect())
    } else if let Some(f32_arr) = values_array.as_any().downcast_ref::<Float32Array>() {
        Ok(f32_arr.iter().map(|v| v.unwrap_or(f32::NAN) as f64).collect())
    } else {
        bail!(
            "List inner type is {:?}, expected Float64 or Float32",
            values_array.data_type()
        )
    }
}

/// Extract a single metadata value from an Arrow column at a given row.
fn extract_metadata_value(col: &Arc<dyn Array>, row: usize) -> MetadataValue {
    if col.is_null(row) {
        return MetadataValue::Null;
    }
    match col.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
                MetadataValue::String(s.value(row).to_string())
            } else {
                // LargeStringArray
                let s = col.as_string::<i64>();
                MetadataValue::String(s.value(row).to_string())
            }
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            MetadataValue::Integer(arr.value(row) as i64)
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            MetadataValue::Integer(arr.value(row))
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            MetadataValue::Float(arr.value(row) as f64)
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            MetadataValue::Float(arr.value(row))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            MetadataValue::Bool(arr.value(row))
        }
        _ => MetadataValue::String(format!("{:?}", col.data_type())),
    }
}
