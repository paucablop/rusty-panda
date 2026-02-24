use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

// ---------------------------------------------------------------------------
// MetadataValue – a single cell in a metadata column
// ---------------------------------------------------------------------------

/// A dynamically-typed metadata value mirroring common Pandas dtypes.
/// Using `BTreeMap` / `BTreeSet` downstream so `MetadataValue` must be `Ord`.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    /// ISO-8601 date string kept as text for simplicity.
    Date(String),
    Null,
}

// -- Manual Eq/Ord so we can put MetadataValue in BTreeSet --

impl Eq for MetadataValue {}

impl PartialOrd for MetadataValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MetadataValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use MetadataValue::*;
        fn discriminant(v: &MetadataValue) -> u8 {
            match v {
                Null => 0,
                Bool(_) => 1,
                Integer(_) => 2,
                Float(_) => 3,
                String(_) => 4,
                Date(_) => 5,
            }
        }
        let da = discriminant(self);
        let db = discriminant(other);
        if da != db {
            return da.cmp(&db);
        }
        match (self, other) {
            (Null, Null) => std::cmp::Ordering::Equal,
            (Bool(a), Bool(b)) => a.cmp(b),
            (Integer(a), Integer(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.total_cmp(b),
            (String(a), String(b)) | (Date(a), Date(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl std::hash::Hash for MetadataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            MetadataValue::String(s) | MetadataValue::Date(s) => s.hash(state),
            MetadataValue::Integer(i) => i.hash(state),
            MetadataValue::Float(f) => f.to_bits().hash(state),
            MetadataValue::Bool(b) => b.hash(state),
            MetadataValue::Null => {}
        }
    }
}

impl fmt::Display for MetadataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataValue::String(s) => write!(f, "{s}"),
            MetadataValue::Integer(i) => write!(f, "{i}"),
            MetadataValue::Float(v) => write!(f, "{v:.4}"),
            MetadataValue::Bool(b) => write!(f, "{b}"),
            MetadataValue::Date(d) => write!(f, "{d}"),
            MetadataValue::Null => write!(f, "<null>"),
        }
    }
}

impl MetadataValue {
    /// Try to interpret the value as an `f64` for numeric colour mapping.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            MetadataValue::Float(v) => Some(*v),
            MetadataValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Spectrum – one row of the DataFrame
// ---------------------------------------------------------------------------

/// A single spectrum (one row of the source DataFrame).
#[derive(Debug, Clone)]
pub struct Spectrum {
    /// Wavenumber axis (x).
    pub x: Vec<f64>,
    /// Intensity axis (y) – same length as `x`.
    pub y: Vec<f64>,
    /// Dynamic metadata columns: column_name → value.
    pub metadata: BTreeMap<String, MetadataValue>,
}

// ---------------------------------------------------------------------------
// SpectralDataset – the complete loaded dataset
// ---------------------------------------------------------------------------

/// The full parsed dataset with pre-computed column indices.
#[derive(Debug, Clone)]
pub struct SpectralDataset {
    /// All spectra (rows).
    pub spectra: Vec<Spectrum>,
    /// Ordered list of metadata column names (excludes x, y).
    pub column_names: Vec<String>,
    /// For each metadata column the sorted set of unique values.
    pub unique_values: BTreeMap<String, BTreeSet<MetadataValue>>,
}

impl SpectralDataset {
    /// Build column indices from the loaded spectra.
    pub fn from_spectra(spectra: Vec<Spectrum>) -> Self {
        let mut column_names_set: BTreeSet<String> = BTreeSet::new();
        let mut unique_values: BTreeMap<String, BTreeSet<MetadataValue>> = BTreeMap::new();

        for sp in &spectra {
            for (col, val) in &sp.metadata {
                column_names_set.insert(col.clone());
                unique_values
                    .entry(col.clone())
                    .or_default()
                    .insert(val.clone());
            }
        }
        let column_names: Vec<String> = column_names_set.into_iter().collect();
        SpectralDataset {
            spectra,
            column_names,
            unique_values,
        }
    }

    /// Number of spectra.
    pub fn len(&self) -> usize {
        self.spectra.len()
    }

    /// Whether the dataset is empty.
    pub fn is_empty(&self) -> bool {
        self.spectra.is_empty()
    }
}
