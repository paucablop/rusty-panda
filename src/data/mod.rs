/// Data layer: core types, loading, and filtering.
///
/// Architecture:
/// ```text
///  .pkl / .json / .csv
///        │
///        ▼
///   ┌──────────┐
///   │  loader   │  parse file → SpectralDataset
///   └──────────┘
///        │
///        ▼
///   ┌──────────────┐
///   │ SpectralDataset│  Vec<Spectrum>, column index
///   └──────────────┘
///        │
///        ▼
///   ┌──────────┐
///   │  filter   │  apply metadata predicates → filtered indices
///   └──────────┘
/// ```

pub mod loader;
pub mod model;
pub mod filter;
