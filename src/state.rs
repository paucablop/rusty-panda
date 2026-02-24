use std::collections::BTreeSet;

use crate::color::ColorMap;
use crate::data::filter::{FilterState, filtered_indices, init_filter_state};
use crate::data::model::{MetadataValue, SpectralDataset};

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

/// The full UI state, independent of rendering.
pub struct AppState {
    /// Loaded dataset (None until user loads a file).
    pub dataset: Option<SpectralDataset>,

    /// Per-column filter selections.
    pub filters: FilterState,

    /// Indices of spectra passing the current filters (cached).
    pub visible_indices: Vec<usize>,

    /// Which metadata column is used for colouring.
    pub color_column: Option<String>,

    /// Active colour map.
    pub color_map: Option<ColorMap>,

    /// Status / error message shown in the UI.
    pub status_message: Option<String>,

    /// Whether a file loading operation is in progress.
    pub loading: bool,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            dataset: None,
            filters: FilterState::default(),
            visible_indices: Vec::new(),
            color_column: None,
            color_map: None,
            status_message: None,
            loading: false,
        }
    }
}

impl AppState {
    /// Ingest a newly loaded dataset, initialise filters and colour.
    pub fn set_dataset(&mut self, dataset: SpectralDataset) {
        self.filters = init_filter_state(&dataset);
        self.visible_indices = (0..dataset.len()).collect();

        // Default colour column: first metadata column (if any).
        self.color_column = dataset.column_names.first().cloned();
        self.rebuild_color_map(&dataset);

        self.dataset = Some(dataset);
        self.status_message = None;
        self.loading = false;
    }

    /// Rebuild the colour map from the current `color_column`.
    pub fn rebuild_color_map(&mut self, dataset: &SpectralDataset) {
        self.color_map = self.color_column.as_ref().and_then(|col| {
            dataset
                .unique_values
                .get(col)
                .map(|vals| ColorMap::new(col, vals))
        });
    }

    /// Recompute `visible_indices` after filter change.
    pub fn refilter(&mut self) {
        if let Some(ds) = &self.dataset {
            self.visible_indices = filtered_indices(ds, &self.filters);
        }
    }

    /// Set colour column and rebuild the map.
    pub fn set_color_column(&mut self, col: String) {
        self.color_column = Some(col);
        if let Some(ds) = &self.dataset {
            let ds_clone = ds.clone();
            self.rebuild_color_map(&ds_clone);
        }
    }

    /// Toggle a single metadata value in a column's filter.
    pub fn toggle_filter_value(&mut self, column: &str, value: &MetadataValue) {
        let selected = self.filters.entry(column.to_string()).or_default();
        if selected.contains(value) {
            selected.remove(value);
        } else {
            selected.insert(value.clone());
        }
        self.refilter();
    }

    /// Select all values in a column.
    pub fn select_all(&mut self, column: &str) {
        if let Some(ds) = &self.dataset {
            if let Some(all_vals) = ds.unique_values.get(column) {
                self.filters.insert(column.to_string(), all_vals.clone());
                self.refilter();
            }
        }
    }

    /// Deselect all values in a column.
    pub fn select_none(&mut self, column: &str) {
        self.filters.insert(column.to_string(), BTreeSet::new());
        self.refilter();
    }
}
