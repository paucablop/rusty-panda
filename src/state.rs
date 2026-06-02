use std::collections::{BTreeMap, BTreeSet};

use crate::color::ColorMap;
use crate::data::filter::{FilterState, filtered_indices, init_filter_state};
use crate::data::model::{MetadataValue, SpectralDataset};

/// Which numerical derivative is applied to all visible spectra.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DerivativeOrder {
    #[default]
    None,
    First,
    Second,
}

/// Active drag state for a selected spectrum.
#[derive(Debug, Clone)]
pub struct SpectrumDragState {
    pub spectrum_index: usize,
    pub start_offset: f64,
    pub start_pointer_y: f64,
}

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

    /// Whether min-max scaling is applied to the spectra.
    pub minmax_scaling: bool,

    /// Whether auto-scaling (auto-fit bounds) is active on the plot.
    pub auto_scale: bool,

    /// Request a one-shot plot reset on the next frame.
    pub plot_needs_reset: bool,

    /// The currently selected spectrum, if any.
    pub selected_spectrum: Option<usize>,

    /// Per-spectrum vertical offsets in plot coordinates.
    pub spectrum_offsets: BTreeMap<usize, f64>,

    /// Temporary drag state while a spectrum is being moved.
    pub spectrum_drag: Option<SpectrumDragState>,

    /// Derivative order applied to all visible spectra (and subtraction result).
    pub derivative_order: DerivativeOrder,

    /// Moving-average window applied before differentiation (1 = no smoothing).
    pub derivative_window: usize,

    /// Whether spectrum subtraction mode is active.
    pub subtraction_mode: bool,

    /// First selected spectrum for subtraction.
    pub subtraction_first: Option<usize>,

    /// Second selected spectrum for subtraction.
    pub subtraction_second: Option<usize>,

    /// Coefficient in `Spectrum1 - a * Spectrum2`.
    pub subtraction_a: f64,

    /// Text input mirror of `subtraction_a`.
    pub subtraction_a_input: String,
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
            minmax_scaling: false,
            auto_scale: true,
            plot_needs_reset: true,
            selected_spectrum: None,
            spectrum_offsets: BTreeMap::new(),
            spectrum_drag: None,
            derivative_order: DerivativeOrder::None,
            derivative_window: 1,
            subtraction_mode: false,
            subtraction_first: None,
            subtraction_second: None,
            subtraction_a: 1.0,
            subtraction_a_input: "1.0".to_string(),
        }
    }
}

impl AppState {
    /// Ingest a newly loaded dataset, initialise filters and colour.
    pub fn set_dataset(&mut self, dataset: SpectralDataset) {
        self.filters = init_filter_state(&dataset);
        self.visible_indices = (0..dataset.len()).collect();
        self.selected_spectrum = None;
        self.spectrum_offsets.clear();
        self.spectrum_drag = None;
        self.derivative_order = DerivativeOrder::None;
        self.derivative_window = 1;
        self.subtraction_first = None;
        self.subtraction_second = None;

        // Default colour column: first metadata column (if any).
        self.color_column = dataset.column_names.first().cloned();
        self.rebuild_color_map(&dataset);

        self.dataset = Some(dataset);
        self.status_message = None;
        self.loading = false;
        self.request_plot_reset();
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
            if self.auto_scale {
                self.request_plot_reset();
            }
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

    pub fn request_plot_reset(&mut self) {
        self.plot_needs_reset = true;
    }

    /// Enable or disable subtraction mode.
    pub fn set_subtraction_mode(&mut self, enabled: bool) {
        self.subtraction_mode = enabled;
        if !enabled {
            self.end_spectrum_drag();
        }
    }

    /// Clear both subtraction selections.
    pub fn clear_subtraction_selection(&mut self) {
        self.subtraction_first = None;
        self.subtraction_second = None;
    }

    /// Toggle a spectrum in the subtraction pair, keeping at most two entries.
    pub fn toggle_subtraction_spectrum(&mut self, index: usize) {
        if self.subtraction_first == Some(index) {
            self.subtraction_first = self.subtraction_second.take();
            return;
        }

        if self.subtraction_second == Some(index) {
            self.subtraction_second = None;
            return;
        }

        if self.subtraction_first.is_none() {
            self.subtraction_first = Some(index);
        } else if self.subtraction_second.is_none() {
            self.subtraction_second = Some(index);
        } else {
            self.subtraction_first = self.subtraction_second;
            self.subtraction_second = Some(index);
        }
    }

    /// Return the active subtraction pair if both spectra are selected.
    pub fn subtraction_pair(&self) -> Option<(usize, usize)> {
        Some((self.subtraction_first?, self.subtraction_second?))
    }

    /// Update the subtraction coefficient and keep the text field in sync.
    pub fn set_subtraction_a(&mut self, value: f64) {
        if value.is_finite() {
            self.subtraction_a = value;
            self.subtraction_a_input = format_value(value);
        }
    }

    /// Update the subtraction coefficient from typed text.
    pub fn set_subtraction_a_from_input(&mut self, input: &str) -> bool {
        let trimmed = input.trim();
        match trimmed.parse::<f64>() {
            Ok(value) if value.is_finite() => {
                self.subtraction_a = value;
                self.subtraction_a_input = trimmed.to_string();
                true
            }
            _ => false,
        }
    }

    /// Return the current vertical offset for a spectrum.
    pub fn spectrum_offset(&self, index: usize) -> f64 {
        self.spectrum_offsets.get(&index).copied().unwrap_or(0.0)
    }

    /// Select a spectrum without changing its position.
    pub fn select_spectrum(&mut self, index: usize) {
        self.selected_spectrum = Some(index);
    }

    /// Begin dragging a selected spectrum.
    pub fn begin_spectrum_drag(&mut self, index: usize, pointer_y: f64) {
        let start_offset = self.spectrum_offset(index);
        self.selected_spectrum = Some(index);
        self.spectrum_drag = Some(SpectrumDragState {
            spectrum_index: index,
            start_offset,
            start_pointer_y: pointer_y,
        });
    }

    /// Update the active spectrum drag.
    pub fn update_spectrum_drag(&mut self, pointer_y: f64) {
        if let Some(drag) = &self.spectrum_drag {
            let offset = drag.start_offset + (pointer_y - drag.start_pointer_y);
            if offset.abs() < f64::EPSILON {
                self.spectrum_offsets.remove(&drag.spectrum_index);
            } else {
                self.spectrum_offsets.insert(drag.spectrum_index, offset);
            }
        }
    }

    /// End any active spectrum drag.
    pub fn end_spectrum_drag(&mut self) {
        self.spectrum_drag = None;
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

fn format_value(value: f64) -> String {
    if value.fract().abs() < f64::EPSILON {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}
