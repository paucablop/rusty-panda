use std::collections::{BTreeMap, BTreeSet};

use super::model::{MetadataValue, SpectralDataset};

// ---------------------------------------------------------------------------
// Filter predicate: which unique values are selected per column
// ---------------------------------------------------------------------------

/// Per-column selection state: maps column_name → set of selected values.
/// If a column is absent or its set is empty, it means "no filter" (show all).
pub type FilterState = BTreeMap<String, BTreeSet<MetadataValue>>;

/// Initialise a [`FilterState`] with all values selected (i.e., show everything).
pub fn init_filter_state(dataset: &SpectralDataset) -> FilterState {
    dataset
        .unique_values
        .iter()
        .map(|(col, vals)| (col.clone(), vals.clone()))
        .collect()
}

/// Return indices of spectra that pass all active filters.
///
/// A spectrum passes a column filter when:
/// * The column is not present in `filters` → passes (no constraint)
/// * The filter set for that column is empty → nothing selected → fails
/// * The spectrum's value for that column is in the selected set → passes
pub fn filtered_indices(dataset: &SpectralDataset, filters: &FilterState) -> Vec<usize> {
    dataset
        .spectra
        .iter()
        .enumerate()
        .filter(|(_, sp)| {
            for (col, selected) in filters {
                if selected.is_empty() {
                    // Nothing selected for this column → hide everything
                    return false;
                }
                // Check all unique values are selected → no effective filter
                if let Some(all_vals) = dataset.unique_values.get(col) {
                    if selected.len() == all_vals.len() {
                        continue; // everything selected, no filtering needed
                    }
                }
                match sp.metadata.get(col) {
                    Some(val) => {
                        if !selected.contains(val) {
                            return false;
                        }
                    }
                    None => {
                        // spectrum doesn't have this column → include only if Null is selected
                        if !selected.contains(&MetadataValue::Null) {
                            return false;
                        }
                    }
                }
            }
            true
        })
        .map(|(i, _)| i)
        .collect()
}
