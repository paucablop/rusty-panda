use std::collections::BTreeMap;

use eframe::egui::Color32;
use palette::{Hsl, IntoColor, Srgb};

use crate::data::model::MetadataValue;

// ---------------------------------------------------------------------------
// Color palette generator
// ---------------------------------------------------------------------------

/// Generates `n` visually distinct colours using evenly spaced hues.
pub fn generate_palette(n: usize) -> Vec<Color32> {
    if n == 0 {
        return Vec::new();
    }
    (0..n)
        .map(|i| {
            let hue = (i as f32 / n as f32) * 360.0;
            let hsl = Hsl::new(hue, 0.75, 0.55);
            let rgb: Srgb = hsl.into_color();
            Color32::from_rgb(
                (rgb.red * 255.0) as u8,
                (rgb.green * 255.0) as u8,
                (rgb.blue * 255.0) as u8,
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Color mapping: metadata value → Color32
// ---------------------------------------------------------------------------

/// Maps unique metadata values of a chosen column to distinct colours.
#[derive(Debug, Clone)]
pub struct ColorMap {
    pub column: String,
    mapping: BTreeMap<MetadataValue, Color32>,
    default_color: Color32,
}

impl ColorMap {
    /// Build a colour map for the given column from its unique values.
    pub fn new(column: &str, unique_values: &std::collections::BTreeSet<MetadataValue>) -> Self {
        let palette = generate_palette(unique_values.len());
        let mapping: BTreeMap<MetadataValue, Color32> = unique_values
            .iter()
            .zip(palette.into_iter())
            .map(|(v, c): (&MetadataValue, Color32)| (v.clone(), c))
            .collect();

        ColorMap {
            column: column.to_string(),
            mapping,
            default_color: Color32::GRAY,
        }
    }

    /// Look up the colour for a given metadata value.
    pub fn color_for(&self, value: &MetadataValue) -> Color32 {
        self.mapping
            .get(value)
            .copied()
            .unwrap_or(self.default_color)
    }

    /// Return the legend entries (value label → colour) for the UI.
    pub fn legend_entries(&self) -> Vec<(String, Color32)> {
        self.mapping
            .iter()
            .map(|(v, c): (&MetadataValue, &Color32)| (v.to_string(), *c))
            .collect()
    }
}
