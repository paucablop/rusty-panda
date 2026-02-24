use eframe::egui::{Color32, Ui};
use egui_plot::{Line, Plot, PlotPoints};

use crate::state::AppState;

// ---------------------------------------------------------------------------
// Spectral plot (central panel)
// ---------------------------------------------------------------------------

/// Render the spectral plot in the central panel.
pub fn spectral_plot(ui: &mut Ui, state: &AppState) {
    let dataset = match &state.dataset {
        Some(ds) => ds,
        None => {
            ui.centered_and_justified(|ui: &mut Ui| {
                ui.heading("Open a file to view spectra  (File → Open…)");
            });
            return;
        }
    };

    let color_map = &state.color_map;
    let color_col = state.color_column.as_deref();

    Plot::new("spectral_plot")
        .legend(egui_plot::Legend::default())
        .x_axis_label("Wavenumber")
        .y_axis_label("Intensity")
        .allow_boxed_zoom(true)
        .allow_drag(true)
        .allow_scroll(true)
        .allow_zoom(true)
        .show(ui, |plot_ui| {
            for &idx in &state.visible_indices {
                let sp = &dataset.spectra[idx];

                // Determine colour from the colour-by column.
                let color = color_col
                    .and_then(|col| {
                        let val = sp.metadata.get(col)?;
                        let cm = color_map.as_ref()?;
                        Some(cm.color_for(val))
                    })
                    .unwrap_or(Color32::LIGHT_BLUE);

                // Build the legend name from the colour column value.
                let name = color_col
                    .and_then(|col| sp.metadata.get(col))
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| format!("spectrum {idx}"));

                let y_values: Vec<f64> = if state.minmax_scaling {
                    let min = sp.y.iter().cloned().fold(f64::INFINITY, f64::min);
                    let max = sp.y.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                    let range = max - min;
                    if range.abs() < f64::EPSILON {
                        vec![0.0; sp.y.len()]
                    } else {
                        sp.y.iter().map(|&yi| (yi - min) / range).collect()
                    }
                } else {
                    sp.y.clone()
                };

                let points: PlotPoints = sp
                    .x
                    .iter()
                    .zip(y_values.iter())
                    .map(|(&xi, &yi)| [xi, yi])
                    .collect();

                let line = Line::new(points)
                    .name(&name)
                    .color(color)
                    .width(1.5);

                plot_ui.line(line);
            }
        });
}
