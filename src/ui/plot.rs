use eframe::egui::{self, Color32, CornerRadius, Frame, Pos2, RichText, Stroke, Ui, Vec2b};
use egui_plot::{Corner, Legend, Line, Plot, PlotPoint, PlotPoints};

use crate::data::model::Spectrum;
use crate::state::{AppState, DerivativeOrder};

// ---------------------------------------------------------------------------
// Spectral plot (central panel)
// ---------------------------------------------------------------------------

/// Render the spectral plot in the central panel.
pub fn spectral_plot(ui: &mut Ui, state: &mut AppState) {
    if state.dataset.is_none() {
        Frame::default()
            .fill(Color32::from_rgb(24, 17, 41))
            .stroke(Stroke::new(1.0, Color32::from_rgb(63, 47, 95)))
            .corner_radius(CornerRadius::same(26))
            .inner_margin(egui::Margin::symmetric(32, 32))
            .show(ui, |ui| {
                ui.centered_and_justified(|ui: &mut Ui| {
                    ui.vertical_centered(|ui| {
                        ui.label(
                            RichText::new("Bring in a dataset")
                                .size(32.0)
                                .strong()
                                .color(Color32::from_rgb(241, 247, 252)),
                        );
                        ui.add_space(8.0);
                        ui.label(
                            RichText::new(
                                "Load Parquet, JSON, or CSV data to turn this canvas into an interactive spectral view.",
                            )
                            .color(Color32::from_rgb(190, 176, 213)),
                        );
                    });
                });
            });
        return;
    }

    let color_map = state.color_map.clone();
    let color_col = state.color_column.clone();
    let visible_indices = state.visible_indices.clone();
    let minmax_scaling = state.minmax_scaling;
    let selected_spectrum = state.selected_spectrum;
    let spectrum_offsets = state.spectrum_offsets.clone();
    let dragging_state = state.spectrum_drag.clone();
    let plot_needs_reset = state.plot_needs_reset;
    let subtraction_mode = state.subtraction_mode;
    let subtraction_pair = state.subtraction_pair();
    let subtraction_a = state.subtraction_a;
    let subtraction_selected = subtraction_pair;
    let derivative_order = state.derivative_order;
    let derivative_window = state.derivative_window;
    let mut pending_drag_begin: Option<(usize, f64)> = None;
    let mut pending_drag_update: Option<f64> = None;
    let mut pending_drag_end = false;
    let mut pending_select: Option<usize> = None;
    let mut pending_subtraction_select: Option<usize> = None;

    Frame::default()
        .fill(Color32::from_rgb(18, 12, 33))
        .stroke(Stroke::new(1.0, Color32::from_rgb(63, 47, 95)))
        .corner_radius(CornerRadius::same(24))
        .inner_margin(egui::Margin::symmetric(18, 18))
        .show(ui, |ui| {
            let mut plot = Plot::new("spectral_plot")
                .legend(
                    Legend::default()
                        .position(Corner::RightTop)
                        .background_alpha(0.85)
                        .follow_insertion_order(true),
                )
                .x_axis_label("Wavenumber")
                .y_axis_label(match (minmax_scaling, derivative_order) {
                    (_, DerivativeOrder::First) => "1st Derivative",
                    (_, DerivativeOrder::Second) => "2nd Derivative",
                    (true, _) => "Normalized Intensity",
                    _ => "Intensity",
                })
                .auto_bounds(Vec2b::new(state.auto_scale, state.auto_scale))
                .allow_boxed_zoom(true)
                .allow_drag(false)
                .allow_scroll(true)
                .allow_zoom(true);

            if plot_needs_reset {
                plot = plot.reset();
            }

            plot
                .show(ui, |plot_ui| {
                    let dataset = state.dataset.as_ref().expect("dataset checked above");
                    let response = plot_ui.response();
                    let pointer_pos = response.hover_pos();

                    let mut hovered_spectrum: Option<usize> = None;
                    let mut hovered_distance = f32::INFINITY;
                    let mut preview_drag_offset: Option<(usize, f64)> = None;

                    if let (Some(pointer_pos), true) = (pointer_pos, response.hovered() || response.dragged() || response.drag_started()) {
                        for &idx in &visible_indices {
                            let sp = &dataset.spectra[idx];
                            let offset = spectrum_offsets.get(&idx).copied().unwrap_or(0.0);
                            let y_values = plotted_y_values(sp, minmax_scaling, offset);
                            let distance = distance_to_polyline(plot_ui, &sp.x, &y_values, pointer_pos);

                            if distance < hovered_distance {
                                hovered_distance = distance;
                                hovered_spectrum = Some(idx);
                            }
                        }

                        if hovered_distance > 10.0 {
                            hovered_spectrum = None;
                        }
                    }

                    if let Some(drag) = &dragging_state {
                        if let Some(pointer) = plot_ui.pointer_coordinate() {
                            let offset = drag.start_offset + (pointer.y - drag.start_pointer_y);
                            preview_drag_offset = Some((drag.spectrum_index, offset));
                        }
                    }

                    if !subtraction_mode && response.drag_started() {
                        if let (Some(idx), Some(pointer)) = (hovered_spectrum, plot_ui.pointer_coordinate()) {
                            preview_drag_offset = Some((idx, spectrum_offsets.get(&idx).copied().unwrap_or(0.0)));
                            pending_drag_begin = Some((idx, pointer.y));
                        }
                    }

                    if response.clicked() {
                        if let Some(idx) = hovered_spectrum {
                            if subtraction_mode {
                                pending_subtraction_select = Some(idx);
                            } else {
                                pending_select = Some(idx);
                            }
                        }
                    }

                    if !subtraction_mode && dragging_state.is_some() {
                        if let Some(pointer) = plot_ui.pointer_coordinate() {
                            pending_drag_update = Some(pointer.y);
                        }

                        if !plot_ui.ctx().input(|i| i.pointer.primary_down()) {
                            pending_drag_end = true;
                        }
                    }

                    for &idx in &visible_indices {
                        let sp = &dataset.spectra[idx];

                        // Determine colour from the colour-by column.
                        let color = color_col
                            .as_ref()
                            .and_then(|col| {
                                let val = sp.metadata.get(col)?;
                                let cm = color_map.as_ref()?;
                                Some(cm.color_for(val))
                            })
                            .unwrap_or(Color32::from_rgb(180, 138, 255));

                        // Build the legend name from the colour column value.
                        let name = color_col
                            .as_ref()
                            .and_then(|col| sp.metadata.get(col))
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| format!("spectrum {idx}"));

                        let mut y_values = plotted_y_values(sp, minmax_scaling, spectrum_offsets.get(&idx).copied().unwrap_or(0.0));

                        if let Some((drag_idx, offset)) = preview_drag_offset {
                            if drag_idx == idx {
                                y_values = plotted_y_values(sp, minmax_scaling, offset);
                            }
                        }

                        y_values = apply_derivative(&sp.x, y_values, derivative_order, derivative_window);

                        let points: PlotPoints = sp
                            .x
                            .iter()
                            .zip(y_values.iter())
                            .map(|(&xi, &yi)| [xi, yi])
                            .collect();

                        let is_subtraction_selected = subtraction_selected
                            .map(|(first, second)| idx == first || idx == second)
                            .unwrap_or(false);
                        let is_selected = selected_spectrum == Some(idx)
                            || dragging_state.as_ref().map(|drag| drag.spectrum_index) == Some(idx)
                            || is_subtraction_selected;
                        let alpha = if is_selected {
                            255
                        } else if visible_indices.len() > 120 {
                            110
                        } else {
                            180
                        };
                        let line = Line::new(points)
                            .name(&name)
                            .color(Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), alpha))
                            .width(if is_selected {
                                3.0
                            } else if visible_indices.len() > 80 {
                                1.1
                            } else {
                                1.8
                            });

                        plot_ui.line(line);
                    }

                    if let Some((first_index, second_index)) = subtraction_pair {
                        if let (Some(first), Some(second)) = (
                            dataset.spectra.get(first_index),
                            dataset.spectra.get(second_index),
                        ) {
                            let first_y = plotted_y_values(first, minmax_scaling, spectrum_offsets.get(&first_index).copied().unwrap_or(0.0));
                            let second_y = plotted_y_values(second, minmax_scaling, spectrum_offsets.get(&second_index).copied().unwrap_or(0.0));
                            let second_resampled = sample_series_on_x(&first.x, &second.x, &second_y);
                            let raw_subtracted: Vec<f64> = first
                                .x
                                .iter()
                                .zip(first_y.iter().zip(second_resampled.iter()))
                                .map(|(_, (&y1, &y2))| y1 - subtraction_a * y2)
                                .collect();
                            let subtracted_y = apply_derivative(&first.x, raw_subtracted, derivative_order, derivative_window);
                            let subtracted_points: PlotPoints = first
                                .x
                                .iter()
                                .zip(subtracted_y.iter())
                                .map(|(&x, &y)| [x, y])
                                .collect();

                            let label = format!("Subtracted: #{first_index} − {subtraction_a:.3}×#{second_index}");
                            let line = Line::new(subtracted_points)
                                .name(&label)
                                .color(Color32::from_rgb(255, 226, 132))
                                .width(3.2);

                            plot_ui.line(line);
                        }
                    }

                });

            if let Some((idx, pointer_y)) = pending_drag_begin {
                state.begin_spectrum_drag(idx, pointer_y);
            }

            if let Some(pointer_y) = pending_drag_update {
                state.update_spectrum_drag(pointer_y);
            }

            if pending_drag_end {
                state.end_spectrum_drag();
            }

            if let Some(idx) = pending_select {
                state.select_spectrum(idx);
            }

            if let Some(idx) = pending_subtraction_select {
                state.toggle_subtraction_spectrum(idx);
            }

            if plot_needs_reset {
                state.plot_needs_reset = false;
            }
        });
}

fn plotted_y_values(spectrum: &Spectrum, minmax_scaling: bool, offset: f64) -> Vec<f64> {
    let mut y_values = if minmax_scaling {
        let min = spectrum.y.iter().copied().fold(f64::INFINITY, f64::min);
        let max = spectrum.y.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let range = max - min;
        if range.abs() < f64::EPSILON {
            vec![0.0; spectrum.y.len()]
        } else {
            spectrum.y.iter().map(|&yi| (yi - min) / range).collect()
        }
    } else {
        spectrum.y.clone()
    };

    if offset.abs() >= f64::EPSILON {
        for value in &mut y_values {
            *value += offset;
        }
    }

    y_values
}

/// Symmetric moving-average (box) filter. `window` is the total span in points;
/// the effective half-width is `window / 2` (integer division).
fn smooth_moving_average(y: &[f64], window: usize) -> Vec<f64> {
    if window <= 1 {
        return y.to_vec();
    }
    let n = y.len();
    let half = window / 2;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let start = i.saturating_sub(half);
        let end = (i + half + 1).min(n);
        let sum: f64 = y[start..end].iter().sum();
        out.push(sum / (end - start) as f64);
    }
    out
}

/// Apply a numerical derivative to y values, using central differences on a non-uniform x grid.
///
/// When `window > 1`, a moving-average pre-smooth is applied first to reduce noise.
/// * `First`  — central difference for interior points, forward/backward at endpoints.
/// * `Second` — three-point non-uniform central difference for interior; boundary values
///   are copied from the adjacent interior point.
fn apply_derivative(x: &[f64], y: Vec<f64>, order: DerivativeOrder, window: usize) -> Vec<f64> {
    let n = x.len();
    if n < 3 || y.len() != n {
        return y;
    }
    let y = if window > 1 && order != DerivativeOrder::None {
        smooth_moving_average(&y, window)
    } else {
        y
    };
    match order {
        DerivativeOrder::None => y,
        DerivativeOrder::First => {
            let mut d = vec![0.0_f64; n];
            // Forward difference at left endpoint
            let dx0 = x[1] - x[0];
            d[0] = if dx0.abs() > f64::EPSILON { (y[1] - y[0]) / dx0 } else { 0.0 };
            // Central differences for interior
            for i in 1..n - 1 {
                let dx = x[i + 1] - x[i - 1];
                d[i] = if dx.abs() > f64::EPSILON { (y[i + 1] - y[i - 1]) / dx } else { 0.0 };
            }
            // Backward difference at right endpoint
            let dxn = x[n - 1] - x[n - 2];
            d[n - 1] = if dxn.abs() > f64::EPSILON { (y[n - 1] - y[n - 2]) / dxn } else { 0.0 };
            d
        }
        DerivativeOrder::Second => {
            let mut d = vec![0.0_f64; n];
            // Central second derivative for interior points (non-uniform grid)
            for i in 1..n - 1 {
                let h1 = x[i] - x[i - 1];
                let h2 = x[i + 1] - x[i];
                let denom = h1 * h2 * (h1 + h2);
                d[i] = if denom.abs() > f64::EPSILON {
                    2.0 * (h1 * y[i + 1] - (h1 + h2) * y[i] + h2 * y[i - 1]) / denom
                } else {
                    0.0
                };
            }
            // Copy adjacent interior values to boundary
            d[0] = d[1];
            d[n - 1] = d[n - 2];
            d
        }
    }
}

fn sample_series_on_x(reference_x: &[f64], source_x: &[f64], source_y: &[f64]) -> Vec<f64> {
    if source_x.len() != source_y.len() || source_x.is_empty() {
        return vec![0.0; reference_x.len()];
    }

    if source_x.len() == 1 {
        return vec![source_y[0]; reference_x.len()];
    }

    let ascending = source_x.first().unwrap() <= source_x.last().unwrap();
    let mut result = Vec::with_capacity(reference_x.len());
    let mut index = 0usize;

    for &x in reference_x {
        if ascending {
            while index + 1 < source_x.len() && source_x[index + 1] < x {
                index += 1;
            }
        } else {
            while index + 1 < source_x.len() && source_x[index + 1] > x {
                index += 1;
            }
        }

        let value = if index + 1 >= source_x.len() {
            *source_y.last().unwrap()
        } else {
            let x0 = source_x[index];
            let x1 = source_x[index + 1];
            let y0 = source_y[index];
            let y1 = source_y[index + 1];
            let delta = x1 - x0;
            if delta.abs() < f64::EPSILON {
                y0
            } else {
                let t = (x - x0) / delta;
                y0 + t * (y1 - y0)
            }
        };

        result.push(value);
    }

    result
}

fn distance_to_polyline(plot_ui: &egui_plot::PlotUi<'_>, x: &[f64], y: &[f64], pointer_pos: Pos2) -> f32 {
    let mut previous: Option<Pos2> = None;
    let mut best_distance = f32::INFINITY;

    for (&x_value, &y_value) in x.iter().zip(y.iter()) {
        let current = plot_ui.screen_from_plot(PlotPoint::new(x_value, y_value));
        if let Some(previous_point) = previous {
            best_distance = best_distance.min(distance_to_segment(pointer_pos, previous_point, current));
        } else {
            best_distance = best_distance.min(pointer_pos.distance(current));
        }
        previous = Some(current);
    }

    best_distance
}

fn distance_to_segment(point: Pos2, start: Pos2, end: Pos2) -> f32 {
    let segment = end - start;
    let length_sq = segment.length_sq();
    if length_sq <= f32::EPSILON {
        return point.distance(start);
    }

    let projection = ((point - start).dot(segment) / length_sq).clamp(0.0, 1.0);
    let closest = start + segment * projection;
    point.distance(closest)
}
