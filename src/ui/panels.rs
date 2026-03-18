use eframe::egui::{self, Align, Button, Color32, CornerRadius, Frame, RichText, ScrollArea, Stroke, Ui};

use crate::state::AppState;

// ---------------------------------------------------------------------------
// Left side panel – filter widgets
// ---------------------------------------------------------------------------

/// Render the left filter panel.
pub fn side_panel(ui: &mut Ui, state: &mut AppState) {
    let mut filters_changed = false;

    ScrollArea::vertical()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            hero_card(ui);
            ui.add_space(14.0);

            let dataset = match &state.dataset {
                Some(ds) => ds,
                None => {
                    section_card(ui, |ui| {
                        ui.label(
                            RichText::new("No dataset loaded yet")
                                .size(18.0)
                                .strong(),
                        );
                        ui.label("Use Open Data to load Parquet, JSON, or CSV files.");
                    });
                    return;
                }
            };

            let columns = dataset.column_names.clone();
            let unique = dataset.unique_values.clone();

            section_card(ui, |ui| {
                ui.label(
                    RichText::new("Color by")
                        .strong()
                        .size(17.0),
                );

                let current_color_col = state.color_column.clone().unwrap_or_else(|| "None".to_string());
                egui::ComboBox::from_id_salt("color_by")
                    .width(ui.available_width())
                    .selected_text(&current_color_col)
                    .show_ui(ui, |ui: &mut Ui| {
                        for col in &columns {
                            if ui
                                .selectable_label(current_color_col == *col, col)
                                .clicked()
                            {
                                state.set_color_column(col.clone());
                            }
                        }
                    });
            });

            ui.add_space(12.0);

            section_card(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new("Metadata filters")
                            .strong()
                            .size(17.0),
                    );
                    ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                        ui.label(
                            RichText::new(format!("{} columns", columns.len()))
                                .small()
                                .color(Color32::from_rgb(191, 175, 214)),
                        );
                    });
                });

                ui.add_space(8.0);

                for col in &columns {
                    let Some(all_values) = unique.get(col) else {
                        continue;
                    };

                    let selected = state.filters.entry(col.clone()).or_default();
                    let n_selected = selected.len();
                    let n_total = all_values.len();
                    let header_text = format!("{col}  ({n_selected}/{n_total})");

                    egui::CollapsingHeader::new(RichText::new(header_text).strong())
                        .id_salt(col)
                        .default_open(false)
                        .show(ui, |ui: &mut Ui| {
                            ui.horizontal(|ui: &mut Ui| {
                                if pill_button(ui, "All").clicked() {
                                    state.select_all(col);
                                }
                                if pill_button(ui, "None").clicked() {
                                    state.select_none(col);
                                }
                            });

                            ui.add_space(6.0);

                            let selected = state.filters.entry(col.clone()).or_default();

                            for val in all_values {
                                let is_selected = selected.contains(val);
                                let label = val.to_string();
                                let mut text = RichText::new(&label);

                                if state.color_column.as_deref() == Some(col) {
                                    if let Some(cm) = &state.color_map {
                                        text = text.color(cm.color_for(val));
                                    }
                                }

                                let mut checked = is_selected;
                                if ui.checkbox(&mut checked, text).changed() {
                                    if checked {
                                        selected.insert(val.clone());
                                    } else {
                                        selected.remove(val);
                                    }
                                    filters_changed = true;
                                }
                            }
                        });

                    ui.add_space(6.0);
                }
            });
        });

    if filters_changed {
        state.refilter();
    }
}

// ---------------------------------------------------------------------------
// Top bar
// ---------------------------------------------------------------------------

/// Render the top menu / toolbar.
pub fn top_bar(ui: &mut Ui, state: &mut AppState) {
    let active_filters = state
        .dataset
        .as_ref()
        .map(|ds| {
            ds.column_names
                .iter()
                .filter(|col| {
                    let Some(all_vals) = ds.unique_values.get(*col) else {
                        return false;
                    };
                    let selected = state.filters.get(*col);
                    match selected {
                        Some(selected) => selected.len() < all_vals.len(),
                        None => false,
                    }
                })
                .count()
        })
        .unwrap_or(0);

    ui.horizontal(|ui| {
        ui.vertical(|ui| {
            ui.label(
                RichText::new("Rusty Panda")
                    .size(28.0)
                    .strong()
                    .color(Color32::from_rgb(241, 247, 252)),
            );
        });

        ui.add_space(12.0);

        ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
            if let Some(msg) = &state.status_message {
                ui.label(RichText::new(msg).color(Color32::from_rgb(255, 126, 126)));
            }

            if state.loading {
                ui.add(egui::Spinner::new().size(18.0));
            }

            if toggle_chip(ui, &mut state.auto_scale, "Auto-scale") && state.auto_scale {
                state.request_plot_reset();
            }

            if toggle_chip(ui, &mut state.minmax_scaling, "Min-max") && state.auto_scale {
                state.request_plot_reset();
            }

            if ui
                .add(
                    Button::new(
                        RichText::new("Open Data")
                            .strong()
                            .color(Color32::from_rgb(250, 254, 255)),
                    )
                    .fill(Color32::from_rgb(148, 92, 255))
                    .corner_radius(CornerRadius::same(18)),
                )
                .clicked()
            {
                open_file_dialog(state);
            }

            if let Some(ds) = &state.dataset {
                stat_badge(ui, format!("{} filters active", active_filters));
                stat_badge(ui, format!("{} visible", state.visible_indices.len()));
                stat_badge(ui, format!("{} spectra", ds.len()));
            }
        });
    });
}

fn hero_card(ui: &mut Ui) {
    section_card(ui, |ui| {
        let logo = egui::include_image!("../../assets/logo.png");
        ui.vertical_centered(|ui: &mut Ui| {
            ui.add(
                egui::Image::new(logo)
                    .max_width(ui.available_width() * 0.7)
                    .max_height(112.0)
                    .corner_radius(14.0),
            );
        });
    });
}

fn section_card(ui: &mut Ui, add_contents: impl FnOnce(&mut Ui)) {
    Frame::default()
        .fill(Color32::from_rgb(26, 18, 45))
        .stroke(Stroke::new(1.0, Color32::from_rgb(63, 47, 95)))
        .corner_radius(CornerRadius::same(20))
        .inner_margin(egui::Margin::symmetric(16, 16))
        .show(ui, add_contents);
}

fn pill_button(ui: &mut Ui, label: &str) -> egui::Response {
    ui.add(
        Button::new(RichText::new(label).small())
            .corner_radius(CornerRadius::same(14))
            .fill(Color32::from_rgb(39, 27, 63)),
    )
}

fn toggle_chip(ui: &mut Ui, value: &mut bool, label: &str) -> bool {
    let fill = if *value {
        Color32::from_rgb(148, 92, 255)
    } else {
        Color32::from_rgb(39, 27, 63)
    };
    let text = if *value {
        Color32::from_rgb(250, 254, 255)
    } else {
        Color32::from_rgb(224, 214, 238)
    };

    if ui
        .add(
            Button::new(RichText::new(label).color(text))
                .fill(fill)
                .corner_radius(CornerRadius::same(18)),
        )
        .clicked()
    {
        *value = !*value;
        return true;
    }

    false
}

fn stat_badge(ui: &mut Ui, text: String) {
    Frame::default()
        .fill(Color32::from_rgb(31, 22, 50))
        .stroke(Stroke::new(1.0, Color32::from_rgb(63, 47, 95)))
        .corner_radius(CornerRadius::same(16))
        .inner_margin(egui::Margin::symmetric(10, 8))
        .show(ui, |ui| {
            ui.label(
                RichText::new(text)
                    .small()
                    .color(Color32::from_rgb(224, 214, 238)),
            );
        });
}

// ---------------------------------------------------------------------------
// File dialog
// ---------------------------------------------------------------------------

pub fn open_file_dialog(state: &mut AppState) {
    let file = rfd::FileDialog::new()
        .set_title("Open spectral data")
        .add_filter("Supported files", &["parquet", "pq", "json", "csv"])
        .add_filter("Parquet", &["parquet", "pq"])
        .add_filter("JSON", &["json"])
        .add_filter("CSV", &["csv"])
        .pick_file();

    if let Some(path) = file {
        state.loading = true;
        match crate::data::loader::load_file(&path) {
            Ok(dataset) => {
                log::info!(
                    "Loaded {} spectra with columns {:?}",
                    dataset.len(),
                    dataset.column_names
                );
                state.set_dataset(dataset);
            }
            Err(e) => {
                log::error!("Failed to load file: {e:#}");
                state.status_message = Some(format!("Error: {e:#}"));
                state.loading = false;
            }
        }
    }
}
