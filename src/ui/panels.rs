use eframe::egui::{self, Color32, ScrollArea, Ui, RichText};

use crate::state::AppState;

// ---------------------------------------------------------------------------
// Left side panel – filter widgets
// ---------------------------------------------------------------------------

/// Render the left filter panel.
pub fn side_panel(ui: &mut Ui, state: &mut AppState) {
    // ---- Logo (centered) ----
    let logo = egui::include_image!("../../assets/logo.png");
    ui.vertical_centered(|ui: &mut Ui| {
        ui.add(
            egui::Image::new(logo)
                .max_width(ui.available_width() * 0.8)
                .max_height(120.0)
                .rounding(4.0),
        );
    });
    ui.add_space(4.0);

    ui.heading("Filters");
    ui.separator();

    let dataset = match &state.dataset {
        Some(ds) => ds,
        None => {
            ui.label("No dataset loaded.");
            return;
        }
    };

    // Clone what we need so we can mutate state inside the loop.
    let columns = dataset.column_names.clone();
    let unique = dataset.unique_values.clone();

    ScrollArea::vertical()
        .auto_shrink([false, false])
        .show(ui, |ui: &mut Ui| {
            // ---- Colour-by selector ----
            ui.strong("Color by");
            let current_color_col = state.color_column.clone().unwrap_or_default();
            egui::ComboBox::from_id_salt("color_by")
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
            ui.separator();

            // ---- Per-column filter widgets (collapsible) ----
            for col in &columns {
                let Some(all_values) = unique.get(col) else {
                    continue;
                };

                let selected = state
                    .filters
                    .entry(col.clone())
                    .or_default();

                // Show count of selected / total in the header
                let n_selected = selected.len();
                let n_total = all_values.len();
                let header_text = format!("{col}  ({n_selected}/{n_total})");

                egui::CollapsingHeader::new(RichText::new(header_text).strong())
                    .id_salt(col)
                    .default_open(false)
                    .show(ui, |ui: &mut Ui| {
                        // Select all / none buttons
                        ui.horizontal(|ui: &mut Ui| {
                            if ui.small_button("All").clicked() {
                                state.select_all(col);
                            }
                            if ui.small_button("None").clicked() {
                                state.select_none(col);
                            }
                        });

                        // Re-borrow after potential mutation from All/None
                        let selected = state
                            .filters
                            .entry(col.clone())
                            .or_default();

                        for val in all_values {
                            let is_selected = selected.contains(val);
                            let label = val.to_string();

                            // Show colour swatch if this is the colour column
                            let mut text = RichText::new(&label);
                            if state.color_column.as_deref() == Some(col) {
                                if let Some(cm) = &state.color_map {
                                    let c = cm.color_for(val);
                                    text = text.color(c);
                                }
                            }

                            let mut checked = is_selected;
                            if ui.checkbox(&mut checked, text).changed() {
                                if checked {
                                    selected.insert(val.clone());
                                } else {
                                    selected.remove(val);
                                }
                            }
                        }
                    });
            }
        });

    // Recompute visible indices after any checkbox changes.
    state.refilter();
}

// ---------------------------------------------------------------------------
// Top bar
// ---------------------------------------------------------------------------

/// Render the top menu / toolbar.
pub fn top_bar(ui: &mut Ui, state: &mut AppState) {
    egui::menu::bar(ui, |ui: &mut Ui| {
        ui.menu_button("File", |ui: &mut Ui| {
            if ui.button("Open…").clicked() {
                open_file_dialog(state);
                ui.close_menu();
            }
        });

        ui.separator();

        if let Some(ds) = &state.dataset {
            ui.label(format!(
                "{} spectra loaded, {} visible",
                ds.len(),
                state.visible_indices.len()
            ));
        }

        ui.separator();

        if ui
            .selectable_label(state.minmax_scaling, "Min-Max Scaling")
            .clicked()
        {
            state.minmax_scaling = !state.minmax_scaling;
        }

        if let Some(msg) = &state.status_message {
            ui.label(RichText::new(msg).color(Color32::RED));
        }
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
