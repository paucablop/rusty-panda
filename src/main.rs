mod app;
mod color;
mod data;
mod state;
mod ui;

use app::RustyPandaApp;
use eframe::egui::{self, Color32, CornerRadius, Stroke};

/// Apply a modern purple dark theme with rounded widgets.
fn setup_purple_theme(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::dark();

    // ---- Backgrounds ----
    visuals.window_fill = Color32::from_rgb(22, 17, 38);
    visuals.panel_fill = Color32::from_rgb(22, 17, 38);
    visuals.faint_bg_color = Color32::from_rgb(30, 24, 48);
    visuals.extreme_bg_color = Color32::from_rgb(14, 10, 24);

    // ---- Selection ----
    visuals.selection.bg_fill = Color32::from_rgb(124, 77, 255);
    visuals.selection.stroke = Stroke::new(1.0, Color32::from_rgb(179, 136, 255));

    // ---- Hyperlink ----
    visuals.hyperlink_color = Color32::from_rgb(179, 136, 255);

    // ---- Window / menu corner radius ----
    visuals.window_corner_radius = CornerRadius::same(12);
    visuals.menu_corner_radius = CornerRadius::same(8);

    // ---- Non-interactive widgets (labels, separators) ----
    visuals.widgets.noninteractive.bg_fill = Color32::from_rgb(30, 24, 48);
    visuals.widgets.noninteractive.weak_bg_fill = Color32::from_rgb(30, 24, 48);
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, Color32::from_rgb(50, 40, 75));
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0, Color32::from_rgb(200, 190, 220));
    visuals.widgets.noninteractive.corner_radius = CornerRadius::same(8);

    // ---- Inactive widgets (buttons at rest) ----
    visuals.widgets.inactive.bg_fill = Color32::from_rgb(45, 37, 69);
    visuals.widgets.inactive.weak_bg_fill = Color32::from_rgb(40, 32, 62);
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, Color32::from_rgb(65, 52, 100));
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, Color32::from_rgb(210, 200, 230));
    visuals.widgets.inactive.corner_radius = CornerRadius::same(8);

    // ---- Hovered widgets ----
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(74, 56, 112);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(64, 48, 98);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.5, Color32::from_rgb(124, 77, 255));
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.5, Color32::from_rgb(230, 220, 250));
    visuals.widgets.hovered.corner_radius = CornerRadius::same(8);

    // ---- Active widgets (being clicked) ----
    visuals.widgets.active.bg_fill = Color32::from_rgb(124, 77, 255);
    visuals.widgets.active.weak_bg_fill = Color32::from_rgb(100, 60, 220);
    visuals.widgets.active.bg_stroke = Stroke::new(1.5, Color32::from_rgb(179, 136, 255));
    visuals.widgets.active.fg_stroke = Stroke::new(2.0, Color32::from_rgb(255, 255, 255));
    visuals.widgets.active.corner_radius = CornerRadius::same(8);

    // ---- Open widgets (expanded dropdowns, menus) ----
    visuals.widgets.open.bg_fill = Color32::from_rgb(55, 44, 82);
    visuals.widgets.open.weak_bg_fill = Color32::from_rgb(48, 38, 72);
    visuals.widgets.open.bg_stroke = Stroke::new(1.0, Color32::from_rgb(124, 77, 255));
    visuals.widgets.open.fg_stroke = Stroke::new(1.0, Color32::from_rgb(230, 220, 250));
    visuals.widgets.open.corner_radius = CornerRadius::same(8);

    ctx.set_visuals(visuals);

    // Spacing for a modern feel
    let mut style = (*ctx.style()).clone();
    style.spacing.button_padding = egui::vec2(8.0, 4.0);
    style.spacing.item_spacing = egui::vec2(8.0, 4.0);
    ctx.set_style(style);
}

fn main() -> eframe::Result {
    env_logger::init();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 800.0])
            .with_min_inner_size([600.0, 400.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Rusty Panda – Spectral Viewer",
        options,
        Box::new(|cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            setup_purple_theme(&cc.egui_ctx);
            Ok(Box::new(RustyPandaApp::default()))
        }),
    )
}
