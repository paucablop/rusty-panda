#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod color;
mod data;
mod state;
mod ui;

use app::RustyPandaApp;
use eframe::egui::{self, Color32, CornerRadius, Stroke};
use image::ImageReader;

fn load_app_icon() -> egui::IconData {
    let image_bytes = include_bytes!("../assets/logo.png");
    let image = ImageReader::new(std::io::Cursor::new(image_bytes))
        .with_guessed_format()
        .expect("Failed to detect app icon format")
        .decode()
        .expect("Failed to decode app icon")
        .into_rgba8();

    let (width, height) = image.dimensions();

    egui::IconData {
        rgba: image.into_raw(),
        width,
        height,
    }
}

/// Apply a modern purple theme with softer contrast and rounded surfaces.
fn setup_modern_theme(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::dark();

    // ---- Backgrounds ----
    visuals.window_fill = Color32::from_rgb(18, 12, 34);
    visuals.panel_fill = Color32::from_rgb(22, 15, 40);
    visuals.faint_bg_color = Color32::from_rgb(34, 24, 56);
    visuals.extreme_bg_color = Color32::from_rgb(12, 8, 24);

    // ---- Selection ----
    visuals.selection.bg_fill = Color32::from_rgb(148, 92, 255);
    visuals.selection.stroke = Stroke::new(1.0, Color32::from_rgb(216, 184, 255));

    // ---- Hyperlink ----
    visuals.hyperlink_color = Color32::from_rgb(201, 156, 255);
    visuals.override_text_color = Some(Color32::from_rgb(235, 228, 244));

    // ---- Window / menu corner radius ----
    visuals.window_corner_radius = CornerRadius::same(18);
    visuals.menu_corner_radius = CornerRadius::same(14);

    // ---- Non-interactive widgets (labels, separators) ----
    visuals.widgets.noninteractive.bg_fill = Color32::from_rgb(30, 22, 48);
    visuals.widgets.noninteractive.weak_bg_fill = Color32::from_rgb(30, 22, 48);
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, Color32::from_rgb(58, 43, 89));
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0, Color32::from_rgb(175, 159, 198));
    visuals.widgets.noninteractive.corner_radius = CornerRadius::same(14);

    // ---- Inactive widgets (buttons at rest) ----
    visuals.widgets.inactive.bg_fill = Color32::from_rgb(43, 31, 67);
    visuals.widgets.inactive.weak_bg_fill = Color32::from_rgb(36, 26, 58);
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, Color32::from_rgb(76, 58, 115));
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, Color32::from_rgb(225, 214, 239));
    visuals.widgets.inactive.corner_radius = CornerRadius::same(14);

    // ---- Hovered widgets ----
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(62, 43, 98);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(54, 38, 86);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.5, Color32::from_rgb(171, 113, 255));
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.5, Color32::from_rgb(246, 239, 252));
    visuals.widgets.hovered.corner_radius = CornerRadius::same(14);

    // ---- Active widgets (being clicked) ----
    visuals.widgets.active.bg_fill = Color32::from_rgb(148, 92, 255);
    visuals.widgets.active.weak_bg_fill = Color32::from_rgb(118, 70, 214);
    visuals.widgets.active.bg_stroke = Stroke::new(1.5, Color32::from_rgb(224, 188, 255));
    visuals.widgets.active.fg_stroke = Stroke::new(2.0, Color32::from_rgb(255, 255, 255));
    visuals.widgets.active.corner_radius = CornerRadius::same(14);

    // ---- Open widgets (expanded dropdowns, menus) ----
    visuals.widgets.open.bg_fill = Color32::from_rgb(46, 33, 72);
    visuals.widgets.open.weak_bg_fill = Color32::from_rgb(40, 29, 63);
    visuals.widgets.open.bg_stroke = Stroke::new(1.0, Color32::from_rgb(171, 113, 255));
    visuals.widgets.open.fg_stroke = Stroke::new(1.0, Color32::from_rgb(240, 230, 251));
    visuals.widgets.open.corner_radius = CornerRadius::same(14);

    visuals.window_stroke = Stroke::new(1.0, Color32::from_rgb(63, 47, 95));

    ctx.set_visuals(visuals);

    // Spacing and typography
    let mut style = (*ctx.style()).clone();
    style.spacing.button_padding = egui::vec2(14.0, 9.0);
    style.spacing.item_spacing = egui::vec2(10.0, 10.0);
    style.spacing.menu_margin = egui::Margin::same(10);
    style.spacing.indent = 18.0;
    style.text_styles.insert(
        egui::TextStyle::Heading,
        egui::FontId::new(26.0, egui::FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Body,
        egui::FontId::new(16.0, egui::FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Button,
        egui::FontId::new(15.0, egui::FontFamily::Proportional),
    );
    style.text_styles.insert(
        egui::TextStyle::Small,
        egui::FontId::new(13.0, egui::FontFamily::Proportional),
    );
    ctx.set_style(style);
}

fn main() -> eframe::Result {
    env_logger::init();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 800.0])
            .with_min_inner_size([600.0, 400.0])
            .with_icon(load_app_icon()),
        ..Default::default()
    };

    eframe::run_native(
        "Rusty Panda – Spectral Viewer",
        options,
        Box::new(|cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            setup_modern_theme(&cc.egui_ctx);
            Ok(Box::new(RustyPandaApp::default()))
        }),
    )
}
