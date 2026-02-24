mod app;
mod color;
mod data;
mod state;
mod ui;

use app::RustyPandaApp;
use eframe::egui;

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
            // Install image loaders so egui can render png/jpg/etc.
            egui_extras::install_image_loaders(&cc.egui_ctx);
            Ok(Box::new(RustyPandaApp::default()))
        }),
    )
}
