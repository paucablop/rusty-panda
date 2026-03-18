use eframe::egui;

use crate::state::AppState;
use crate::ui::{panels, plot};

// ---------------------------------------------------------------------------
// eframe App implementation
// ---------------------------------------------------------------------------

pub struct RustyPandaApp {
    pub state: AppState,
}

impl Default for RustyPandaApp {
    fn default() -> Self {
        Self {
            state: AppState::default(),
        }
    }
}

impl eframe::App for RustyPandaApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // ---- Top panel: menu bar ----
        egui::TopBottomPanel::top("top_bar")
            .frame(
                egui::Frame::default()
                    .fill(egui::Color32::from_rgb(20, 14, 35))
                    .inner_margin(egui::Margin::symmetric(20, 16)),
            )
            .show(ctx, |ui| {
            panels::top_bar(ui, &mut self.state);
            });

        // ---- Left side panel: filters ----
        egui::SidePanel::left("filter_panel")
            .default_width(300.0)
            .min_width(260.0)
            .resizable(true)
            .frame(
                egui::Frame::default()
                    .fill(egui::Color32::from_rgb(15, 10, 28))
                    .inner_margin(egui::Margin::symmetric(16, 18)),
            )
            .show(ctx, |ui| {
                panels::side_panel(ui, &mut self.state);
            });

        // ---- Central panel: plot ----
        egui::CentralPanel::default()
            .frame(
                egui::Frame::default()
                    .fill(egui::Color32::from_rgb(10, 7, 20))
                    .inner_margin(egui::Margin::symmetric(20, 18)),
            )
            .show(ctx, |ui| {
                plot::spectral_plot(ui, &mut self.state);
            });
    }
}
