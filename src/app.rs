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
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            panels::top_bar(ui, &mut self.state);
        });

        // ---- Left side panel: filters ----
        egui::SidePanel::left("filter_panel")
            .default_width(220.0)
            .resizable(true)
            .show(ctx, |ui| {
                panels::side_panel(ui, &mut self.state);
            });

        // ---- Central panel: plot ----
        egui::CentralPanel::default().show(ctx, |ui| {
            plot::spectral_plot(ui, &self.state);
        });
    }
}
