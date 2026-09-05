mod app;
mod types;

use anyhow::Result;
use app::MiniRedisGuiApp;
use eframe::egui;

fn main() -> Result<()> {
    // Create dedicated Tokio multi-threaded runtime for background Redis async tasks
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let handle = rt.handle().clone();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([900.0, 650.0])
            .with_min_inner_size([700.0, 500.0])
            .with_title("Mini Redis Studio"),
        ..Default::default()
    };

    eframe::run_native(
        "Mini Redis Studio",
        options,
        Box::new(|_cc| Ok(Box::new(MiniRedisGuiApp::new(handle)))),
    )
    .map_err(|e| anyhow::anyhow!("Failed to run eframe GUI: {}", e))
}
