# Rusty Panda – Spectral Data Viewer

A cross-platform (macOS, Windows, Linux) desktop application for visualizing spectral data, built entirely in Rust with **egui**.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      main.rs                            │
│   eframe::run_native → RustyPandaApp                    │
└──────────────────┬──────────────────────────────────────┘
                   │
       ┌───────────┼──────────────┐
       ▼           ▼              ▼
┌──────────┐ ┌──────────┐ ┌────────────┐
│  app.rs  │ │ state.rs │ │  color.rs  │
│  (eframe │ │ AppState │ │  Palette   │
│   App)   │ │ + filters│ │  ColorMap  │
└──────────┘ └──────────┘ └────────────┘
       │           │
       ▼           ▼
┌─────────────────────────────┐
│         ui/                 │
│  panels.rs  - side panel,   │
│               top bar,      │
│               file dialog   │
│  plot.rs    - egui_plot     │
│               rendering     │
└─────────────────────────────┘
       │
       ▼
┌─────────────────────────────┐
│        data/                │
│  model.rs   - Spectrum,     │
│               SpectralDataset│
│               MetadataValue │
│  loader.rs  - Parquet, JSON,│
│               CSV parsing   │
│  filter.rs  - FilterState,  │
│               filtered_indices│
└─────────────────────────────┘
```

## Module Responsibilities

| Module | Responsibility |
|---|---|
| `data::model` | Core types: `Spectrum`, `SpectralDataset`, `MetadataValue` |
| `data::loader` | File parsing (Parquet, JSON, CSV) |
| `data::filter` | Filtering logic, independent of UI |
| `state` | `AppState`: filters, colour column, visible indices |
| `color` | HSL palette generation, `ColorMap` metadata→colour |
| `ui::panels` | Side panel (checkboxes), top bar (menu), file dialog |
| `ui::plot` | `egui_plot` rendering of filtered spectra |
| `app` | `eframe::App` implementation, layout |

## Crate Choices

| Crate | Purpose | Why |
|---|---|---|
| `eframe` 0.31 | Application framework | Native egui runtime for macOS/Windows/Linux |
| `egui_plot` 0.31 | Plotting | First-party egui plotting, hardware-accelerated |
| `rfd` 0.15 | File dialogs | Native OS dialogs, cross-platform |
| `arrow` 54 | Arrow in-memory format | Read columnar data from Parquet (List, primitives) |
| `parquet` 54 | Parquet reader | Read `.parquet` files natively in pure Rust |
| `serde_json` 1 | JSON parsing | Fast, reliable JSON loader |
| `csv` 1 | CSV parsing | Industry-standard CSV reader |
| `palette` 0.7 | Colour generation | Perceptually uniform colour spaces |
| `anyhow` / `thiserror` | Error handling | Ergonomic error chains |

## Data Flow

```
User clicks "Open…"
   │
   ▼
rfd::FileDialog → file path
   │
   ▼
loader::load_file(path)
   │  dispatches by extension (.parquet / .json / .csv)
   ▼
SpectralDataset { spectra, column_names, unique_values }
   │
   ▼
AppState::set_dataset()
   │  init filters (all selected)
   │  choose default colour column
   │  build ColorMap
   ▼
update() loop
   │
   ├─ SidePanel: checkboxes per metadata column
   │     toggle → state.toggle_filter_value()
   │            → state.refilter()
   │
   ├─ TopBar: file count, status messages
   │
   └─ CentralPanel: egui_plot
         iterate visible_indices
         color each line by ColorMap
```

## File Formats

### Parquet (recommended)

The preferred format. Works with both **Pandas** and **Polars**.

Expected schema:
- `x`: `List<Float64>` — wavenumber arrays
- `y`: `List<Float64>` — intensity arrays
- Any additional columns — metadata (strings, ints, floats, bools)

Generate from **Pandas**:
```python
import pandas as pd
df.to_parquet("data.parquet", engine="pyarrow")
```

Generate from **Polars**:
```python
import polars as pl
df.write_parquet("data.parquet")
```

Or use the bundled converter: `python scripts/convert.py input.pkl output.parquet`

### JSON

```json
[
  {"x": [4000.0, 3998.0], "y": [0.12, 0.14], "sample": "A", "pH": 7.0},
  ...
]
```

Generate with: `df.to_json("output.json", orient="records")`

### CSV

```csv
x,y,sample,pH
4000.0;3998.0,0.12;0.14,A,7.0
```

x and y values are semicolon-separated within the CSV cell.

## Building

```bash
cargo build --release
```

## Running

```bash
cargo run --release
```

Or use the sample data generator:

```bash
cd scripts
python generate_sample_data.py
cd ..
cargo run --release
# Then File → Open… → select sample_data.parquet
```

## Caveats & Edge Cases

### Large Datasets
- Each spectrum with 1000 wavenumbers uses ~16KB (two `Vec<f64>`)
- 10,000 spectra ≈ 160MB — fits comfortably in memory
- Parquet files are compressed — a 160MB dataset may be only ~20MB on disk
- `egui_plot` renders all visible lines every frame; >5000 overlaid spectra may drop below 60fps
- Consider downsampling or LOD for very large datasets

### Type Coercion
- Arrow `Int32`/`Int64` → Rust `i64`; Arrow `Float32`/`Float64` → Rust `f64`
- Arrow `null` → `MetadataValue::Null`
- Arrow `Utf8`/`LargeUtf8` → `MetadataValue::String`
- Arrow `Boolean` → `MetadataValue::Bool`
- List columns (`List<Float64>`, `LargeList<Float64>`) → `Vec<f64>`

### UI Scaling
- egui handles DPI scaling natively on macOS and Windows
- Plot performance scales with number of visible points × visible spectra
- Filter panel scrolls for datasets with many metadata columns or many unique values

### Future Extensibility
- **Zoom/brush**: already supported by `egui_plot` (box zoom, drag, scroll)
- **Export**: add export button → write filtered spectra to Parquet/JSON/CSV
- **Async loading**: use `poll_promise` or channels for background file I/O
- **Theming**: egui supports light/dark themes via `Visuals`
- **Arrow IPC**: add `.arrow` / `.ipc` support for streaming use cases
