use std::sync::Arc;

use arrow::array::{
    Float64Array, Float64Builder, Int64Array, ListBuilder, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

fn gaussian(x: f64, mu: f64, sigma: f64, amplitude: f64) -> f64 {
    amplitude * (-(x - mu).powi(2) / (2.0 * sigma.powi(2))).exp()
}

fn generate_spectrum(
    wavenumbers: &[f64],
    peaks: &[(f64, f64, f64)],
    noise_level: f64,
    rng: &mut SimpleRng,
) -> Vec<f64> {
    wavenumbers
        .iter()
        .map(|&wn| {
            let signal: f64 = peaks
                .iter()
                .map(|&(mu, sigma, amp)| gaussian(wn, mu, sigma, amp))
                .sum();
            signal + rng.gauss(0.0, noise_level)
        })
        .collect()
}

/// Minimal deterministic PRNG (xoshiro256**)
struct SimpleRng {
    state: [u64; 4],
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        let mut s = [0u64; 4];
        let mut x = seed;
        for slot in &mut s {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            *slot = x;
        }
        SimpleRng { state: s }
    }

    fn next_u64(&mut self) -> u64 {
        let result = (self.state[1].wrapping_mul(5))
            .rotate_left(7)
            .wrapping_mul(9);
        let t = self.state[1] << 17;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= t;
        self.state[3] = self.state[3].rotate_left(45);
        result
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Box-Muller transform for normal distribution
    fn gauss(&mut self, mean: f64, std_dev: f64) -> f64 {
        let u1 = self.next_f64().max(1e-15);
        let u2 = self.next_f64();
        let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
        mean + std_dev * z
    }
}

fn main() {
    let mut rng = SimpleRng::new(42);

    // Wavenumbers: 4000 → 2002, step 2
    let wavenumbers: Vec<f64> = (0..1000).map(|i| 4000.0 - i as f64 * 2.0).collect();

    let samples = ["Sample_A", "Sample_B", "Sample_C"];
    let concentrations = [0.1, 0.5, 1.0, 2.0, 5.0];
    let operators = ["Alice", "Bob"];

    let sample_peaks: Vec<(&str, Vec<(f64, f64, f64)>)> = vec![
        ("Sample_A", vec![(3400.0, 80.0, 0.8), (2900.0, 40.0, 0.5), (2350.0, 30.0, 0.3)]),
        ("Sample_B", vec![(3200.0, 60.0, 0.6), (2800.0, 50.0, 0.7), (2500.0, 35.0, 0.4)]),
        ("Sample_C", vec![(3600.0, 70.0, 0.9), (3000.0, 45.0, 0.4), (2200.0, 25.0, 0.5)]),
    ];

    // Collect all rows
    let mut all_x: Vec<Vec<f64>> = Vec::new();
    let mut all_y: Vec<Vec<f64>> = Vec::new();
    let mut all_sample: Vec<String> = Vec::new();
    let mut all_conc: Vec<f64> = Vec::new();
    let mut all_operator: Vec<String> = Vec::new();
    let mut all_id: Vec<i64> = Vec::new();

    let mut row_id: i64 = 0;
    for sample in &samples {
        let peaks_base = &sample_peaks
            .iter()
            .find(|(name, _)| name == sample)
            .unwrap()
            .1;

        for &conc in &concentrations {
            let peaks: Vec<(f64, f64, f64)> = peaks_base
                .iter()
                .map(|&(mu, sigma, amp)| (mu, sigma, amp * conc))
                .collect();

            for &operator in &operators {
                let y = generate_spectrum(&wavenumbers, &peaks, 0.005 * conc, &mut rng);

                all_x.push(wavenumbers.clone());
                all_y.push(y);
                all_sample.push(sample.to_string());
                all_conc.push(conc);
                all_operator.push(operator.to_string());
                all_id.push(row_id);
                row_id += 1;
            }
        }
    }

    // Build Arrow arrays
    let mut x_builder = ListBuilder::new(Float64Builder::new());
    for row in &all_x {
        let values = x_builder.values();
        for &v in row {
            values.append_value(v);
        }
        x_builder.append(true);
    }
    let x_array = x_builder.finish();

    let mut y_builder = ListBuilder::new(Float64Builder::new());
    for row in &all_y {
        let values = y_builder.values();
        for &v in row {
            values.append_value(v);
        }
        y_builder.append(true);
    }
    let y_array = y_builder.finish();

    let sample_array = StringArray::from(
        all_sample.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    );
    let conc_array = Float64Array::from(all_conc);
    let operator_array = StringArray::from(
        all_operator.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    );
    let id_array = Int64Array::from(all_id);

    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::List(Arc::new(Field::new("item", DataType::Float64, true))), false),
        Field::new("y", DataType::List(Arc::new(Field::new("item", DataType::Float64, true))), false),
        Field::new("sample", DataType::Utf8, false),
        Field::new("concentration", DataType::Float64, false),
        Field::new("operator", DataType::Utf8, false),
        Field::new("measurement_id", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(x_array),
            Arc::new(y_array),
            Arc::new(sample_array),
            Arc::new(conc_array),
            Arc::new(operator_array),
            Arc::new(id_array),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Write Parquet
    let output_path = "sample_data.parquet";
    let file = std::fs::File::create(output_path).expect("Failed to create output file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("Failed to create writer");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    println!(
        "Wrote {} spectra ({} wavenumbers each) to {output_path}",
        row_id,
        wavenumbers.len()
    );
}
