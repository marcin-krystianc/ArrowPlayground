// src/main.rs
// > cargo build --release && ./target/release/benchmarks

use std::{fs::File};

use std::time::Instant;

// Import necessary types from arrow-schema and parquet crates
use parquet::file::{
    reader::{FileReader, SerializedFileReader},
};

// The main function is marked with #[tokio::main] to run async code
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/my.parquet"; // Make sure this file exists in the same directory

    println!("Attempting to read metadata from: {}", file_path);

    // Record the start time
    let start = Instant::now();
    for _i in 0..1000 {
        // 1. Open the Parquet file
        let file = File::open(file_path)?;

        // 2. Create a SerializedFileReader
        // This reader is used to read Parquet files and their metadata.
        // It's "serialized" because it expects a Read + Seek stream, like a File.
        let reader = SerializedFileReader::new(file)?;

        // 3. Get the FileMetaData
        // This contains top-level information about the Parquet file.
        let _file_metadata = reader.metadata().file_metadata();
    }

    // Record the end time
    let end = Instant::now();
    
    // Calculate the duration
    let duration = end.duration_since(start); // or `start.elapsed()`
    println!("Milliseconds: {} ms", duration.as_millis());

    println!("\nSuccessfully read Parquet metadata!");

    Ok(())
}