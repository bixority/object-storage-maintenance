mod commands;
mod compressor;
mod helpers;
mod object_storage;
mod s3;
mod uploader;

use crate::commands::archive;
use async_compression::Level;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use std::error::Error;
use std::io;
use std::io::Write;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(ValueEnum, Debug, Clone)]
enum Compression {
    Fastest,
    Best,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Archive {
        #[arg(long)]
        src: String,

        #[arg(long)]
        dst: String,

        #[arg(long)]
        cutoff: Option<DateTime<Utc>>,

        #[arg(long, default_value_t = 100 * 1024 * 1024)] // 100MB
        buffer: usize,

        #[arg(long, value_enum, default_value_t = Compression::Fastest)]
        compression: Compression,
    },
}

#[derive(Parser, Debug)]
#[command(version, about = "Object storage maintenance tool", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    match args.command {
        Some(Commands::Archive {
            src,
            dst,
            cutoff,
            buffer,
            compression,
        }) => {
            let level = match compression {
                Compression::Fastest => Level::Fastest,
                Compression::Best => Level::Best,
            };

            if let Err(e) = archive(src, dst, cutoff, buffer, level).await {
                eprintln!("Error running 'archive' command: {e}");
            }
        }
        None => {
            println!("No subcommand selected. Add a subcommand like 'archive'.");
        }
    }

    io::stdout().flush().unwrap();

    Ok(())
}
