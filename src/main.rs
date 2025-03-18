mod commands;
mod compressor;
mod helpers;
mod object_storage;
mod s3;
mod uploader;

use crate::commands::archive;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use std::error::Error;
use std::io;
use std::io::Write;

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

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .dns_resolver(trust_dns_resolver::TokioAsyncResolver::tokio_from_system_conf().unwrap())
        .build()
        .unwrap();

    match args.command {
        Some(Commands::Archive {
            src,
            dst,
            cutoff,
            buffer,
        }) => {
            if let Err(e) = archive(src, dst, cutoff, buffer).await {
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
