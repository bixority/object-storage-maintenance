mod commands;
mod object_storage;
mod s3;
mod uploader;

use crate::commands::archive;
use clap::{Parser, Subcommand};
use std::error::Error;

#[derive(Subcommand, Debug)]
enum Commands {
    Archive {
        #[arg(long)]
        src_bucket: String,

        #[arg(long)]
        src_prefix: String,

        #[arg(long)]
        dst_bucket: String,

        #[arg(long)]
        dst_prefix: String,
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
            src_bucket,
            src_prefix,
            dst_bucket,
            dst_prefix,
        }) => {
            if let Err(e) = archive(src_bucket, src_prefix, dst_bucket, dst_prefix).await {
                eprintln!("Error running 'archive' command: {}", e);
            }
        }
        None => {
            println!("No subcommand selected. Add a subcommand like 'archive'.");
        }
    }

    Ok(())
}
