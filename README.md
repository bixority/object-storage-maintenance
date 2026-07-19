# Object Storage Maintenance Tool

## Overview

The **Object Storage Maintenance Tool** is a command-line utility designed to **archive and compress objects** stored in
object storage in a **streaming** manner. It helps manage storage efficiently by gathering objects older than a
particular date and storing them in a single **TAR** archive, compressed with **XZ**.

### Why Use This Tool?

In object storage environments, a large number of small objects (e.g., audit logs, event records) can lead to
inefficient storage use. For example:

- Many objects are **tiny (100 bytes or even 0 bytes)** but still occupy **4KB** due to storage overhead.
- Storing such objects as a **TAR archive** reduces storage overhead.
- Compressing the archive with **XZ** further optimizes storage usage.

## Features

- **Archive Objects**: Consolidates multiple objects into a single **TAR** file.
- **Streaming Compression**: Uses **XZ** to reduce storage footprint.
- **Multi-Cloud Support**: Works with AWS S3, Google Cloud Storage, Azure Blob Storage, and local files.
- **S3-Compatible**: Works with AWS S3 compatible object storages (MinIO, Cloudflare R2, etc.).
- **Efficient Storage Management**: Helps save costs by reducing wasted space.

## Installation

### Prerequisites

- Rust (latest stable version)
- `cargo` package manager

### Build

```shell
make build
```

or

```shell
cargo build --release
```

### Build and compress the binary with UPX

```shell
make release
```

The binary will be located at `target/object-storage-maintenance`.

## Usage

Set the environment variables for S3 client:

```dotenv
S3_REGION="eu-north-1"
S3_ACCESS_KEY_ID=
S3_SECRET_ACCESS_KEY=
```

Alternatively, standard AWS environment variables can be used:

```dotenv
AWS_REGION="eu-north-1"
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

Note: `S3_REGION` (or `AWS_REGION`) defaults to `us-east-1`.

Set the object storage endpoint if you are using a non-standard S3 storage location:

```dotenv
S3_ENDPOINT_URL="https://my-storage.company.com:9000"
```

Alternatively:

```dotenv
AWS_ENDPOINT_URL_S3="https://my-storage.company.com:9000"
```

For S3-compatible storage without HTTPS, you can use:

```dotenv
S3_ALLOW_HTTP="true"
```

### Other Storage Providers

The tool also supports Google Cloud Storage (`gs://`), Azure Blob Storage (`az://`), and local files (`file://`). Use the standard environment variables for each provider as supported by the [object_store](https://docs.rs/object_store/latest/object_store/) crate.

Run the tool with the `archive` command to move and compress objects (it will automatically delete original
objects after successful archiving):

```shell
object-storage-maintenance archive \
    --src s3://project/audit/ \
    --dst s3://archive/audit/ \
    --cutoff 2025-01-01T00:00:00+00:00 \
    --buffer 104857600 \
    --compression best
```

### Command-line Arguments

| Argument        | Description                                                     | Required |
|-----------------|-----------------------------------------------------------------|----------|
| `--src`         | Source bucket and prefix containing the objects to archive.     | &#x2611; |
| `--dst`         | Destination bucket and prefix where the archive will be stored. | &#x2611; |
| `--cutoff`      | Cutoff timestamp in ISO format.                                 |          |
| `--buffer`      | Buffer size in bytes (default: 104857600 = 100MB)               |          |
| `--compression` | Compression level "fastest" or "best" (default: fastest)        |          |

### Note

- Keep in mind that AWS S3 multipart upload allows up to 10,000 parts. Since maximum total object size is 5TB - make
  sure your part (buffer) size multiplied by 10,000 fits into 5TB. Buffer size is being defaulted to 100MB since it's a
  best practice to use multipart upload for objects that are 100 MB or larger instead of uploading them in a single
  operation.
- If cutoff is not being passed - all the objects will be archived.
- Best compression level is memory hungry (up to ~1GB), but it does its job pretty well.

### Run in a container

```shell
docker run --rm --env-file .env ghcr.io/bixority/object-storage-maintenance:v0.3.0 \
    archive \
    --src s3://project/audit/ \
    --dst s3://archive/audit/ \
    --cutoff 2025-01-01T00:00:00+00:00 \
    --buffer 104857600 \
    --compression best
```

There is intentionally no `:latest` tag so there are no surprises after seamless upgrade.

## Example Use Case

Imagine you have **millions of tiny log files** stored in `s3://project/audit/`:

- Each object is **100 bytes** but takes **4KB** on disk/storage.
- You can **archive them into a single TAR archive**.
- **The archive is compressed with XZ** to save additional space.

After running the tool, the **tar.xz archive** is stored in `s3://archive/audit/`, significantly reducing storage
costs. Original objects are deleted from the source.

## License

GPL-3.0 License. See `LICENSE` for details.

## Contributing

Feel free to submit issues and pull requests!

## Author

Maintained by Olegs Korsaks / Bixority SIA.
