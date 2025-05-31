# BunnyHop

<div align="center">
  <img src="icon.png" alt="BunnyHop Icon" width="128" height="128">
</div>

A Python tool for syncing files from a local directory to Bunny.net CDN storage with intelligent caching and progress tracking.

**Author**: [ofirgalcon](https://github.com/ofirgalcon)

## Features

- **Intelligent Sync**: Only uploads changed files using MD5 checksums
- **Progress Tracking**: Real-time upload progress with speed indicators and file analysis progress
- **Performance Optimized**: Fast checksums and parallel processing for large file collections
- **Smart Caching**: Metadata-based cache to avoid unnecessary checksum calculations
- **Cleanup**: Automatically removes deleted files and empty directories
- **Safe Operations**: Confirmation prompts before making changes
- **Flexible Configuration**: JSON config file with command-line overrides

## Configuration

### Setup

1. Copy the example configuration file:
   ```bash
   cp config.json.example config.json
   ```

2. Edit `config.json` with your settings:

   **Note**: The script automatically looks for `config.json` in the same directory as the script file.
   ```json
   {
       "src_dir": "/path/to/your/source/directory",
       "bunny_storage_url": "https://region.storage.bunnycdn.com/your-storage-zone",
       "bunny_api_key": "your-bunny-api-key-here",
       "cache_dir": "~/.bunny_cache",
       "excluded_files": [".DS_Store", "._.DS_Store", "Thumbs.db", ".AppleDouble"],
       "fast_checksum": true,
       "parallel_analysis": true
   }
   ```

### Configuration Options

- **src_dir**: Local directory to sync from
- **bunny_storage_url**: Your Bunny.net storage zone URL (supports subdirectories - see examples below)
- **bunny_api_key**: Your Bunny.net API access key
- **cache_dir**: Directory for storing file checksums and metadata (supports `~` for home directory)
- **excluded_files**: List of file patterns to exclude from sync
- **fast_checksum**: Use fast checksums for large files (>10MB) - samples file chunks instead of full file
- **parallel_analysis**: Enable parallel processing for file analysis (recommended for large file collections)

### Subdirectory Support

The script fully supports uploading to subdirectories within your Bunny storage zone. You can specify the target subdirectory directly in the `bunny_storage_url`:

**Examples:**

```json
{
    "bunny_storage_url": "https://region.storage.bunnycdn.com/your-storage-zone"
}
```

```json
{
    "bunny_storage_url": "https://region.storage.bunnycdn.com/your-storage-zone/website"
}
```

```json
{
    "bunny_storage_url": "https://region.storage.bunnycdn.com/your-storage-zone/projects/2024/website"
}
```

**How it works:**
- The script preserves your local directory structure within the specified destination
- Local file: `/local/files/docs/readme.txt` → Remote: `your-zone/website/docs/readme.txt`
- Supports nested subdirectories of any depth
- Automatically creates directories as needed
- Handles directory cleanup when files are deleted

**Command line override:**
```bash
python bunnyhop.py --storage-url "https://region.storage.bunnycdn.com/your-zone/subfolder"
```

### Security

The `config.json` file is automatically excluded from version control and Cursor indexing to protect your sensitive API keys.

## Usage

### Basic Usage
```bash
python bunnyhop.py
```

### Command Line Options
```bash
# Use a different config file
python bunnyhop.py --config /path/to/config.json

# Override config file settings
python bunnyhop.py --src-dir /different/path --api-key new-key

# Show help
python bunnyhop.py --help
```

## Performance Optimizations

For large file collections (1000+ files, especially >50MB each), the script includes several optimizations:

### Fast Checksums
- **Enabled by default** via `"fast_checksum": true`
- For files >10MB: Uses file size + modification time + sample chunks instead of full file hash
- **Offers significant speed benefits for large files (approx. 10-50x faster processing).**
- **Trade-off**: Slightly less precise change detection (but still very reliable)

### Parallel Processing
- **Enabled by default** via `"parallel_analysis": true`
- Uses up to 4 threads for concurrent file analysis
- **Leverages multi-core systems for accelerated analysis (approx. 2-4x faster).**
- Automatically disabled for small file collections (<10 files)

### Smart Metadata Caching
- Stores file size and modification time alongside checksums
- Uses cached checksums for faster decision-making when file metadata hasn't changed
- **Provides rapid analysis for unchanged files using cached metadata (cache is updated with the relevant checksum).**

### Expected Performance
- **Without optimizations**: 1000 files @ 50MB each = ~6 minutes
- **With optimizations**: Same workload = ~30-90 seconds (first run), ~5-15 seconds (subsequent runs)

### Progress Display Features
- **Total Script Time Remaining**: Shows estimated time for entire sync operation
- **Intelligent Estimation**: Uses both file count and bytes transferred for accuracy
- **Real-time Updates**: Updates every 0.5 seconds during large file uploads
- **Missing File Handling**: Gracefully handles files that disappear during sync

To disable optimizations if needed:
```json
{
    "fast_checksum": false,
    "parallel_analysis": false
}
```

## Requirements

- Python 3.7+
- `requests` library
- `urllib3` library

Install dependencies:
```bash
pip install -r requirements.txt
```

## Version Management

This project uses semantic versioning. Current version: 1.0.0

To update versions:
- Bug fixes: "bump patch version"
- New features: "bump minor version"
- Breaking changes: "bump major version" 