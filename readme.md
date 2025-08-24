# KPTV Stream Synchronization Tool

KPTV is a Python-based stream synchronization tool that fetches IPTV streams from multiple providers and synchronizes them to a database with advanced filtering capabilities.

## Features

- **Multi-Provider Support**: Sync streams from multiple IPTV providers
- **Multiple Stream Types**: Support for live TV, series, and VOD content
- **Flexible Input Formats**: Compatible with both API endpoints and M3U playlists
- **Advanced Filtering**: Include/exclude streams using regex patterns and text matching
- **Multi-threaded Processing**: Efficient concurrent processing of multiple providers
- **Database Integration**: Complete MySQL database integration with stored procedures
- **Caching System**: Intelligent caching to improve performance
- **Debug Logging**: Comprehensive debug output for troubleshooting
- **Automatic Cleanup**: Built-in stream cleanup and fixup operations

## Requirements

- Python 3.10 or higher
- MySQL database server
- Required Python packages (see `requirements.txt`)

## Installation

### From Source

1. Clone or download the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Compiled Binary

Use the provided `compile.sh` script to create a standalone executable:

```bash
chmod +x compile.sh
sudo ./compile.sh
```

This will create a `kptv` binary in the `release/` directory and install it to `/usr/local/bin/`.

## Configuration

### Database Setup

Create a `.kptvconf` configuration file in your project directory or home directory:

```json
{
    "dbserver": "localhost",
    "dbport": 3306,
    "dbuser": "your_username",
    "dbpassword": "your_password",
    "dbschema": "your_database",
    "db_tblprefix": "your_table_prefix_"
}
```

### Required Database Tables

The application expects the following database structure:
- `stream_providers` - Provider configuration
- `stream_filters` - User-defined filters
- `stream_temp` - Temporary sync table
- Various stored procedures for cleanup and sync operations

## Usage

### Command Line Interface

```bash
./main.py -a <action> [options]
```

or with the compiled binary:

```bash
kptv -a <action> [options]
```

### Available Actions

#### Sync Streams
```bash
# Sync all streams from all providers
./main.py -a sync

# Sync only live streams
./main.py -a sync --live

# Sync only series
./main.py -a sync --series

# Sync only VOD content
./main.py -a sync --vod

# Sync from specific provider
./main.py -a sync --provider 123

# Enable debug output
./main.py -a sync --debug
```

#### Fixup Operations
```bash
# Run fixup operations to match channel numbers, logos, and TVG IDs
./main.py -a fixup
```

### Help
```bash
./main.py -h
```

## Architecture

### Core Components

- **`main.py`** - Application entry point and action dispatcher
- **`common/common.py`** - Argument parsing and common utilities
- **`config/config.py`** - Configuration management with automatic file discovery
- **`db/db.py`** - Database abstraction layer with connection pooling
- **`sync/sync.py`** - Main synchronization orchestrator with threading
- **`sync/get.py`** - Stream fetching from providers (API and M3U support)
- **`sync/filter.py`** - Stream filtering engine
- **`sync/data.py`** - Data management and database operations
- **`utils/`** - Utility modules (caching, HTTP requests, debugging)

### Provider Types

The application supports two types of providers:

1. **API Providers** (`sp_type = 0`): Use JSON API endpoints
   - Live streams: `/player_api.php?action=get_live_streams`
   - Series: `/player_api.php?action=get_series`
   - VOD: `/player_api.php?action=get_vod_streams`

2. **M3U Providers** (`sp_type = 1`): Use M3U playlist files
   - Direct M3U URL parsing with EXTINF metadata

### Filtering System

Streams can be filtered using various methods:

- **Include Filters** (`sf_type_id = 0`): Whitelist streams matching patterns
- **Contains Filters** (`sf_type_id = 1`): Exclude streams containing text
- **Regex Name Filters** (`sf_type_id = 2`): Exclude by regex on stream name
- **Regex URL Filters** (`sf_type_id = 3`): Exclude by regex on stream URL

### Threading and Performance

- Configurable thread pool for concurrent provider processing
- Intelligent request rate limiting to avoid overwhelming providers
- Connection pooling for database operations
- Chunked processing for large datasets
- Comprehensive error handling and recovery

## Database Schema

### Key Tables

- **`stream_providers`**: Provider configurations
- **`stream_filters`**: User filtering rules
- **`stream_temp`**: Temporary staging table
- **Main stream tables**: Final synchronized data

### Stored Procedures

- `Streams_All_Sync`: Sync from temp to main tables
- `Streams_CleanUp`: Remove outdated streams
- `Streams_FixUp`: Match logos, channel numbers, TVG IDs
- `Provider_Update_Refreshed`: Update sync timestamps

## Debugging

Enable debug output with the `--debug` flag:

```bash
./main.py -a sync --debug
```

Debug output includes:
- Configuration file discovery
- Database operations
- Sync progress and statistics
- Request details
- Filter operations

## Error Handling

The application includes comprehensive error handling:

- Provider connection failures are logged but don't stop other providers
- Database transaction rollback on errors
- Automatic retry logic for network requests
- Graceful degradation for missing data
- Detailed error reporting in sync summaries

## Performance Tuning

### Threading
- Default: 4-8 threads based on CPU cores
- Configurable via constructor parameters

### Database
- Connection pooling with configurable pool size
- Batch insert operations for large datasets
- Chunked processing to manage memory usage

### Caching
- Provider and filter data caching
- Configurable TTL and cache size limits
- Thread-safe operations

## Example Output

```
******************************************************************************
STARTING PROVIDER SYNC
Providers to process:
- Provider 1
- Provider 2
******************************************************************************

******************************************************************************
SYNC SUMMARY
******************************************************************************
SUCCESSFUL PROVIDERS:
- Provider 1: 5000/4500 streams
- Provider 2: 3000/2800 streams

STATISTICS:
Total providers: 2
Successful: 2
Failed: 0
Total time: 45.2 seconds

SYNC COMPLETED SUCCESSFULLY
******************************************************************************
```

## Dependencies

See `requirements.txt` for complete list:

- **PyMySQL**: MySQL database connectivity
- **requests**: HTTP client with retry logic
- **regex**: Enhanced regex support for filtering
- **m3u-parser**: M3U playlist parsing
- **pyinstaller**: Binary compilation (build-time)

## License

This project appears to be a custom IPTV management solution. Please ensure compliance with your local laws and provider terms of service when using this software.

## Contributing

When contributing to this project:

1. Maintain Python 3.10+ compatibility
2. Follow the existing code structure and patterns
3. Add appropriate debug logging for new features
4. Update this README for significant changes
5. Test with both API and M3U provider types

## Troubleshooting

### Common Issues

1. **Config file not found**: Ensure `.kptvconf` is in the current directory, source directory, or home directory
2. **Database connection errors**: Verify database credentials and server accessibility
3. **Provider sync failures**: Check provider URLs and credentials, enable debug output
4. **Memory issues with large providers**: Adjust chunk sizes and thread counts
5. **Import errors**: Ensure all requirements are installed

### Debug Steps

1. Enable debug output with `--debug`
2. Check configuration file location and format
3. Verify database connectivity and table structure
4. Test individual providers with `--provider` flag
5. Review sync summary for specific error messages