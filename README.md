# Telegram Channel Scraper

A robust Python application that downloads all files from a Telegram channel, archives them into a ZIP file, and uploads the archive to Cloudflare R2 storage.

## ✨ Features

- **Concurrent Downloads**: Download multiple files simultaneously for faster processing
- **Smart Filtering**: Filter files by size, extension, and skip existing downloads
- **Retry Logic**: Automatic retry with exponential backoff for failed operations
- **File Validation**: Integrity checking to ensure complete downloads
- **Progress Tracking**: Real-time progress reporting with detailed statistics
- **Comprehensive Logging**: Detailed logs saved to file for debugging
- **Archive Creation**: ZIP compression with validation
- **Cloud Storage**: Direct upload to Cloudflare R2 with metadata

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Telegram API credentials
- Cloudflare R2 account and credentials

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd telegram-scrapper
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Run the scraper**
   ```bash
   python main.py
   ```

## ⚙️ Configuration

### Required Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# Telegram Configuration
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_SESSION_STRING=your_session_string_here
TELEGRAM_CHANNEL=your_channel_id_here

# Cloudflare R2 Configuration  
R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your_access_key_here
R2_SECRET_ACCESS_KEY=your_secret_key_here
R2_BUCKET_NAME=your_bucket_name_here
```

### Optional Configuration

```bash
# Download Configuration (Optional - defaults provided)
MAX_RETRIES=3                    # Number of retry attempts
RETRY_DELAY=5                    # Base delay between retries (seconds)
MAX_CONCURRENT_DOWNLOADS=3       # Concurrent download limit
FILE_SIZE_LIMIT_MB=500          # Skip files larger than this (0 = no limit)
ALLOWED_EXTENSIONS=pdf,doc,docx,txt,zip,mp4,avi,mkv  # Filter by extensions
SKIP_EXISTING=true              # Skip already downloaded files
```

### Getting Telegram Credentials

1. **API ID and Hash**:
   - Visit https://my.telegram.org/apps
   - Create a new application
   - Note down your `api_id` and `api_hash`

2. **Session String**:
   - Use Telethon's session generation tools
   - Or run a simple script to generate it:
   ```python
   from telethon.sync import TelegramClient
   from telethon.sessions import StringSession
   
   with TelegramClient(StringSession(), api_id, api_hash) as client:
       print(client.session.save())
   ```

3. **Channel ID**:
   - Forward a message from the channel to @userinfobot
   - Or use the channel's numeric ID (without the -100 prefix)

### Setting up Cloudflare R2

1. **Create R2 Bucket**:
   - Log in to Cloudflare Dashboard
   - Go to R2 Object Storage
   - Create a new bucket

2. **Generate API Token**:
   - Go to "Manage R2 API Tokens"
   - Create a new token with read/write permissions
   - Note down the Access Key ID and Secret Access Key

3. **Get Endpoint URL**:
   - Format: `https://<account-id>.r2.cloudflarestorage.com`
   - Find your account ID in the Cloudflare dashboard

## 📊 Usage Examples

### Basic Usage
```bash
python main.py
```

### With Custom Configuration
```bash
# Set environment variables and run
export MAX_CONCURRENT_DOWNLOADS=5
export FILE_SIZE_LIMIT_MB=1000
export ALLOWED_EXTENSIONS=pdf,docx,mp4
python main.py
```

### Sample Output
```
=== Telegram Scraper Starting ===
Connected as John Doe
Found channel: My Channel (ID: 1234567890)
Found 45 files to download (total size: 1,234.5 MB)
Overall progress: 23.4% (12 downloaded, 2 failed)
Creating zip archive with 43 files (total size: 1,200.3 MB)
Uploading to Cloudflare R2...
✅ All operations completed successfully!
📊 Summary:
   - Files processed: 43
   - Total size: 1,200.3 MB
   - Total time: 245.2 seconds
   - Upload speed: 294.5 MB/min
   - R2 key: 20241201_143052_channel_files.zip
=== Telegram Scraper Finished ===
```

## 🔧 Advanced Features

### File Filtering

**By Size**: Skip files larger than specified limit
```bash
FILE_SIZE_LIMIT_MB=100  # Skip files > 100MB
```

**By Extension**: Only download specific file types
```bash
ALLOWED_EXTENSIONS=pdf,doc,docx,txt  # Only documents
ALLOWED_EXTENSIONS=mp4,avi,mkv,mov   # Only videos
```

**Skip Existing**: Avoid re-downloading files
```bash
SKIP_EXISTING=true  # Skip if file already exists locally
```

### Concurrency Control

Adjust concurrent downloads based on your system:
```bash
MAX_CONCURRENT_DOWNLOADS=1   # Conservative (slow but safe)
MAX_CONCURRENT_DOWNLOADS=3   # Balanced (default)
MAX_CONCURRENT_DOWNLOADS=10  # Aggressive (fast but resource-intensive)
```

### Retry Configuration

Configure retry behavior:
```bash
MAX_RETRIES=5      # More retries for unstable connections
RETRY_DELAY=10     # Longer delays between retries
```

## 📝 Logging

The application creates detailed logs in:
- **Console**: Real-time progress and status
- **File**: `telegram_scraper.log` with detailed debugging information

Log levels include:
- `INFO`: General progress and status
- `WARNING`: Non-critical issues (e.g., skipped files)
- `ERROR`: Critical errors and failures

## 🛠️ Troubleshooting

### Common Issues

**1. Authentication Errors**
```
Error: Could not connect to Telegram
```
- Verify your `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and `TELEGRAM_SESSION_STRING`
- Ensure the session string is valid and not expired

**2. Channel Access Issues**
```
Error: Channel with ID 1234567890 not found
```
- Verify the channel ID is correct
- Ensure your account has access to the channel
- Try using the channel username instead of ID

**3. R2 Upload Failures**
```
Error: R2 upload error: NoSuchBucket
```
- Verify your R2 credentials and bucket name
- Check the endpoint URL format
- Ensure the bucket exists and you have write permissions

**4. Memory Issues**
```
Error: MemoryError during download
```
- Reduce `MAX_CONCURRENT_DOWNLOADS`
- Set `FILE_SIZE_LIMIT_MB` to skip large files
- Ensure sufficient disk space

**5. Network Timeouts**
```
Error: FloodWaitError: Must wait X seconds
```
- The application automatically handles this
- Consider increasing `RETRY_DELAY` for unstable connections

### Performance Tips

1. **Optimize Concurrency**: Start with 3 concurrent downloads and adjust based on performance
2. **Filter Unnecessary Files**: Use extension and size filters to avoid downloading unwanted content
3. **Monitor Resources**: Watch disk space and memory usage during large downloads
4. **Network Stability**: Use retry configuration for unstable connections

## 📁 Project Structure

```
telegram-scrapper/
├── main.py              # Main application code
├── requirements.txt     # Python dependencies
├── .env                 # Environment variables (create from .env.example)
├── .env.example         # Environment variables template
├── .gitignore          # Git ignore rules
├── README.md           # This file
└── telegram_scraper.log # Generated log file
```

## 🔒 Security Notes

- **Never commit `.env` files** to version control
- **Rotate credentials regularly**, especially session strings
- **Use restricted R2 tokens** with minimal required permissions
- **Monitor R2 usage** to prevent unexpected charges

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This tool is for educational and personal use only. Ensure you have proper permissions to download content from Telegram channels. Respect copyright laws and Telegram's Terms of Service. The authors are not responsible for misuse of this software.

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Review the log file (`telegram_scraper.log`) for detailed error information
3. Open an issue on GitHub with:
   - Error message
   - Configuration (without sensitive data)
   - Log file excerpt
   - Steps to reproduce

---

**Happy Scraping! 🎉** 
