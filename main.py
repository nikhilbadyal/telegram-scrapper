import asyncio
import logging
from pathlib import Path
from zipfile import ZipFile
from tempfile import mkdtemp
from typing import List, Optional, Set, Callable
import os
import time
from datetime import datetime
import hashlib
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import aiofiles

from dotenv import load_dotenv
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument, InputPeerChannel, Channel
from telethon.errors import RPCError, FloodWaitError

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('telegram_scraper.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


@dataclass
class DownloadStats:
    """Statistics for download operations."""
    total_files: int = 0
    downloaded_files: int = 0
    failed_files: int = 0
    total_size: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class Config:
    """Configuration class for environment variables."""
    TELEGRAM_API_ID: int = int(os.getenv("TELEGRAM_API_ID", ""))
    TELEGRAM_API_HASH: str = os.getenv("TELEGRAM_API_HASH", "")
    TELEGRAM_SESSION_STRING: str = os.getenv("TELEGRAM_SESSION_STRING", "")
    TELEGRAM_CHANNEL: str = os.getenv("TELEGRAM_CHANNEL", "")
    R2_ENDPOINT_URL: str = os.getenv("R2_ENDPOINT_URL", "")
    R2_ACCESS_KEY_ID: str = os.getenv("R2_ACCESS_KEY_ID", "")
    R2_SECRET_ACCESS_KEY: str = os.getenv("R2_SECRET_ACCESS_KEY", "")
    R2_BUCKET_NAME: str = os.getenv("R2_BUCKET_NAME", "")
    
    # New configuration options
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY: int = int(os.getenv("RETRY_DELAY", "5"))
    MAX_CONCURRENT_DOWNLOADS: int = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "3"))
    FILE_SIZE_LIMIT_MB: int = int(os.getenv("FILE_SIZE_LIMIT_MB", "500"))
    ALLOWED_EXTENSIONS: Set[str] = set(os.getenv("ALLOWED_EXTENSIONS", "").split(",")) if os.getenv("ALLOWED_EXTENSIONS") else set()
    SKIP_EXISTING: bool = os.getenv("SKIP_EXISTING", "true").lower() == "true"

    @classmethod
    def validate(cls):
        """Validate that all required environment variables are set."""
        required_vars = [
            'TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TELEGRAM_SESSION_STRING',
            'TELEGRAM_CHANNEL', 'R2_ENDPOINT_URL', 'R2_ACCESS_KEY_ID',
            'R2_SECRET_ACCESS_KEY', 'R2_BUCKET_NAME'
        ]
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        # Validate numeric values
        if cls.MAX_RETRIES < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
        if cls.RETRY_DELAY < 0:
            raise ValueError("RETRY_DELAY must be non-negative")
        if cls.MAX_CONCURRENT_DOWNLOADS < 1:
            raise ValueError("MAX_CONCURRENT_DOWNLOADS must be positive")
        if cls.FILE_SIZE_LIMIT_MB < 0:
            raise ValueError("FILE_SIZE_LIMIT_MB must be non-negative")


async def retry_async(func: Callable, max_retries: int = 3, delay: int = 5, *args, **kwargs):
    """Retry an async function with exponential backoff."""
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"Failed after {max_retries} retries: {e}")
                raise
            
            wait_time = delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds...")
            await asyncio.sleep(wait_time)


class TelegramDownloader:
    """Handles downloading files from a Telegram channel."""

    def __init__(self):
        self.base_download_dir = Path(mkdtemp(prefix="telegram_downloads_"))
        self.stats = DownloadStats()
        self.semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_DOWNLOADS)
        logger.info(f"Using temporary download directory: {self.base_download_dir}")

    async def _get_channel_entity(self, client: TelegramClient, channel_id: int) -> Optional[Channel]:
        """Get the channel entity by searching through dialogs."""
        try:
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if isinstance(entity, Channel) and entity.id == channel_id:
                    return entity
        except RPCError as e:
            logger.error(f"Error while fetching dialogs: {e}")
        return None

    def _should_download_file(self, message) -> bool:
        """Check if a file should be downloaded based on filters."""
        if not isinstance(message.media, MessageMediaDocument):
            return False
        
        # Check file size limit
        if Config.FILE_SIZE_LIMIT_MB > 0:
            file_size_mb = message.file.size / (1024 * 1024)
            if file_size_mb > Config.FILE_SIZE_LIMIT_MB:
                logger.info(f"Skipping file {message.file.name} (size: {file_size_mb:.1f}MB exceeds limit)")
                return False
        
        # Check file extension filter
        if Config.ALLOWED_EXTENSIONS:
            file_ext = Path(message.file.name or "").suffix.lower().lstrip('.')
            if file_ext not in Config.ALLOWED_EXTENSIONS:
                logger.info(f"Skipping file {message.file.name} (extension '{file_ext}' not allowed)")
                return False
        
        # Check if file already exists
        if Config.SKIP_EXISTING:
            file_name = self._generate_filename(message)
            file_path = self.base_download_dir / file_name
            if file_path.exists():
                logger.info(f"Skipping existing file: {file_name}")
                return False
        
        return True

    async def download_channel_files(self) -> List[Path]:
        """Download all documents from a Telegram channel."""
        downloaded_files: List[Path] = []
        self.stats.start_time = datetime.now()

        try:
            async with TelegramClient(
                    StringSession(Config.TELEGRAM_SESSION_STRING),
                    Config.TELEGRAM_API_ID,
                    Config.TELEGRAM_API_HASH
            ) as client:
                logger.info(f"Connected as {await client.get_me()}")

                try:
                    channel_id = int(Config.TELEGRAM_CHANNEL)
                except ValueError:
                    raise ValueError(f"Invalid channel ID: {Config.TELEGRAM_CHANNEL}")

                target_entity = await self._get_channel_entity(client, channel_id)
                if not target_entity:
                    raise ValueError(f"Channel with ID {channel_id} not found in dialogs")

                input_peer = InputPeerChannel(
                    channel_id=target_entity.id,
                    access_hash=target_entity.access_hash
                )
                logger.info(f"Found channel: {target_entity.title} (ID: {target_entity.id})")

                # First pass: count total files to download
                messages_to_download = []
                async for message in client.iter_messages(input_peer):
                    if self._should_download_file(message):
                        messages_to_download.append(message)
                        self.stats.total_size += message.file.size

                self.stats.total_files = len(messages_to_download)
                logger.info(f"Found {self.stats.total_files} files to download "
                           f"(total size: {self.stats.total_size / (1024*1024):.1f} MB)")

                # Download files with concurrency control
                semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_DOWNLOADS)
                download_tasks = []
                
                for message in messages_to_download:
                    task = self._download_single_file(client, message, semaphore)
                    download_tasks.append(task)

                # Execute downloads with progress reporting
                for task in asyncio.as_completed(download_tasks):
                    try:
                        file_path = await task
                        if file_path:
                            downloaded_files.append(file_path)
                            self.stats.downloaded_files += 1
                        else:
                            self.stats.failed_files += 1
                    except Exception as e:
                        logger.error(f"Download task failed: {e}")
                        self.stats.failed_files += 1
                    
                    # Progress reporting
                    progress = (self.stats.downloaded_files + self.stats.failed_files) / self.stats.total_files * 100
                    logger.info(f"Overall progress: {progress:.1f}% "
                               f"({self.stats.downloaded_files} downloaded, {self.stats.failed_files} failed)")

        except Exception as e:
            logger.error(f"Telegram client error: {e}")
            raise
        finally:
            self.stats.end_time = datetime.now()

        return downloaded_files

    async def _download_single_file(self, client: TelegramClient, message, semaphore: asyncio.Semaphore) -> Optional[Path]:
        """Download a single file with retry logic and concurrency control."""
        async with semaphore:
            file_name = self._generate_filename(message)
            file_path = self.base_download_dir / file_name
            
            try:
                await retry_async(
                    self._download_file_with_validation,
                    Config.MAX_RETRIES,
                    Config.RETRY_DELAY,
                    client, message, file_path
                )
                return file_path
            except Exception as e:
                logger.error(f"Failed to download {file_name} after retries: {e}")
                return None

    async def _download_file_with_validation(self, client: TelegramClient, message, file_path: Path) -> None:
        """Download a file with validation."""
        expected_size = message.file.size
        
        def progress_callback(current, total):
            percent = (current / total) * 100
            if percent % 10 == 0 or percent > 95:  # Log every 10% or near completion
                logger.info(f"Downloading {file_path.name}: {percent:.1f}%")

        # Download the file
        await client.download_media(
            message,
            file=file_path,
            progress_callback=progress_callback
        )
        
        # Validate downloaded file
        if not file_path.exists():
            raise FileNotFoundError(f"Downloaded file not found: {file_path}")
        
        actual_size = file_path.stat().st_size
        if actual_size != expected_size:
            file_path.unlink()  # Remove corrupted file
            raise ValueError(f"File size mismatch: expected {expected_size}, got {actual_size}")
        
        logger.info(f"Successfully downloaded and validated: {file_path.name}")

    def _generate_filename(self, message) -> str:
        """Generate a unique filename for the downloaded file."""
        if message.file.name:
            base_name = message.file.name
        else:
            ext = message.file.ext or ""
            base_name = f"{message.id}_{message.file.mime_type.replace('/', '_')}{ext}"
        
        # Ensure filename is unique
        file_path = self.base_download_dir / base_name
        counter = 1
        while file_path.exists():
            name_parts = base_name.rsplit('.', 1)
            if len(name_parts) == 2:
                new_name = f"{name_parts[0]}_{counter}.{name_parts[1]}"
            else:
                new_name = f"{base_name}_{counter}"
            file_path = self.base_download_dir / new_name
            counter += 1
        
        return file_path.name

    def get_stats(self) -> DownloadStats:
        """Get download statistics."""
        return self.stats

    def cleanup(self):
        """Clean up downloaded files."""
        try:
            for file in self.base_download_dir.glob("*"):
                if file.is_file():
                    file.unlink()
                elif file.is_dir():
                    # Remove directory and its contents
                    import shutil
                    shutil.rmtree(file)
            self.base_download_dir.rmdir()
            logger.info("Cleaned up temporary files")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


class FileZipper:
    """Handles zipping files with compression and validation."""

    @staticmethod
    def zip_files(file_paths: List[Path], output_path: Path, compression_level: int = 6) -> None:
        """Zip the given files into a single archive with compression."""
        if not file_paths:
            raise ValueError("No files to zip")

        try:
            total_size = sum(f.stat().st_size for f in file_paths if f.exists())
            logger.info(f"Creating zip archive with {len(file_paths)} files "
                       f"(total size: {total_size / (1024*1024):.1f} MB)")
            
            with ZipFile(output_path, 'w', compression=compression_level) as zipf:
                for i, file_path in enumerate(file_paths, 1):
                    if file_path.exists():
                        zipf.write(file_path, arcname=file_path.name)
                        if i % 10 == 0:  # Progress reporting
                            logger.info(f"Zipped {i}/{len(file_paths)} files")
                    else:
                        logger.warning(f"File not found, skipping: {file_path}")
            
            # Validate zip file
            try:
                with ZipFile(output_path, 'r') as zipf:
                    zipf.testzip()
                logger.info(f"Created and validated zip archive at {output_path}")
            except Exception as e:
                logger.error(f"Zip validation failed: {e}")
                raise
                
        except Exception as e:
            logger.error(f"Error creating zip file: {e}")
            raise


class R2Uploader:
    """Handles uploading files to Cloudflare R2 with retry logic."""

    def __init__(self):
        self.session = boto3.session.Session()
        self.s3_client = self.session.client(
            service_name='s3',
            endpoint_url=Config.R2_ENDPOINT_URL,
            aws_access_key_id=Config.R2_ACCESS_KEY_ID,
            aws_secret_access_key=Config.R2_SECRET_ACCESS_KEY,
        )

    def upload_file(self, file_path: Path) -> str:
        """Upload a file to R2 storage with retry logic."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Generate unique key with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        key = f"{timestamp}_{file_path.name}"
        
        def _upload():
            self.s3_client.upload_file(
                Filename=str(file_path),
                Bucket=Config.R2_BUCKET_NAME,
                Key=key,
                ExtraArgs={
                    'Metadata': {
                        'upload_time': timestamp,
                        'original_name': file_path.name,
                        'file_size': str(file_path.stat().st_size)
                    }
                }
            )
            return key

        try:
            # Retry upload with exponential backoff
            for attempt in range(Config.MAX_RETRIES + 1):
                try:
                    uploaded_key = _upload()
                    logger.info(f"Successfully uploaded {file_path.name} to R2 bucket "
                               f"'{Config.R2_BUCKET_NAME}' with key '{uploaded_key}'")
                    return uploaded_key
                except (BotoCoreError, ClientError) as e:
                    if attempt == Config.MAX_RETRIES:
                        logger.error(f"R2 upload failed after {Config.MAX_RETRIES} retries: {e}")
                        raise
                    
                    wait_time = Config.RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Upload attempt {attempt + 1} failed: {e}. "
                                 f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    
        except Exception as e:
            logger.error(f"Unexpected upload error: {e}")
            raise

    def check_file_exists(self, key: str) -> bool:
        """Check if a file exists in R2 storage."""
        try:
            self.s3_client.head_object(Bucket=Config.R2_BUCKET_NAME, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise


async def main():
    """Main execution function with comprehensive error handling."""
    downloader = None
    start_time = datetime.now()
    
    try:
        logger.info("=== Telegram Scraper Starting ===")
        Config.validate()

        downloader = TelegramDownloader()
        zipper = FileZipper()
        uploader = R2Uploader()

        # Download files
        logger.info("Starting download from Telegram channel...")
        files = await downloader.download_channel_files()

        # Report download statistics
        stats = downloader.get_stats()
        duration = (stats.end_time - stats.start_time).total_seconds() if stats.end_time and stats.start_time else 0
        logger.info(f"Download completed: {stats.downloaded_files}/{stats.total_files} files "
                   f"({stats.failed_files} failed) in {duration:.1f} seconds")

        if not files:
            logger.warning("No files found to download")
            return

        # Zip files
        zip_path = downloader.base_download_dir / f"channel_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        logger.info(f"Creating zip archive at {zip_path}...")
        zipper.zip_files(files, zip_path)

        # Upload to R2
        logger.info("Uploading to Cloudflare R2...")
        uploaded_key = uploader.upload_file(zip_path)
        
        # Final statistics
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
        
        logger.info(f"✅ All operations completed successfully!")
        logger.info(f"📊 Summary:")
        logger.info(f"   - Files processed: {len(files)}")
        logger.info(f"   - Total size: {total_size_mb:.1f} MB")
        logger.info(f"   - Total time: {total_duration:.1f} seconds")
        logger.info(f"   - Upload speed: {total_size_mb/total_duration*60:.1f} MB/min")
        logger.info(f"   - R2 key: {uploaded_key}")
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        raise
    finally:
        if downloader:
            downloader.cleanup()
        logger.info("=== Telegram Scraper Finished ===")


if __name__ == "__main__":
    asyncio.run(main())
