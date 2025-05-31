#!/usr/bin/env python3
"""
BunnyHop - Bunny.net storage zone sync tool
Syncs files from a local directory to Bunny.net CDN storage
"""

__version__ = "1.0.0"

import os
import sys
import json
import hashlib
import time
import urllib.parse
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
import concurrent.futures
import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import argparse
from dataclasses import dataclass
from datetime import datetime

# Configuration
@dataclass
class Config:
    src_dir: str = ""
    bunny_storage_url: str = ""
    bunny_api_key: str = ""
    cache_dir: str = ""
    excluded_files: List[str] = None
    fast_checksum: bool = True  # Use fast checksums for large files
    parallel_analysis: bool = True  # Use parallel processing for analysis
    
    @classmethod
    def load_from_file(cls, config_file: str = "config.json") -> 'Config':
        """Load configuration from JSON file."""
        try:
            # If config_file is just a filename (no path), search in multiple locations
            if not os.path.dirname(config_file):
                search_paths = [
                    # 1. Current working directory
                    os.path.join(os.getcwd(), config_file),
                    # 2. Original script directory (resolving symlinks)
                    os.path.join(os.path.dirname(os.path.realpath(__file__)), config_file),
                    # 3. Symlink directory (if different from original)
                    os.path.join(os.path.dirname(os.path.abspath(__file__)), config_file),
                    # 4. User's home directory
                    os.path.expanduser(f"~/{config_file}")
                ]
                
                # Remove duplicates while preserving order
                unique_paths = []
                for path in search_paths:
                    if path not in unique_paths:
                        unique_paths.append(path)
                
                # Try each path until we find the config file
                for config_path in unique_paths:
                    if os.path.exists(config_path):
                        config_file = config_path
                        break
                else:
                    # If no config found, show helpful error with search locations
                    locations = '\n  '.join(unique_paths)
                    raise FileNotFoundError(
                        f"Configuration file '{config_file}' not found.\n"
                        f"Searched in the following locations:\n  {locations}\n"
                        f"Please create the config file in one of these locations or specify the full path."
                    )
            
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            # Expand user path for cache_dir
            if 'cache_dir' in config_data:
                config_data['cache_dir'] = os.path.expanduser(config_data['cache_dir'])
            
            return cls(**config_data)
        except FileNotFoundError:
            raise
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file '{config_file}': {e}")
        except Exception as e:
            raise Exception(f"Error loading configuration: {e}")
    
    def __post_init__(self):
        if self.excluded_files is None:
            self.excluded_files = [".DS_Store", "._.DS_Store", "Thumbs.db", ".AppleDouble"]

# Colors for terminal output
class Colors:
    GREEN = '\033[0;32m'
    YELLOW = '\033[0;33m'
    RED = '\033[0;31m'
    BLUE = '\033[0;34m'
    BOLD = '\033[1m'
    NC = '\033[0m'  # No Color

# Statistics tracking
@dataclass
class SyncStats:
    files_uploaded: int = 0
    files_skipped: int = 0
    files_deleted: int = 0
    files_missing: int = 0
    directories_deleted: int = 0
    total_bytes_uploaded: int = 0
    total_upload_time: float = 0.0
    overall_script_start_time: float = 0.0 # Renamed from script_start_time
    first_byte_upload_time: Optional[float] = None # Time when first actual upload activity began
    sync_operations_start_time: Optional[float] = None # Time when sync operations start after confirmation
    
    def __post_init__(self):
        self.overall_script_start_time = time.time()

class BunnyStorageSync:
    def __init__(self, config: Config):
        self.config = config
        self.stats = SyncStats()
        self.session = self._create_session()
        
        # Ensure cache directory exists
        Path(self.config.cache_dir).mkdir(parents=True, exist_ok=True)
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy and proper headers."""
        session = requests.Session()
        
        # Set up retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'AccessKey': self.config.bunny_api_key,
            'User-Agent': f'BunnyHop/{__version__}'
        })
        
        return session
    
    def print_msg(self, message: str, color: str = Colors.NC):
        """Print formatted message with timestamp."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {color}{message}{Colors.NC}")
    
    def clear_line(self):
        """Clear the current line properly, handling line wrapping."""
        # Move cursor to beginning of line and clear entire line
        print('\r\033[K', end='', flush=True)
    
    def clear_progress_lines(self, text_length: int):
        """Clear multiple lines that may have been created by text wrapping."""
        try:
            import shutil
            terminal_width = shutil.get_terminal_size().columns
            # Calculate how many lines the text would span
            lines_used = (text_length + terminal_width - 1) // terminal_width
            
            # Clear current line and move up to clear wrapped lines
            print('\r\033[K', end='', flush=True)
            for _ in range(lines_used - 1):
                print('\033[1A\033[K', end='', flush=True)
        except:
            # Fallback to simple line clear if terminal size detection fails
            print('\r\033[K', end='', flush=True)
    
    def truncate_path(self, path: str, max_length: int = 50) -> str:
        """Truncate a file path for display if it's too long."""
        if len(path) <= max_length:
            return path
        # Show beginning and end of path with ... in middle
        if max_length < 10:
            return path[:max_length]
        start_len = (max_length - 3) // 2
        end_len = max_length - 3 - start_len
        return f"{path[:start_len]}...{path[-end_len:]}"
    
    def should_exclude(self, filename: str) -> bool:
        """Check if a file should be excluded from sync."""
        basename = os.path.basename(filename)
        return basename in self.config.excluded_files
    
    def format_size(self, bytes_size: int) -> str:
        """Convert bytes to human-readable format using decimal (base-10) units."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_size < 1000:
                if unit == 'B':
                    return f"{bytes_size}{unit}"
                return f"{bytes_size:.1f}{unit}"
            bytes_size /= 1000
        return f"{bytes_size:.2f}TB"
    
    def calc_speed(self, bytes_size: int, duration: float) -> str:
        """Calculate upload speed using decimal (base-10) units."""
        if duration <= 0:
            return "0.00 MB/s"
        speed = bytes_size / duration / 1000 / 1000
        return f"{speed:.2f} MB/s"
    
    def format_time_remaining(self, bytes_remaining: int, current_speed_bps: float) -> str:
        """Calculate and format estimated time remaining."""
        if current_speed_bps <= 0:
            return "calculating..."
        
        seconds_remaining = bytes_remaining / current_speed_bps
        
        if seconds_remaining < 60:
            return f"{seconds_remaining:.0f}s"
        elif seconds_remaining < 3600:
            minutes = int(seconds_remaining // 60)
            seconds = int(seconds_remaining % 60)
            return f"{minutes:02d}m:{seconds:02d}s"
        else:
            hours = int(seconds_remaining // 3600)
            minutes = int((seconds_remaining % 3600) // 60)
            return f"{hours}h:{minutes:02d}m"
    
    def calculate_total_time_remaining(self, files_completed: int, total_files: int, bytes_completed_script: int, total_bytes_script: int) -> str:
        """Calculate total script time remaining based on overall progress."""
        if total_bytes_script <= 0 or bytes_completed_script < 0: # Allow bytes_completed_script to be 0
            return "calculating..."
        
        if bytes_completed_script == 0: # If no bytes uploaded yet (neither previous nor current chunk)
            return "calculating..." # Cannot estimate speed

        current_timestamp = time.time()
        effective_elapsed_time: float

        if self.stats.first_byte_upload_time is not None:
            effective_elapsed_time = current_timestamp - self.stats.first_byte_upload_time
        else:
            # This state (bytes_completed_script > 0 but first_byte_upload_time is None)
            # should ideally not be reached if first_byte_upload_time is set correctly.
            # This indicates a logic error in the calling sequence or state management.
            return "calculating..." # Safest return if state is inconsistent

        # Ensure effective_elapsed_time is positive.
        if effective_elapsed_time <= 0.001: # Needs a small positive duration for meaningful speed.
            return "calculating..." 

        current_speed_bps = bytes_completed_script / effective_elapsed_time
        
        if current_speed_bps <= 0: # Speed must be positive
            return "calculating..."
        
        bytes_remaining = total_bytes_script - bytes_completed_script
        if bytes_remaining < 0: 
            bytes_remaining = 0 

        remaining_time_seconds = bytes_remaining / current_speed_bps
        if remaining_time_seconds < 0: 
            remaining_time_seconds = 0
        
        if remaining_time_seconds < 60:
            return f"{remaining_time_seconds:.0f}s"
        elif remaining_time_seconds < 3600:
            minutes = int(remaining_time_seconds // 60)
            seconds = int(remaining_time_seconds % 60)
            return f"{minutes:02d}m:{seconds:02d}s"
        else:
            hours = int(remaining_time_seconds // 3600)
            minutes = int((remaining_time_seconds % 3600) // 60)
            return f"{hours}h:{minutes:02d}m"
    
    def get_file_checksum(self, filepath: str) -> Optional[str]:
        """Calculate MD5 checksum of a file with optimized chunk size."""
        try:
            hash_md5 = hashlib.md5()
            # Use larger chunk size for better performance on large files
            chunk_size = 1024 * 1024  # 1MB chunks
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except (FileNotFoundError, OSError, PermissionError) as e:
            self.print_msg(f"Warning: Cannot read file for checksum '{filepath}': {e}", Colors.YELLOW)
            return None
    
    def get_file_checksum_fast(self, filepath: str) -> Optional[str]:
        """Calculate fast checksum using file size + mtime + first/last chunks."""
        try:
            stat = os.stat(filepath)
            file_size = stat.st_size
            mtime = int(stat.st_mtime)
            
            # For small files, use full checksum
            if file_size < 10 * 1024 * 1024:  # 10MB
                return self.get_file_checksum(filepath)
            
            # For large files, use size + mtime + sample chunks
            hash_md5 = hashlib.md5()
            hash_md5.update(f"{file_size}:{mtime}".encode())
            
            chunk_size = 64 * 1024  # 64KB chunks
            with open(filepath, "rb") as f:
                # Read first chunk
                first_chunk = f.read(chunk_size)
                hash_md5.update(first_chunk)
                
                # Read middle chunk
                if file_size > chunk_size * 2:
                    f.seek(file_size // 2)
                    middle_chunk = f.read(chunk_size)
                    hash_md5.update(middle_chunk)
                
                # Read last chunk
                if file_size > chunk_size:
                    f.seek(-chunk_size, 2)
                    last_chunk = f.read(chunk_size)
                    hash_md5.update(last_chunk)
            
            return hash_md5.hexdigest()
        except (FileNotFoundError, OSError, PermissionError) as e:
            self.print_msg(f"Warning: Cannot read file for fast checksum '{filepath}': {e}", Colors.YELLOW)
            # Try fallback to full checksum
            return self.get_file_checksum(filepath)
        except Exception as e:
            self.print_msg(f"Warning: Unexpected error calculating checksum for '{filepath}': {e}", Colors.YELLOW)
            return None
    
    def create_cache_filename(self, relative_path: str) -> str:
        """Create a safe filename for cache storage."""
        # Replace path separators and special characters
        safe_name = relative_path.replace('/', '_').replace('\\', '_')
        # Keep only alphanumeric, period, underscore, and dash
        result = ''.join(c if c.isalnum() or c in '._-' else '_' for c in safe_name)
        return result
    
    def get_cached_checksum(self, relative_path: str) -> Optional[str]:
        """Get cached checksum for a file."""
        cache_filename = self.create_cache_filename(relative_path)
        cache_file = Path(self.config.cache_dir) / f"{cache_filename}.md5"
        
        try:
            if cache_file.exists():
                return cache_file.read_text().strip()
        except Exception:
            pass
        return None
    
    def get_cached_metadata(self, relative_path: str) -> Optional[Dict]:
        """Get cached file metadata (size, mtime)."""
        cache_filename = self.create_cache_filename(relative_path)
        cache_file = Path(self.config.cache_dir) / f"{cache_filename}.meta"
        
        try:
            if cache_file.exists():
                return json.loads(cache_file.read_text())
        except Exception:
            pass
        return None
    
    def save_metadata_to_cache(self, relative_path: str, file_path: str, checksum: str):
        """Save file metadata and checksum to cache."""
        try:
            stat = os.stat(file_path)
            metadata = {
                'size': stat.st_size,
                'mtime': stat.st_mtime,
                'checksum': checksum
            }
            
            cache_filename = self.create_cache_filename(relative_path)
            cache_file = Path(self.config.cache_dir) / f"{cache_filename}.meta"
            cache_file.write_text(json.dumps(metadata))
            
            # Also save the checksum file for backward compatibility
            self.save_checksum_to_cache(relative_path, checksum)
        except Exception as e:
            self.print_msg(f"Warning: Could not save metadata to cache: {e}", Colors.YELLOW)
    
    def file_needs_checksum(self, relative_path: str, file_path: str) -> bool:
        """Check if file needs checksum calculation based on metadata."""
        try:
            cached_meta = self.get_cached_metadata(relative_path)
            if not cached_meta:
                return True
            
            current_stat = os.stat(file_path)
            
            # Check if size or mtime changed
            if (cached_meta.get('size') != current_stat.st_size or 
                abs(cached_meta.get('mtime', 0) - current_stat.st_mtime) > 1):
                return True
            
            return False
        except Exception:
            return True
    
    def save_checksum_to_cache(self, relative_path: str, checksum: str):
        """Save checksum to cache."""
        cache_filename = self.create_cache_filename(relative_path)
        cache_file = Path(self.config.cache_dir) / f"{cache_filename}.md5"
        
        try:
            cache_file.write_text(checksum)
        except Exception as e:
            self.print_msg(f"Warning: Could not save checksum to cache: {e}", Colors.YELLOW)
    
    def remove_from_cache(self, relative_path: str):
        """Remove checksum from cache."""
        cache_filename = self.create_cache_filename(relative_path)
        cache_file = Path(self.config.cache_dir) / f"{cache_filename}.md5"
        
        try:
            if cache_file.exists():
                cache_file.unlink()
        except Exception:
            pass
    
    def get_remote_files(self, current_path: str = "") -> Tuple[Set[str], Set[str]]:
        """Get all remote files and directories recursively."""
        remote_files = set()
        remote_directories = set()
        total_remote_size = 0
        
        print("  Fetching remote file list...", end='', flush=True)
        
        try:
            total_remote_size = self._get_remote_files_recursive(current_path, remote_files, remote_directories, total_remote_size)
            print(f"\r\033[K  ✓ Found {len(remote_files)} remote files in {len(remote_directories)} directories ({self.format_size(total_remote_size)})")
        except Exception as e:
            print(f"\r\033[K  ✗ Error getting remote files: {e}")
            self.print_msg(f"Error getting remote files: {e}", Colors.RED)
        
        return remote_files, remote_directories
    
    def _get_remote_files_recursive(self, current_path: str, remote_files: Set[str], remote_directories: Set[str], total_size: int = 0) -> int:
        """Recursively get remote files and directories from a directory."""
        # Construct API URL
        if current_path:
            if not current_path.endswith('/'):
                current_path += '/'
            url = f"{self.config.bunny_storage_url}/{urllib.parse.quote(current_path, safe='/')}"
        else:
            url = f"{self.config.bunny_storage_url}/"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            items = response.json()
            if not isinstance(items, list):
                return total_size
            
            for item in items:
                object_name = item.get('ObjectName', '')
                is_directory = item.get('IsDirectory', False)
                
                if not object_name:
                    continue
                
                if is_directory:
                    # Add directory to the set
                    dir_path = f"{current_path}{object_name}"
                    remote_directories.add(dir_path)
                    
                    # Recursively get files from subdirectory
                    total_size = self._get_remote_files_recursive(dir_path, remote_files, remote_directories, total_size)
                else:
                    # Add file to the set
                    full_path = f"{current_path}{object_name}"
                    remote_files.add(full_path)
                    
                    # Add file size to total (if available)
                    file_size = item.get('Length', 0)
                    if isinstance(file_size, (int, float)) and file_size > 0:
                        total_size += int(file_size)
                    
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 404:
                    # Directory doesn't exist, which is fine
                    return total_size
            self.print_msg(f"Error fetching directory '{current_path}': {e}", Colors.RED)
        
        return total_size
    
    def remote_file_exists(self, relative_path: str) -> bool:
        """Check if file exists on remote server."""
        encoded_path = urllib.parse.quote(relative_path, safe='/')
        url = f"{self.config.bunny_storage_url}/{encoded_path}"
        
        try:
            response = self.session.head(url, timeout=10)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def upload_file(self, local_path: str, relative_path: str, file_index: int = 0, total_files: int = 0, total_upload_size: int = 0) -> bool:
        """Upload a single file to Bunny Storage with total script progress tracking."""
        try:
            file_size = os.path.getsize(local_path)
            
            # URL encode the remote path
            encoded_path = urllib.parse.quote(relative_path, safe='/')
            url = f"{self.config.bunny_storage_url}/{encoded_path}"
            
            upload_start = time.time() # Marks the start of attempting to upload *this* file

            # Set first_byte_upload_time if this is the very first upload activity of the script
            # and the file has content.
            if self.stats.first_byte_upload_time is None and file_size > 0:
                if self.stats.files_uploaded == 0: # No prior files have *completed* uploading.
                    self.stats.first_byte_upload_time = upload_start
            
            # Measure upload time
            last_progress_length = 0
            
            # For small files (< 1MB), don't show progress percentage
            if file_size < 1024 * 1024:
                progress_msg = f"  Uploading: {relative_path} ({self.format_size(file_size)})"
                print(progress_msg, end='', flush=True)
                last_progress_length = len(progress_msg)
                
                with open(local_path, 'rb') as f:
                    response = self.session.put(
                        url,
                        data=f,
                        headers={'Content-Type': 'application/octet-stream'},
                        timeout=300
                    )
            else:
                # For larger files, show progress
                uploaded_bytes = 0
                last_progress_time = upload_start
                
                initial_msg = f"  Uploading: {relative_path} ({self.format_size(file_size)}) - 0.0%"
                print(initial_msg, end='', flush=True)
                last_progress_length = len(initial_msg)
                
                def file_generator():
                    nonlocal uploaded_bytes, last_progress_time, last_progress_length
                    with open(local_path, 'rb') as f:
                        while True:
                            chunk = f.read(262144)
                            if not chunk:
                                break
                            uploaded_bytes += len(chunk)
                            current_time = time.time()
                            
                            # Update progress every 0.5 seconds
                            if current_time - last_progress_time >= 0.5:
                                progress_percent = (uploaded_bytes / file_size) * 100
                                elapsed = current_time - upload_start
                                if elapsed > 0:
                                    speed_mbps = uploaded_bytes / elapsed / 1000 / 1000  # MB/s
                                    
                                    # Calculate total script time remaining
                                    if total_files > 0 and total_upload_size > 0:
                                        files_completed = file_index
                                        bytes_completed = self.stats.total_bytes_uploaded + uploaded_bytes
                                        total_time_remaining = self.calculate_total_time_remaining(files_completed, total_files, bytes_completed, total_upload_size)
                                        display_path = self.truncate_path(relative_path, 40)
                                        progress_msg = f"  Uploading: {display_path} ({self.format_size(file_size)}) - {progress_percent:.1f}% ({speed_mbps:.2f} MB/s, {total_time_remaining} total remaining)"
                                    else:
                                        # Fallback to individual file time remaining
                                        speed_bps = uploaded_bytes / elapsed  # bytes/s
                                        bytes_remaining = file_size - uploaded_bytes
                                        time_remaining = self.format_time_remaining(bytes_remaining, speed_bps)
                                        display_path = self.truncate_path(relative_path, 40)
                                        progress_msg = f"  Uploading: {display_path} ({self.format_size(file_size)}) - {progress_percent:.1f}% ({speed_mbps:.2f} MB/s, {time_remaining} remaining)"
                                    
                                    # Clear previous progress lines and print new one
                                    self.clear_progress_lines(last_progress_length)
                                    print(progress_msg, end='', flush=True)
                                    last_progress_length = len(progress_msg)
                                    
                                last_progress_time = current_time
                            
                            yield chunk
                
                response = self.session.put(
                    url,
                    data=file_generator(),
                    headers={'Content-Type': 'application/octet-stream'},
                    timeout=300
                )
            
            upload_end = time.time()
            upload_duration = upload_end - upload_start
            
            if response.status_code in (200, 201):
                # Update statistics
                self.stats.files_uploaded += 1
                self.stats.total_bytes_uploaded += file_size
                self.stats.total_upload_time += upload_duration
                
                upload_speed = self.calc_speed(file_size, upload_duration)
                # Clear the progress lines completely and print success message
                self.clear_progress_lines(last_progress_length)
                print(f"  {Colors.GREEN}✓ Uploaded: {relative_path} ({self.format_size(file_size)}) - {upload_speed}{Colors.NC}")
                
                return True
            else:
                self.clear_progress_lines(last_progress_length)
                print(f"  ✗ Upload failed: {relative_path} (HTTP {response.status_code})")
                return False
                
        except Exception as e:
            self.clear_progress_lines(last_progress_length)
            print(f"  ✗ Upload failed: {relative_path} - {e}")
            return False
    
    def delete_remote_file(self, relative_path: str) -> bool:
        """Delete a file from remote storage."""
        try:
            encoded_path = urllib.parse.quote(relative_path, safe='/')
            url = f"{self.config.bunny_storage_url}/{encoded_path}"
            
            response = self.session.delete(url, timeout=30)
            
            if response.status_code in (200, 204, 404):  # 404 is OK - file already gone
                self.stats.files_deleted += 1
                self.print_msg(f"✓ Deleted: {relative_path}", Colors.GREEN)
                return True
            else:
                self.print_msg(f"✗ Delete failed: {relative_path} (HTTP {response.status_code})", Colors.RED)
                return False
                
        except Exception as e:
            self.print_msg(f"✗ Delete failed: {relative_path} - {e}", Colors.RED)
            return False
    
    def delete_remote_directory(self, relative_path: str) -> bool:
        """Delete an empty directory from remote storage."""
        try:
            # Ensure path ends with / for directory deletion
            if not relative_path.endswith('/'):
                relative_path += '/'
                
            encoded_path = urllib.parse.quote(relative_path, safe='/')
            url = f"{self.config.bunny_storage_url}/{encoded_path}"
            
            response = self.session.delete(url, timeout=30)
            
            if response.status_code in (200, 204, 404):  # 404 is OK - directory already gone
                self.stats.directories_deleted += 1
                self.print_msg(f"✓ Deleted directory: {relative_path}", Colors.GREEN)
                return True
            else:
                self.print_msg(f"✗ Directory delete failed: {relative_path} (HTTP {response.status_code})", Colors.RED)
                return False
                
        except Exception as e:
            self.print_msg(f"✗ Directory delete failed: {relative_path} - {e}", Colors.RED)
            return False
    
    def cleanup_empty_directories(self, remote_directories: Set[str], remaining_remote_files: Set[str]):
        """Clean up empty directories after file deletion."""
        if not remote_directories:
            return
            
        # Sort directories by depth (deepest first) to delete from bottom up
        sorted_dirs = sorted(remote_directories, key=lambda x: x.count('/'), reverse=True)
        
        directories_to_delete = []
        
        for directory in sorted_dirs:
            # Check if directory is empty (no files in it)
            # A directory is empty if no remaining files start with the directory path
            dir_prefix = directory + '/' if not directory.endswith('/') else directory
            has_files = any(file_path.startswith(dir_prefix) for file_path in remaining_remote_files)
            
            if not has_files:
                directories_to_delete.append(directory)
        
        if directories_to_delete:
            print()
            self.print_msg("Removing empty directories...")
            for directory in directories_to_delete:
                self.delete_remote_directory(directory)
    
    def get_local_files(self) -> Dict[str, str]:
        """Get all local files with their full paths."""
        local_files = {}
        src_path = Path(self.config.src_dir)
        
        if not src_path.exists():
            self.print_msg(f"Source directory does not exist: {self.config.src_dir}", Colors.RED)
            return local_files
        
        # First pass: count total files for progress display
        print("  Scanning local files...", end='', flush=True)
        all_files = list(src_path.rglob('*'))
        total_files = sum(1 for f in all_files if f.is_file())
        
        if total_files == 0:
            print(" no files found")
            return local_files
        
        processed_files = 0
        total_size = 0
        last_progress_time = time.time()
        
        for file_path in all_files:
            if file_path.is_file():
                relative_path = str(file_path.relative_to(src_path))
                
                # Skip excluded files
                if not self.should_exclude(relative_path):
                    local_files[relative_path] = str(file_path)
                    # Add file size to total
                    try:
                        total_size += file_path.stat().st_size
                    except OSError:
                        # If we can't get file size, continue without adding to total
                        pass
                
                processed_files += 1
                current_time = time.time()
                
                # Update progress every 0.5 seconds or for every 100 files
                if current_time - last_progress_time >= 0.5 or processed_files % 100 == 0 or processed_files == total_files:
                    progress_percent = (processed_files / total_files) * 100
                    print(f"\r\033[K  Scanning local files... {processed_files}/{total_files} ({progress_percent:.1f}%)", end='', flush=True)
                    last_progress_time = current_time
        
        print(f"\r\033[K  ✓ Found {len(local_files)} local files ({self.format_size(total_size)}, scanned {total_files} total)")
        return local_files
    
    def _analyze_single_file(self, relative_path: str, full_path: str, remote_files: Set[str]) -> Optional[Tuple[bool, int, str]]:
        """Analyze a single file for changes. Returns (needs_upload, file_size, checksum) or None if file missing."""
        # Validate file existence first
        try:
            file_size = os.path.getsize(full_path)
        except (FileNotFoundError, OSError) as e:
            self.print_msg(f"Warning: File missing during analysis '{relative_path}': {e}", Colors.YELLOW)
            return None
        
        # Quick check: if file metadata hasn't changed, use cached checksum
        if not self.file_needs_checksum(relative_path, full_path):
            cached_meta = self.get_cached_metadata(relative_path)
            if cached_meta and 'checksum' in cached_meta:
                cached_checksum = cached_meta['checksum']
                if relative_path not in remote_files:
                    return True, file_size, cached_checksum  # File doesn't exist remotely
                else:
                    return False, file_size, cached_checksum  # File unchanged
        
        # Calculate checksum (use fast method for large files if enabled)
        if self.config.fast_checksum:
            local_checksum = self.get_file_checksum_fast(full_path)
        else:
            local_checksum = self.get_file_checksum(full_path)
        
        # If checksum calculation failed, skip this file
        if local_checksum is None:
            self.print_msg(f"Warning: Skipping file due to checksum failure '{relative_path}'", Colors.YELLOW)
            return None
            
        cached_checksum = self.get_cached_checksum(relative_path)
        
        needs_upload = False
        if cached_checksum:
            # We have a cached checksum
            if local_checksum != cached_checksum:
                # File has changed locally
                needs_upload = True
            elif relative_path not in remote_files:
                # File doesn't exist remotely
                needs_upload = True
        else:
            # No cached checksum
            if relative_path not in remote_files:
                # File doesn't exist remotely
                needs_upload = True
            else:
                # File exists remotely but no cache - save checksum for future
                pass
        
        return needs_upload, file_size, local_checksum
    
    def analyze_changes(self, local_files: Dict[str, str], remote_files: Set[str]) -> Tuple[List[str], List[str], int]:
        """Analyze what files need to be uploaded and deleted with parallel processing."""
        files_to_upload = []
        files_to_delete = []
        total_upload_size = 0
        
        total_local_files = len(local_files)
        
        if total_local_files == 0:
            # Check which remote files should be deleted
            for remote_file in remote_files:
                if self.should_exclude(remote_file):
                    continue
                if remote_file not in local_files:
                    files_to_delete.append(remote_file)
            return files_to_upload, files_to_delete, total_upload_size
        
        print("  Analyzing file changes...", end='', flush=True)
        
        if self.config.parallel_analysis and total_local_files > 10:
            # Use thread pool for parallel processing
            max_workers = min(4, os.cpu_count() or 1)  # Limit to 4 threads to avoid overwhelming disk I/O
            processed_files = 0
            last_progress_time = time.time()
            progress_lock = threading.Lock()
            
            def update_progress():
                nonlocal processed_files, last_progress_time
                with progress_lock:
                    processed_files += 1
                    current_time = time.time()
                    
                    # Update progress every 0.5 seconds or for every 20 files
                    if total_local_files > 10 and (current_time - last_progress_time >= 0.5 or processed_files % 20 == 0 or processed_files == total_local_files):
                        progress_percent = (processed_files / total_local_files) * 100
                        print(f"\r\033[K  Analyzing file changes... {processed_files}/{total_local_files} ({progress_percent:.1f}%)", end='', flush=True)
                        last_progress_time = current_time
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all file analysis tasks
                future_to_file = {
                    executor.submit(self._analyze_single_file, rel_path, full_path, remote_files): rel_path
                    for rel_path, full_path in local_files.items()
                }
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_file):
                    relative_path = future_to_file[future]
                    full_path = local_files[relative_path]
                    
                    try:
                        result = future.result()
                        
                        if result is None:
                            # File was missing or had errors, skip it
                            self.stats.files_missing += 1
                            self.print_msg(f"Skipping missing file: {relative_path}", Colors.YELLOW)
                        else:
                            needs_upload, file_size, checksum = result
                            
                            if needs_upload:
                                files_to_upload.append(relative_path)
                                total_upload_size += file_size
                            else:
                                self.stats.files_skipped += 1
                            
                            # Save metadata to cache
                            self.save_metadata_to_cache(relative_path, full_path, checksum)
                        
                    except Exception as e:
                        self.print_msg(f"Error analyzing {relative_path}: {e}", Colors.YELLOW)
                        # Don't add missing files to upload queue
                        self.print_msg(f"Skipping file due to analysis error: {relative_path}", Colors.YELLOW)
                    
                    update_progress()
        else:
            # Sequential processing for smaller file sets or when parallel is disabled
            processed_files = 0
            last_progress_time = time.time()
            
            for relative_path, full_path in local_files.items():
                try:
                    result = self._analyze_single_file(relative_path, full_path, remote_files)
                    
                    if result is None:
                        # File was missing or had errors, skip it
                        self.stats.files_missing += 1
                        self.print_msg(f"Skipping missing file: {relative_path}", Colors.YELLOW)
                    else:
                        needs_upload, file_size, checksum = result
                        
                        if needs_upload:
                            files_to_upload.append(relative_path)
                            total_upload_size += file_size
                        else:
                            self.stats.files_skipped += 1
                        
                        # Save metadata to cache
                        self.save_metadata_to_cache(relative_path, full_path, checksum)
                    
                except Exception as e:
                    self.print_msg(f"Error analyzing {relative_path}: {e}", Colors.YELLOW)
                    # Don't add missing files to upload queue
                    self.print_msg(f"Skipping file due to analysis error: {relative_path}", Colors.YELLOW)
                
                processed_files += 1
                current_time = time.time()
                
                # Update progress every 0.5 seconds or for every 50 files
                if total_local_files > 10 and (current_time - last_progress_time >= 0.5 or processed_files % 50 == 0 or processed_files == total_local_files):
                    progress_percent = (processed_files / total_local_files) * 100
                    print(f"\r\033[K  Analyzing file changes... {processed_files}/{total_local_files} ({progress_percent:.1f}%)", end='', flush=True)
                    last_progress_time = current_time
        
        print(f"\r\033[K  ✓ Analysis complete: {len(files_to_upload)} to upload, {len(files_to_delete)} to delete")
        
        # Check which remote files should be deleted
        for remote_file in remote_files:
            if self.should_exclude(remote_file):
                continue
                
            if remote_file not in local_files:
                files_to_delete.append(remote_file)
        
        return files_to_upload, files_to_delete, total_upload_size
    
    def confirm_changes(self, files_to_upload: List[str], files_to_delete: List[str], upload_size: int) -> bool:
        """Ask user to confirm changes."""
        total_changes = len(files_to_upload) + len(files_to_delete)
        
        if total_changes == 0:
            self.print_msg("No changes needed - everything is already in sync!", Colors.GREEN)
            return False
        
        print()
        self.print_msg("=== Summary of Changes ===", Colors.BOLD)
        
        if files_to_upload:
            size_formatted = self.format_size(upload_size)
            self.print_msg(f"Files to upload: {len(files_to_upload)} ({size_formatted})", Colors.GREEN)
        
        if files_to_delete:
            self.print_msg(f"Files to delete: {len(files_to_delete)}", Colors.RED)
        
        print()
        response = input(f"{Colors.YELLOW}Do you want to proceed with these changes? (y/N): {Colors.NC}")
        
        return response.lower() in ('y', 'yes')
    
    def sync_files(self):
        """Main sync function."""
        self.print_msg(f"=== BunnyHop v{__version__} ===", Colors.BOLD)
        self.print_msg(f"Source: {self.config.src_dir}", Colors.BLUE)
        self.print_msg(f"Destination: {self.config.bunny_storage_url}", Colors.BLUE)
        # self.print_msg(f"Excluded files: {', '.join(self.config.excluded_files)}", Colors.BLUE)
        self.print_msg(f"Upload chunk size: 256 KB", Colors.BLUE)
        print()
        
        # Get local and remote files
        self.print_msg("Analyzing files...")
        local_files = self.get_local_files()
        remote_files, remote_directories = self.get_remote_files()
        
        # Analyze what changes are needed
        files_to_upload, files_to_delete, upload_size = self.analyze_changes(local_files, remote_files)
        print()
        
        # Ask for confirmation
        if not self.confirm_changes(files_to_upload, files_to_delete, upload_size):
            return
        
        # Record start time for sync operations *after* confirmation
        self.stats.sync_operations_start_time = time.time()
        
        print()
        self.print_msg("Proceeding with sync...", Colors.GREEN)
        print()
        
        # Upload files
        if files_to_upload:
            # Validate files still exist and calculate total upload size
            validated_files = []
            total_upload_size = 0
            missing_files = []
            
            for relative_path in files_to_upload:
                full_path = local_files[relative_path]
                try:
                    file_size = os.path.getsize(full_path)
                    validated_files.append(relative_path)
                    total_upload_size += file_size
                except (FileNotFoundError, OSError) as e:
                    missing_files.append(relative_path)
                    self.print_msg(f"File missing before upload '{relative_path}': {e}", Colors.YELLOW)
                        
            if missing_files:
                self.stats.files_missing += len(missing_files)
                self.print_msg(f"Skipping {len(missing_files)} missing files that were queued for upload", Colors.YELLOW)
            
            if validated_files:
                total_files_to_upload = len(validated_files)
                
                for file_index, relative_path in enumerate(validated_files):
                    full_path = local_files[relative_path]
                    
                    success = self.upload_file(full_path, relative_path, file_index, total_files_to_upload, total_upload_size)
                    
                    if success:
                        # Update cache with new checksum and metadata
                        if self.config.fast_checksum:
                            checksum = self.get_file_checksum_fast(full_path)
                        else:
                            checksum = self.get_file_checksum(full_path)
                        
                        if checksum is not None:
                            self.save_metadata_to_cache(relative_path, full_path, checksum)
        
        # Delete remote files
        if files_to_delete:
            print()
            self.print_msg("Removing remote files...")
            for relative_path in files_to_delete:
                success = self.delete_remote_file(relative_path)
                if success:
                    self.remove_from_cache(relative_path)
                    # Remove from remote_files set so we can identify empty directories
                    remote_files.discard(relative_path)
        
        # Clean up empty directories
        self.cleanup_empty_directories(remote_directories, remote_files)
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print sync summary."""
        end_time = time.time()
        
        total_duration = 0.0
        if self.stats.sync_operations_start_time is not None:
            total_duration = end_time - self.stats.sync_operations_start_time
        else:
            # Fallback or indicate if sync didn't run post-confirmation
            # For now, if sync_operations_start_time wasn't set, it implies no operations ran post-confirmation
            # or an issue. We could use overall_script_start_time for a total script time if desired.
            # However, the request is for "pure sync time".
            pass # total_duration remains 0.0 or could be calculated differently if needed.

        # Format duration
        if total_duration < 0: total_duration = 0 # Should not happen
        
        if total_duration < 60:
            duration_str = f"{total_duration:.0f}s"
        else:
            minutes = int(total_duration // 60)
            seconds = int(total_duration % 60)
            duration_str = f"{minutes:02d}m:{seconds:02d}s"
        
        # Calculate average speed
        if self.stats.files_uploaded > 0 and self.stats.total_upload_time > 0:
            avg_speed = self.calc_speed(self.stats.total_bytes_uploaded, self.stats.total_upload_time)
        else:
            avg_speed = "N/A"
        
        print()
        self.print_msg("=== Summary ===", Colors.BOLD)
        self.print_msg(f"Files uploaded: {self.stats.files_uploaded}", Colors.GREEN)
        self.print_msg(f"Files unchanged: {self.stats.files_skipped}", Colors.BLUE)
        self.print_msg(f"Files deleted: {self.stats.files_deleted}", Colors.RED)
        if self.stats.files_missing > 0:
            self.print_msg(f"Files missing/skipped: {self.stats.files_missing}", Colors.YELLOW)
        self.print_msg(f"Directories deleted: {self.stats.directories_deleted}", Colors.RED)
        self.print_msg(f"Total size uploaded: {self.format_size(self.stats.total_bytes_uploaded)}", Colors.BLUE)
        self.print_msg(f"Average upload speed: {avg_speed}", Colors.BLUE)
        self.print_msg(f"Total sync time: {duration_str}", Colors.BLUE)
        self.print_msg("Sync complete!", Colors.BOLD)
        print()
        self.print_msg(f"=== End of BunnyHop v{__version__} ===", Colors.BOLD)

def main():
    parser = argparse.ArgumentParser(description=f'BunnyHop - Bunny.net storage zone sync tool (v{__version__})')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('--config', default='config.json', help='Configuration file path (default: config.json)')
    parser.add_argument('--src-dir', help='Source directory to sync (overrides config file)')
    parser.add_argument('--api-key', help='Bunny.net API key (overrides config file)')
    parser.add_argument('--storage-url', help='Bunny.net storage URL (overrides config file)')
    parser.add_argument('--cache-dir', help='Cache directory for checksums (overrides config file)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    
    args = parser.parse_args()
    
    # Load config from file
    try:
        config = Config.load_from_file(args.config)
    except Exception as e:
        print(f"{Colors.RED}Configuration Error: {e}{Colors.NC}")
        print(f"{Colors.YELLOW}Please ensure your config file exists and contains valid JSON.{Colors.NC}")
        sys.exit(1)
    
    # Override config with command line arguments
    if args.src_dir:
        config.src_dir = args.src_dir
    if args.api_key:
        config.bunny_api_key = args.api_key
    if args.storage_url:
        config.bunny_storage_url = args.storage_url
    if args.cache_dir:
        config.cache_dir = args.cache_dir
    
    # Create and run sync
    try:
        sync = BunnyStorageSync(config)
        sync.sync_files()
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Sync cancelled by user.{Colors.NC}")
        sys.exit(1)
    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.NC}")
        sys.exit(1)

if __name__ == "__main__":
    main() 