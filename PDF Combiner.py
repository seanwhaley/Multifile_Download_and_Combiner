
"""A PDF processing utility for downloading and combining PDFs from the White House OMB memoranda page.
This module provides functionality to download PDF memoranda from the White House Office of
Management and Budget (OMB) website and combine them into a single PDF document. It includes
logging capabilities and configuration management.
Classes:
    Config: Application configuration container with default paths and URLs
    PDFProcessor: Main class handling PDF operations (downloading, processing, combining)
Functions:
    setup_application_logging: Configures application-wide logging
    main: Application entry point
Example:
    $ python pdf_combiner.py
Dependencies:
    - requests
    - beautifulsoup4
    - PyPDF2
    - logging
    - os
    - datetime
    - typing
    - dataclasses
    - re
    - urllib
The application will:
1. Fetch PDF links from the OMB memoranda page
2. Download individual PDFs to a specified directory
3. Combine all downloaded PDFs into a single document
4. Save the combined PDF with a timestamp
5. Log all operations to both console and file
Notes:
    - Requires write permissions in the specified download and output directories
    - Uses rotating file logs with 5MB size limit and 3 backup files
    - Handles both relative and absolute URLs
    - Implements error handling and logging for all major operations
"""
import os
from io import BytesIO
from typing import List, Optional, Dict
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass
import logging
from logging.handlers import RotatingFileHandler
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader, PdfWriter
from requests.exceptions import HTTPError, RequestException
from typing import Dict, List, Tuple
import time

def setup_application_logging(name: str, log_dir: str) -> logging.Logger:
    """Configure application-wide logging with file and console handlers."""
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
        
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    # Rotating file handler
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f"{name}.log"),
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

@dataclass
class Config:
    """Application configuration."""
    BASE_URL: str = 'https://www.whitehouse.gov'
    MEMO_URL: str = 'https://www.whitehouse.gov/omb/information-for-agencies/memoranda/'
    A11_URL: str = 'https://www.whitehouse.gov/wp-content/uploads/2018/06/a11_web_toc.pdf'
    DOWNLOAD_DIR: str = os.path.join(os.path.expanduser('~'), 'Downloads', 'DOJ EA Proposal', 'Downloaded_PDFs')
    OUTPUT_DIR: str = os.path.join(os.path.expanduser('~'), 'Downloads', 'DOJ EA Proposal', 'Combined_Files')
    LOG_DIR: str = os.path.join(os.path.dirname(__file__), 'logs')
    PDF_PATTERN: str = r'.*\.pdf$'
    WORD_LIMIT: int = 50000  # Maximum words per combined PDF
    CACHE_FILE: str = os.path.join(os.path.dirname(__file__), 'download_cache.json')
    FORCE_DOWNLOAD: bool = False

class PDFProcessor:
    """Handles PDF download, processing and combination operations."""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_application_logging("pdf_combiner", config.LOG_DIR)
        self._validate_paths()
    
    def _validate_paths(self) -> None:
        """Ensure all required paths exist and are writable."""
        for path in [self.config.DOWNLOAD_DIR, self.config.OUTPUT_DIR]:
            if not os.path.isabs(path):
                raise ValueError(f"Path must be absolute: {path}")
            os.makedirs(path, exist_ok=True)
            if not os.access(path, os.W_OK):
                raise PermissionError(f"Directory not writable: {path}")
    
    def get_pdf_links(self) -> List[str]:
        """Retrieve PDF links from the memo page."""
        try:
            self.logger.info("Fetching PDF links from memo page...")
            response = requests.get(self.config.MEMO_URL, timeout=30)
            response.raise_for_status()
            
            self.logger.info("Parsing webpage content...")
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=re.compile(self.config.PDF_PATTERN))
            
            pdf_links = [self._get_absolute_url(link['href']) for link in links]
            self.logger.info(f"Found {len(pdf_links)} PDF links")
            return pdf_links
            
        except Exception as e:
            self.logger.error(f"Error getting PDF links: {str(e)}")
            return []

    def _get_absolute_url(self, relative_url: str) -> str:
        """Convert relative URL to absolute URL."""
        if bool(urlparse(relative_url).netloc):
            return relative_url
        return urljoin(self.config.BASE_URL, relative_url)

    def _should_download(self, url: str, target_path: str) -> bool:
        """Check if file should be downloaded."""
        if self.config.FORCE_DOWNLOAD:
            return True
            
        if not os.path.exists(target_path):
            return True
            
        try:
            response = requests.head(url, timeout=30)
            remote_size = int(response.headers.get('content-length', 0))
            local_size = os.path.getsize(target_path)
            
            if remote_size != local_size:
                return True
                
            self.logger.debug(f"Skipping existing file: {target_path}")
            return False
            
        except Exception as e:
            self.logger.warning(f"Error checking file {url}: {str(e)}")
            return True

    def download_pdfs(self, urls: List[str], max_retries: int = 3) -> Tuple[Dict[str, str], List[str]]:
        """
        Download PDFs from provided URLs.
        Returns tuple of (successful_downloads, failed_urls)
        """
        downloaded_files = {}
        failed_urls = []
        
        for url in urls:
            retries = 0
            while retries < max_retries:
                try:
                    filename = os.path.join(self.config.DOWNLOAD_DIR, os.path.basename(url))
                    
                    if not self._should_download(url, filename):
                        downloaded_files[url] = filename
                        break
                        
                    response = requests.get(url, timeout=30)
                    
                    if response.status_code == 404:
                        self.logger.warning(f"File not found (404): {url}")
                        failed_urls.append(url)
                        break
                        
                    response.raise_for_status()
                    
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    downloaded_files[url] = filename
                    self.logger.info(f"Downloaded: {url}")
                    break
                    
                except HTTPError as he:
                    if he.response.status_code == 404:
                        self.logger.warning(f"File not found (404): {url}")
                        failed_urls.append(url)
                        break
                    retries += 1
                    if retries == max_retries:
                        self.logger.error(f"Max retries reached for {url}: {str(he)}")
                        failed_urls.append(url)
                    else:
                        time.sleep(1)  # Wait before retry
                        
                except Exception as e:
                    self.logger.error(f"Error downloading {url}: {str(e)}")
                    failed_urls.append(url)
                    break
                    
        return downloaded_files, failed_urls

    def _count_words(self, pdf_reader: PdfReader) -> int:
        """Count approximate words in a PDF."""
        word_count = 0
        for page in pdf_reader.pages:
            text = page.extract_text()
            words = text.split()
            word_count += len(words)
        return word_count

    def combine_pdfs(self, downloaded_files: Dict[str, str]) -> List[str]:
        """Combine downloaded PDFs into multiple files based on word limit."""
        output_paths = []
        current_merger = PdfWriter()
        current_word_count = 0
        file_counter = 1
        
        try:
            for filepath in downloaded_files.values():
                reader = PdfReader(filepath)
                file_words = self._count_words(reader)
                
                # If adding this file would exceed limit, save current and start new
                if current_word_count + file_words > self.config.WORD_LIMIT and current_word_count > 0:
                    output_path = os.path.join(
                        self.config.OUTPUT_DIR,
                        f'combined_memos_part{file_counter}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                    )
                    with open(output_path, 'wb') as output_file:
                        current_merger.write(output_file)
                    output_paths.append(output_path)
                    self.logger.info(f"Created combined PDF part {file_counter} at: {output_path}")
                    
                    # Reset for next file
                    current_merger = PdfWriter()
                    current_word_count = 0
                    file_counter += 1
                
                # Add pages to current merger
                for page in reader.pages:
                    current_merger.add_page(page)
                current_word_count += file_words
            
            # Save final file if there's anything left
            if current_word_count > 0:
                output_path = os.path.join(
                    self.config.OUTPUT_DIR,
                    f'combined_memos_part{file_counter}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                )
                with open(output_path, 'wb') as output_file:
                    current_merger.write(output_file)
                output_paths.append(output_path)
                self.logger.info(f"Created final combined PDF part {file_counter} at: {output_path}")
            
            return output_paths
            
        except Exception as e:
            self.logger.error(f"Error combining PDFs: {str(e)}")
            return []

def main() -> None:
    try:
        config = Config()
        processor = PDFProcessor(config)
        
        pdf_links = processor.get_pdf_links()
        if not pdf_links:
            processor.logger.error("No PDF links found")
            return

        downloaded_files, failed_urls = processor.download_pdfs(pdf_links)
        if failed_urls:
            processor.logger.warning(f"Failed to download {len(failed_urls)} files")
        
        if downloaded_files:
            output_paths = processor.combine_pdfs(downloaded_files)
            if output_paths:
                processor.logger.info(f"Created {len(output_paths)} combined PDF files")
    except Exception as e:
        processor.logger.exception("Fatal error in main application")
        raise
    finally:
        processor.logger.info("Application shutting down")

if __name__ == "__main__":
    main()