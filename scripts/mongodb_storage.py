import gridfs
import json
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os
import hashlib
from urllib.parse import urlparse
import mimetypes

class ScrapedDataStorage:
    def __init__(self, connection_string="mongodb://admin:scraperpass123@mongodb:27017/", db_name="scraped_data"):
        """Initialize MongoDB connection for containerized environment"""
        try:
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            self.db = self.client[db_name]
            self.fs = gridfs.GridFS(self.db)
            
            # Collections for different data types
            self.pages_collection = self.db['scraped_pages']
            self.files_collection = self.db['scraped_files'] 
            self.metadata_collection = self.db['scraping_sessions']
            
            # Test connection
            self.client.admin.command('ping')
            print("✅ MongoDB connection established successfully")
            
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
            raise
    
    def create_scraping_session(self, start_url, max_depth, user_agent, dag_run_id=None):
        """Create a new scraping session record"""
        doc = {
            "start_url": start_url,
            "domain": urlparse(start_url).netloc,
            "max_depth": max_depth,
            "user_agent": user_agent,
            "dag_run_id": dag_run_id,
            "started_at": datetime.utcnow(),
            "status": "running",
            "pages_scraped": 0,
            "files_downloaded": 0,
            "total_file_size": 0
        }
        result = self.metadata_collection.insert_one(doc)
        print(f"🚀 Created scraping session: {result.inserted_id}")
        return result.inserted_id
    
    def store_scraped_page(self, url, content, page_format, depth, session_id):
        """Store HTML/text content from scraped pages"""
        content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
        
        # Check for duplicate content
        existing = self.pages_collection.find_one({"content_hash": content_hash})
        if existing:
            print(f"🔁 Duplicate content skipped: {url}")
            return existing['_id']
        
        doc = {
            "url": url,
            "domain": urlparse(url).netloc,
            "content": content,
            "format": page_format,
            "depth": depth,
            "scraped_at": datetime.utcnow(),
            "session_id": session_id,
            "content_hash": content_hash,
            "content_length": len(content)
        }
        
        result = self.pages_collection.insert_one(doc)
        print(f"📄 Stored page: {url}")
        return result.inserted_id
    
    def store_file(self, file_path, source_url, file_type, session_id):
        """Store different types of files based on their format"""
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return None
            
        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        # Create base metadata
        metadata = {
            "filename": filename,
            "source_url": source_url,
            "source_domain": urlparse(source_url).netloc,
            "file_type": file_type,
            "file_size": file_size,
            "scraped_at": datetime.utcnow(),
            "session_id": session_id
        }
        
        # Handle different file types
        try:
            if file_type == 'txt':
                return self._store_text_file(file_path, metadata)
            elif file_type == 'json':
                return self._store_json_file(file_path, metadata)
            elif file_type == 'csv':
                return self._store_csv_file(file_path, metadata)
            elif file_type in ['xlsx', 'xls']:
                return self._store_excel_file(file_path, metadata)
            else:
                # Store as binary in GridFS (PDF, DOCX, audio, etc.)
                return self._store_binary_file(file_path, metadata)
        except Exception as e:
            print(f"❌ Error storing file {filename}: {e}")
            return None
    
    def _store_text_file(self, file_path, metadata):
        """Store text files as documents"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            doc = {
                **metadata, 
                "content": content, 
                "storage_type": "document",
                "word_count": len(content.split()),
                "line_count": len(content.splitlines())
            }
            
            result = self.files_collection.insert_one(doc)
            print(f"📝 Stored text file: {metadata['filename']}")
            return result.inserted_id
        except Exception as e:
            print(f"❌ Error storing text file: {e}")
            return None
    
    def _store_json_file(self, file_path, metadata):
        """Store JSON files as structured documents"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            doc = {
                **metadata, 
                "data": json_data, 
                "storage_type": "document",
                "json_keys": list(json_data.keys()) if isinstance(json_data, dict) else None
            }
            
            result = self.files_collection.insert_one(doc)
            print(f"🔧 Stored JSON file: {metadata['filename']}")
            return result.inserted_id
        except Exception as e:
            print(f"❌ Error storing JSON file: {e}")
            return None
    
    def _store_csv_file(self, file_path, metadata):
        """Store CSV files as structured documents"""
        try:
            df = pd.read_csv(file_path)
            records = df.to_dict('records')
            
            doc = {
                **metadata, 
                "records": records, 
                "columns": df.columns.tolist(),
                "row_count": len(df),
                "storage_type": "document"
            }
            
            result = self.files_collection.insert_one(doc)
            print(f"📊 Stored CSV file: {metadata['filename']} ({len(df)} rows)")
            return result.inserted_id
        except Exception as e:
            print(f"❌ Error storing CSV file: {e}")
            return None
    
    def _store_excel_file(self, file_path, metadata):
        """Store Excel files with multiple sheets"""
        try:
            excel_data = pd.read_excel(file_path, sheet_name=None)
            
            sheets_data = {}
            total_rows = 0
            for sheet_name, df in excel_data.items():
                sheets_data[sheet_name] = {
                    "records": df.to_dict('records'),
                    "columns": df.columns.tolist(),
                    "row_count": len(df)
                }
                total_rows += len(df)
            
            doc = {
                **metadata, 
                "sheets": sheets_data,
                "sheet_names": list(excel_data.keys()),
                "total_rows": total_rows,
                "storage_type": "document"
            }
            
            result = self.files_collection.insert_one(doc)
            print(f"📈 Stored Excel file: {metadata['filename']} ({len(excel_data)} sheets, {total_rows} total rows)")
            return result.inserted_id
        except Exception as e:
            print(f"❌ Error storing Excel file: {e}")
            return None
    
    def _store_binary_file(self, file_path, metadata):
        """Store binary files using GridFS"""
        try:
            with open(file_path, 'rb') as f:
                file_id = self.fs.put(
                    f,
                    filename=metadata['filename'],
                    **{k: v for k, v in metadata.items() if k != 'filename'}
                )
            
            # Store reference in files collection
            doc = {
                **metadata, 
                "gridfs_id": file_id, 
                "storage_type": "gridfs"
            }
            
            result = self.files_collection.insert_one(doc)
            print(f"💾 Stored binary file: {metadata['filename']} ({metadata['file_size']} bytes)")
            return result.inserted_id
        except Exception as e:
            print(f"❌ Error storing binary file: {e}")
            return None
    
    def update_scraping_session(self, session_id, status, pages_scraped=0, files_downloaded=0, total_file_size=0):
        """Update scraping session with final results"""
        try:
            self.metadata_collection.update_one(
                {"_id": session_id},
                {
                    "$set": {
                        "status": status,
                        "pages_scraped": pages_scraped,
                        "files_downloaded": files_downloaded,
                        "total_file_size": total_file_size,
                        "completed_at": datetime.utcnow()
                    }
                }
            )
            print(f"✅ Updated session {session_id}: {pages_scraped} pages, {files_downloaded} files")
        except Exception as e:
            print(f"❌ Error updating session: {e}")
    
    def get_session_stats(self, session_id):
        """Get statistics for a scraping session"""
        try:
            session = self.metadata_collection.find_one({"_id": session_id})
            if not session:
                return None
                
            pages_count = self.pages_collection.count_documents({"session_id": session_id})
            files_count = self.files_collection.count_documents({"session_id": session_id})
            
            return {
                "session_info": session,
                "pages_scraped": pages_count,
                "files_stored": files_count
            }
        except Exception as e:
            print(f"❌ Error getting session stats: {e}")
            return None