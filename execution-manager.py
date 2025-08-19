import os
import subprocess
import pandas as pd
import findspark
import streamlit as st
from hdfs import InsecureClient
from pymongo import MongoClient
from config.config import Config
from log_utils.logger_setup import LoggerSetup

findspark.init()

class ExecutionManager:
    def __init__(self):
        self.logger = LoggerSetup().logger
        self.hdfs_client = self._connect_hdfs()
        self.mongo_coll = self._connect_mongo()

    def _connect_hdfs(self):
        try:
            return InsecureClient(Config.HDFS_URL)
        except Exception as e:
            st.error(f" Failed to connect to HDFS: {e}")
            self.logger.error(f"HDFS connection failed: {e}")
            return None

    def _connect_mongo(self):
        try:
            client = MongoClient(Config.MONGODB_URI)
            db = client[Config.MONGO_DATABASE]
            return db[Config.MONGO_COLLECTION]
        except Exception as e:
            st.error(f" Failed to connect to MongoDB: {e}")
            self.logger.error(f"MongoDB connection failed: {e}")
            return None

    def _safe_read_file(self, file_path):
        try:
            with self.hdfs_client.read(file_path, encoding=None) as reader:
                content = reader.read(10 * 1024)  # 10KB limit
            try:
                return content.decode('utf-8')
            except UnicodeDecodeError:
                return content.decode('latin-1', errors='ignore')
        except Exception as e:
            self.logger.error(f"Failed to read file: {e}")
            return None

    def _preview_file(self, file_path):
        ext = os.path.splitext(file_path)[1].lower()
        if ext != '.py':
            st.info(" Only Python files can be previewed.")
            return
        content = self._safe_read_file(file_path)
        if content:
            st.subheader(f"Preview of {file_path}")
            st.text(content[:1000])

    def _get_and_track_files(self):
        file_paths = []
        try:
            file_list = self.hdfs_client.list(Config.HDFS_DIRECTORY)
            for fname in file_list:
                fpath = f"{Config.HDFS_DIRECTORY}/{fname}"
                file_paths.append(fpath)
                if not self.mongo_coll.find_one({'code_path': fpath}):
                    self.mongo_coll.insert_one({
                        'code_path': fpath,
                        'status': 'pending',
                        'last_run': None,
                        'message': None
                    })
            return file_paths
        except Exception as e:
            self.logger.error(f"Error fetching files: {e}")
            return []

    def run_jobs(self):
        files = self._get_and_track_files()
        py_files = [f for f in files if f.endswith('.py')]

        if not py_files:
            st.warning("No Python (.py) files found.")
            return

        form = st.form("job_form")
        selected_files = form.multiselect("Select files to run", py_files)
        retry_count = form.text_input("Retry count for failures", value="1")
        submitted = form.form_submit_button("Run")

        if submitted:
            for file_path in selected_files:
                self._preview_file(file_path)
                cmd = f"spark-submit --master spark://0.0.0.0:7077 hdfs://localhost:9000{file_path}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

                if result.returncode == 0:
                    self._update_status(file_path, "success", "Execution succeeded")
                    st.success(f"✅ {file_path} executed successfully.")
                else:
                    self._handle_retry(file_path, cmd, int(retry_count))

    def _handle_retry(self, file_path, cmd, retries):
        self.logger.warning(f"Initial run failed. Retrying {file_path}...")
        for attempt in range(1, retries + 1):
            st.write(f"🔁 Retry {attempt}: `{file_path}`")
            retry_result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if retry_result.returncode == 0:
                self._update_status(file_path, "success", f"Retry {attempt} succeeded")
                st.success(f"✅ Retry {attempt} succeeded: `{file_path}`")
                return
        self._update_status(file_path, "failed", f"All {retries} retries failed")
        st.error(f"❌ All retries failed: `{file_path}`")

    def _update_status(self, file_path, status, message):
        self.mongo_coll.update_one({'code_path': file_path}, {
            '$set': {
                'status': status,
                'last_run': pd.Timestamp.now(),
                'message': message
            }
        })
