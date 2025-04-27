# modules/storage_handler.py
import streamlit as st
from google.cloud import storage
import google.oauth2.service_account
import json
from .utils import GCS_REPORT_FOLDER
import uuid
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# init_gcs function remains the same...
@st.cache_resource
def init_gcs() -> storage.Bucket | None:
    """Initializes Google Cloud Storage client and returns the bucket object."""
    try:
        gcs_sa_info = json.loads(st.secrets["GCS_SERVICE_ACCOUNT_JSON"])
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(gcs_sa_info)
        logger.info("Initializing GCS client...")
        storage_client = storage.Client(credentials=credentials)
        bucket_name = st.secrets["GCS_BUCKET_NAME"]
        bucket = storage_client.bucket(bucket_name)
        if bucket.exists():
             logger.info(f"GCS client initialized. Connected to bucket: {bucket_name}")
             return bucket
        else:
             st.error(f"GCS Bucket '{bucket_name}' not found or access denied.")
             logger.error(f"Error: GCS Bucket '{bucket_name}' not found or access denied.")
             return None
    except KeyError as e:
         st.error(f"🔥 Missing GCS secret: {e}. Check Streamlit secrets.")
         logger.error(f"Missing GCS secret: {e}")
         return None
    except Exception as e:
        st.error(f"🔥 GCS initialization failed: {e}")
        logger.error(f"GCS init error: {e}", exc_info=True)
        return None


def upload_photo(bucket: storage.Bucket, report_id: str, file_obj, extension: str) -> tuple[str | None, str | None, str | None]:
    """Uploads photo to GCS and returns public URL and GCS path (for Uniform Bucket Access)."""
    if not bucket: return None, None, "GCS bucket not initialized."
    if not file_obj: return None, None, "No file object provided for upload."

    try:
        gcs_path = f"{GCS_REPORT_FOLDER}/{report_id}{extension}"
        blob = bucket.blob(gcs_path)

        file_obj.seek(0)
        blob.upload_from_file(file_obj, content_type=file_obj.type)
        logger.info(f"Image uploaded to GCS path: {gcs_path}")

        # --- CORRECTION: Do NOT call make_public() for Uniform Buckets ---
        # blob.make_public() # REMOVE OR COMMENT OUT THIS LINE

        # --- Construct the public URL manually for uniform access ---
        # Format: https://storage.googleapis.com/BUCKET_NAME/OBJECT_NAME
        public_url = f"https://storage.googleapis.com/{bucket.name}/{gcs_path}"
        logger.info(f"Constructed Public URL (Uniform Access): {public_url}")

        return public_url, gcs_path, None # Success

    except Exception as e:
        error_msg = f"GCS upload failed: {e}"
        logger.error(error_msg, exc_info=True)
        # Check if it's a permission error during upload itself
        if "403" in str(e) or "permission" in str(e).lower():
             error_msg += " (Check Service Account permissions - needs Storage Object Creator/Admin)"
        return None, None, error_msg

# delete_photo function remains the same...
def delete_photo(bucket: storage.Bucket, gcs_path: str) -> tuple[bool, str | None]:
    """Deletes a photo from GCS given its path."""
    if not bucket: return False, "GCS bucket not initialized."
    if not gcs_path: return False, "No GCS path provided for deletion."
    try:
        blob = bucket.blob(gcs_path)
        if blob.exists():
             blob.delete()
             logger.info(f"Deleted image {gcs_path} from GCS.")
             return True, None
        else:
             logger.warning(f"Image {gcs_path} not found in GCS for deletion.")
             return True, "Image already deleted or path incorrect."
    except Exception as e:
        error_msg = f"GCS deletion failed for {gcs_path}: {e}"
        # Check for permission error during delete
        if "403" in str(e) or "permission" in str(e).lower():
             error_msg += " (Check Service Account permissions - needs Storage Object Admin)"
        logger.error(error_msg, exc_info=True)
        return False, error_msg