import streamlit as st
from supabase import create_client, Client
import datetime
from .utils import DB_TABLE_REPORTS
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@st.cache_resource # Cache the Supabase client connection
def init_supabase() -> Client | None:
    """Initializes and returns the Supabase client."""
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_SERVICE_KEY"]
        logger.info("Initializing Supabase client...")
        client = create_client(url, key)
        
        logger.info("Supabase client initialized.")
        return client
    except KeyError as e:
        st.error(f"🔥 Missing Supabase secret: {e}. Check Streamlit secrets.")
        logger.error(f"Missing Supabase secret: {e}")
        return None
    except Exception as e:
        st.error(f"🔥 Supabase initialization failed: {e}")
        logger.error(f"Supabase init error: {e}", exc_info=True)
        return None

def insert_report(supabase: Client, report_data: dict) -> tuple[str | None, str | None]:
    """Inserts a report into the Supabase database."""
    if not supabase: return None, "Supabase client not initialized."
    try:
        required = ['report_id', 'report_size', 'report_type', 'image_url', 'gcs_path', 'status']
        if not all(k in report_data for k in required):
            missing = [k for k in required if k not in report_data]
            return None, f"Missing required fields for DB insert: {missing}"

        db_payload = {k: v for k, v in report_data.items() if k in [
            'report_id', 'report_size', 'report_type', 'image_url', 'gcs_path',
            'original_filename', 'status', 'latitude', 'longitude', 'location_accuracy']}

        response = supabase.table(DB_TABLE_REPORTS).insert(db_payload).execute()
        logger.info(f"Supabase insert response: {response}")

        # Handle potential variations in Supabase response structure
        if hasattr(response, 'data') and response.data:
            inserted_id = response.data[0].get('report_id')
            if inserted_id == report_data['report_id']:
                logger.info(f"Successfully inserted report: {report_data['report_id']}")
                return report_data['report_id'], None
            else:
                logger.warning(f"Insert successful but response format unexpected/ID mismatch: {response.data}")
                return report_data['report_id'], "Insert successful, but validation unclear."
        elif hasattr(response, 'count') and response.count is not None and response.count > 0:
            logger.warning(f"Insert count indicates success ({response.count}), but data is empty. Assuming success for ID: {report_data['report_id']}")
            return report_data['report_id'], None
        elif hasattr(response, 'error') and response.error:
            error_msg = f"Supabase insert error: {response.error.message}"
            logger.error(error_msg)
            return None, error_msg
        else:
            error_msg = f"Insert failed, unknown response structure: {response}"
            logger.error(error_msg)
            return None, error_msg

    except Exception as e:
        error_msg = f"Database error inserting report: {e}"
        logger.error(error_msg, exc_info=True)
        return None, error_msg

def fetch_reports(supabase: Client, limit: int = 100) -> tuple[list[dict] | None, str | None]:
    """Fetches recent reports from Supabase."""
    if not supabase: return None, "Supabase client not initialized."
    try:
        response = supabase.table(DB_TABLE_REPORTS)\
                           .select("*")\
                           .order("created_at", desc=True)\
                           .limit(limit)\
                           .execute()
        logger.info(f"Supabase fetch response: {response}") # Log response

        if hasattr(response, 'data'):
            # Parse timestamp strings
            for report in response.data:
                if 'created_at' in report and isinstance(report['created_at'], str):
                    try: report['created_at_dt'] = datetime.datetime.fromisoformat(report['created_at'].replace('Z', '+00:00'))
                    except ValueError: report['created_at_dt'] = None
                else: report['created_at_dt'] = report.get('created_at')
            return response.data, None # Success, return list (can be empty)
        elif hasattr(response, 'error') and response.error:
            error_msg = f"Fetch failed: {response.error.message}"
            logger.error(error_msg)
            return None, error_msg
        else: # No data, no error
            logger.info("No reports found matching criteria.")
            return [], None # Return empty list

    except Exception as e:
        error_msg = f"Database error fetching reports: {e}"
        logger.error(error_msg, exc_info=True)
        return None, error_msg

def update_report_status(supabase: Client, report_id: str, new_status: str) -> tuple[bool, str | None]:
    """Updates the status of a specific report."""
    if not supabase: return False, "Supabase client not initialized."
    try:
        response = supabase.table(DB_TABLE_REPORTS)\
                           .update({"status": new_status})\
                           .eq("report_id", report_id)\
                           .execute()
        logger.info(f"Supabase update response for {report_id}: {response}")

        # Check different response possibilities for success
        if hasattr(response, 'data') and response.data:
            logger.info(f"Report {report_id} status updated to {new_status}.")
            return True, None
        elif hasattr(response, 'count') and response.count is not None and response.count > 0:
             logger.info(f"Report {report_id} status updated to {new_status} (based on count).")
             return True, None
        elif hasattr(response, 'error') and response.error:
             error_msg = f"Update failed: {response.error.message}"
             logger.error(error_msg)
             return False, error_msg
        else: # No data, no count, no error -> likely report_id didn't match
             error_msg = f"Update failed for report {report_id} (Report ID might not exist)."
             logger.warning(error_msg + f" Response: {response}")
             return False, error_msg

    except Exception as e:
        error_msg = f"Database error updating status for {report_id}: {e}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg

def delete_report_metadata(supabase: Client, report_id: str) -> tuple[bool, str | None]:
    """Deletes a report's metadata from Supabase."""
    if not supabase: return False, "Supabase client not initialized."
    try:
        response = supabase.table(DB_TABLE_REPORTS)\
                           .delete()\
                           .eq("report_id", report_id)\
                           .execute()
        logger.info(f"Supabase delete response for {report_id}: {response}")

        # Check different response possibilities for success
        if hasattr(response, 'data') and response.data:
             logger.info(f"Deleted report metadata {report_id} from DB.")
             return True, None
        elif hasattr(response, 'count') and response.count is not None and response.count > 0:
             logger.info(f"Deleted report metadata {report_id} from DB (based on count).")
             return True, None
        elif hasattr(response, 'error') and response.error:
             error_msg = f"Delete failed: {response.error.message}"
             logger.error(error_msg)
             return False, error_msg
        else: # No data, no count, no error -> likely report_id didn't match
             error_msg = f"Delete metadata failed for report {report_id} (Report ID might not exist)."
             logger.warning(error_msg + f" Response: {response}")
             # Consider if not found should be success or failure - let's say failure for clarity
             return False, error_msg

    except Exception as e:
        error_msg = f"Database error deleting metadata for {report_id}: {e}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg