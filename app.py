import streamlit as st
import pandas as pd
import datetime
import json
import uuid
from pathlib import Path
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from modules.data_handler import load_and_process_data
    from modules.db_handler import init_supabase, insert_report, fetch_reports, update_report_status, delete_report_metadata
    from modules.storage_handler import init_gcs, upload_photo, delete_photo

except ImportError as e:
     st.error(f"Error importing modules: {e}. Make sure the 'modules' folder exists and contains __init__.py.")
     st.stop()


# JS Communication
from streamlit_js_eval import get_geolocation

# --- Page Config ---
st.set_page_config(
    page_title="TrashGuard",
    page_icon="♻️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Initialize Connections ---
@st.cache_resource
def initialize_backend():
    """Initialize Supabase and GCS, return clients."""
    logger.info("Attempting to initialize backend services...")
    supabase_client = init_supabase()
    gcs_bucket = init_gcs()
    return supabase_client, gcs_bucket

supabase_client, gcs_bucket = initialize_backend()

# --- Load Secrets ---
try:
    MAPS_API_KEY = st.secrets["MAPS_KEY"]
    CKAN_RID = st.secrets["CKAN_RID"]
    # Firebase/GCS secrets checked during init
    if not supabase_client or not gcs_bucket:
        st.error("Failed to initialize backend services (Supabase/GCS). Check secrets and logs.")
        st.stop()
except KeyError as e:
    st.error(f"❌ Missing core secret key: {e}.")
    st.stop()

# --- Geolocation State ---
if 'report_location' not in st.session_state:
    st.session_state.report_location = None

# --- Load CKAN Data ---
# Wrap data loading in try-except block at app level
try:
    processed_df, hotspot_points = load_and_process_data(CKAN_RID)
except Exception as e:
    st.error(f"Error during initial data load: {e}")
    logger.error(f"Initial data load failed: {e}", exc_info=True)
    processed_df = None
    hotspot_points = None


# --- Main UI ---
st.title("♻️ TrashGuard San José")
st.markdown("Visualizing hotspots & reporting illegal dumping with persistent storage.")

if processed_df is not None:
     st.metric(label="Illegal Dumping Reports (Last 90 Days, Processed)", value=f"{len(processed_df):,}")
else:
     st.metric(label="Illegal Dumping Reports (Last 90 Days, Processed)", value="Error loading")


# --- Map & Geolocation ---
st.subheader("Illegal Dumping Heatmap & Report Location")
if not MAPS_API_KEY:
    st.error("Google Maps API Key not found.")
elif hotspot_points is None: # Check if data loading failed
    st.error("Failed to load heatmap data (CKAN fetch/process error).")
else:
    # Geolocation Button
    st.write("Use the button below to capture your current location for the report.")
    # This component will be disabled if executed server-side during non-interactive phases
    # Need to handle potential errors if streamlit_js_eval cannot run
    geo_loc = None
    try:
        with st.spinner("Waiting for location capture..."): # Show spinner
            geo_loc = get_geolocation(key="geo") # Add a key for stability
    except Exception as e:
         st.warning(f"Could not render geolocation component: {e}")


    # Update session state and show feedback
    if geo_loc and 'coords' in geo_loc:
        st.session_state.report_location = {
            "latitude": geo_loc['coords']['latitude'],
            "longitude": geo_loc['coords']['longitude'],
            "accuracy": geo_loc['coords']['accuracy'],
            "timestamp": geo_loc['timestamp']
        }
        # Display captured location outside the spinner context if successful
        st.success(f"📍 Location captured: {st.session_state.report_location['latitude']:.5f}, {st.session_state.report_location['longitude']:.5f} (Accuracy: {st.session_state.report_location['accuracy']:.0f}m)")

    elif geo_loc and 'error' in geo_loc:
         st.warning(f"Could not get location: {geo_loc['error']}")
         st.session_state.report_location = None # Clear any previous location on error

    # Map Display
    hotspots_json = json.dumps(hotspot_points)
    map_html = f"""
    <div id="map" style="height:500px; width:100%;"></div>
    <script>
        function initMap() {{
            const sanJose = {{ lat: 37.3382, lng: -121.8863 }};
            const map = new google.maps.Map(document.getElementById('map'), {{ zoom: 12, center: sanJose, mapTypeId: 'roadmap' }});
            const heatmapData = {hotspots_json};
            if (heatmapData && heatmapData.length > 0) {{
                 const points = heatmapData.map(p => {{
                     const lat = parseFloat(p.location.lat); const lng = parseFloat(p.location.lng);
                     return (!isNaN(lat) && !isNaN(lng)) ? {{ location: new google.maps.LatLng(lat, lng), weight: p.weight }} : null;
                 }}).filter(p => p !== null);
                 if (points.length > 0) {{ new google.maps.visualization.HeatmapLayer({{ data: points, radius: 20, maxIntensity: 15, opacity: 0.75 }}).setMap(map); }}
                 else {{ displayNoDataMessage(map, sanJose); }}
            }} else {{ displayNoDataMessage(map, sanJose); }}

             const currentLocation = {json.dumps(st.session_state.report_location)}; // Pass Python state to JS
             if (currentLocation && currentLocation.latitude) {{
                 const marker = new google.maps.Marker({{
                     position: {{ lat: currentLocation.latitude, lng: currentLocation.longitude }},
                     map: map,
                     title: `Captured Location (Accuracy: ${{currentLocation.accuracy?.toFixed(0)}}m)`
                 }});
                 map.setCenter({{ lat: currentLocation.latitude, lng: currentLocation.longitude }});
                 map.setZoom(16);
                 // Add info window?
                 const infowindow = new google.maps.InfoWindow({{
                    content: `Captured Location<br>Acc: ${{currentLocation.accuracy?.toFixed(0)}}m`
                 }});
                 marker.addListener('click', () => {{ infowindow.open(map, marker); }});
             }}
        }}
        function displayNoDataMessage(map, position) {{
             const infoWindow = new google.maps.InfoWindow({{ content: "No heatmap data...", position: position }}); infoWindow.open(map);
        }}
    </script>
    <script async defer src="https://maps.googleapis.com/maps/api/js?key={MAPS_API_KEY}&libraries=visualization&callback=initMap"></script>
    """
    with st.container():
        st.components.v1.html(map_html, height=520)

st.markdown("---")

# --- Report Form ---
st.subheader("📸 Report New Illegal Dumping")

col1, col2 = st.columns(2)
with col1:
    # Use a consistent key, maybe tied to submission status? Or just keep it simple.
    uploaded_photo = st.file_uploader("1. Upload Photo:", type=["jpg", "png", "jpeg"], key="report_photo_uploader")
    if uploaded_photo: st.image(uploaded_photo, caption="Preview", width=200)
with col2:
    report_size = st.selectbox("2. Estimate Size:", ["<Select>", "Small", "Medium", "Large"], key="report_size_select")
    report_type = st.selectbox("3. Main Type:", ["<Select>", "Household Bags", "Furniture", "Mattress", "E-waste", "Tires", "Construction", "Hazardous", "Yard Waste", "Other"], key="report_type_select")
    if st.session_state.report_location:
         st.write(f"✅ Using captured location.")
    else:
         st.write("❓ Location not captured (use button above map).")

submit_ready = (uploaded_photo is not None and report_size != "<Select>" and report_type != "<Select>")
submit_label = "⬆️ Submit Report" if submit_ready else "Complete Fields Above to Submit"

# Handle submission outside the columns
if st.button(submit_label, key="submit_report_button", disabled=not submit_ready):
    if supabase_client and gcs_bucket:
        with st.spinner("Submitting report..."):
            try:
                report_id = str(uuid.uuid4())
                image_extension = Path(uploaded_photo.name).suffix.lower()
                if image_extension not in ['.jpg', '.jpeg', '.png']: image_extension = '.jpg' # Sanitize

                # 1. Upload photo to GCS
                logger.info(f"Uploading photo for report {report_id}...")
                image_url, gcs_path, storage_error = upload_photo(gcs_bucket, report_id, uploaded_photo, image_extension)

                if storage_error:
                    st.error(f"Failed to upload image: {storage_error}")
                    # No need to proceed if image upload failed
                else:
                    # 2. Prepare metadata for Supabase
                    report_data = {
                        "report_id": report_id,
                        "report_size": report_size,
                        "report_type": report_type,
                        "image_url": image_url,
                        "gcs_path": gcs_path,
                        "original_filename": uploaded_photo.name,
                        "status": "New",
                        "latitude": None, "longitude": None, "location_accuracy": None
                    }
                    if st.session_state.report_location:
                        report_data["latitude"] = st.session_state.report_location['latitude']
                        report_data["longitude"] = st.session_state.report_location['longitude']
                        report_data["location_accuracy"] = st.session_state.report_location['accuracy']

                    # 3. Insert metadata into Supabase
                    logger.info(f"Inserting metadata for report {report_id} into Supabase...")
                    inserted_id, db_error = insert_report(supabase_client, report_data)

                    if db_error:
                        st.error(f"Failed to save report metadata: {db_error}")
                        logger.warning("Attempting to delete orphaned image from storage...")
                        deleted, del_err = delete_photo(gcs_bucket, gcs_path)
                        if del_err: st.error(f"Failed to delete orphaned image: {del_err}")
                        else: st.info("Orphaned image deleted.")
                    else:
                        st.success(f"✅ Report submitted successfully! (ID: {inserted_id})")
                        # Clear state after successful submission
                        st.session_state.report_location = None
                        # Clearing file_uploader is tricky, rely on rerun
                        logger.info("Report submission successful, preparing rerun...")
                        st.experimental_rerun() # Use rerun to clear form state

            except Exception as e:
                 st.error(f"An unexpected error occurred during submission: {e}")
                 logger.error(f"Submission process error: {e}", exc_info=True)

    else:
        st.error("Backend services not initialized. Cannot submit.")


st.markdown("---")

# --- Admin Panel ---
st.sidebar.title("Admin Panel")
admin_password = st.sidebar.text_input("Enter Admin Password:", type="password", key="admin_pass")
DEMO_PASSWORD = "admin" # Keep simple for demo

if admin_password == DEMO_PASSWORD:
    if not supabase_client or not gcs_bucket:
         st.sidebar.error("Backend services not initialized.")
    else:
        st.sidebar.success("Access Granted")
        st.sidebar.subheader("Submitted Reports")

        # Fetch reports
        reports, fetch_error = fetch_reports(supabase_client)

        if fetch_error:
            st.sidebar.error(f"Error fetching reports: {fetch_error}")
        elif not reports:
            st.sidebar.info("No reports found.")
        else:
            report_data_display = []
            report_details = {} # Store full data dict by ID

            for report in reports:
                report_id = report["report_id"]
                report_details[report_id] = report # Store full dict

                # Format for display
                ts_dt = report.get("created_at_dt") # Use parsed datetime
                time_str = ts_dt.strftime('%Y-%m-%d %H:%M UTC') if isinstance(ts_dt, datetime.datetime) else report.get("created_at", "N/A")[:16]
                lat = report.get("latitude"); lon = report.get("longitude")
                loc_str = f"{lat:.4f}, {lon:.4f}" if lat is not None and lon is not None else "-"

                report_data_display.append({
                    "Report ID": report_id[:8], # Show partial ID
                    "Time": time_str,
                    "Status": report.get("status", "N/A"),
                    "Size": report.get("report_size", "N/A"),
                    "Type": report.get("report_type", "N/A"),
                    "Location": loc_str,
                    "_doc_id": report_id # Full ID for lookup
                })

            if report_data_display:
                 report_df = pd.DataFrame(report_data_display).set_index("_doc_id")
                 # Select columns to display in admin table
                 cols_to_display = ["Report ID", "Time", "Status", "Size", "Type", "Location"]
                 st.sidebar.dataframe(report_df[cols_to_display], use_container_width=True)

                 # Admin Actions
                 st.sidebar.subheader("Manage Report")
                 # Use full ID for selection options, but display partial? Or use partial? Let's use full ID internally.
                 report_ids_list = ["<Select Report>"] + list(report_details.keys())
                 # Display shorter IDs in dropdown for readability
                 display_id_map = {"<Select Report>": "<Select Report>"}
                 display_id_map.update({f"{rid[:8]}... ({report_details[rid].get('created_at_dt','').strftime('%H:%M') if report_details[rid].get('created_at_dt') else ''})": rid for rid in report_ids_list[1:]})

                 selected_display_id = st.sidebar.selectbox("Select Report:", list(display_id_map.keys()), key="admin_select_supabase")
                 selected_report_id = display_id_map[selected_display_id] # Get full ID

                 if selected_report_id != "<Select Report>":
                    selected_data = report_details.get(selected_report_id)
                    if selected_data:
                        st.sidebar.caption(f"Details for: {selected_report_id[:8]}...")
                        # Display details...
                        st.sidebar.text(f"Status: {selected_data.get('status')}")
                        st.sidebar.text(f"Size: {selected_data.get('report_size')}")
                        st.sidebar.text(f"Type: {selected_data.get('report_type')}")
                        lat = selected_data.get("latitude"); lon = selected_data.get("longitude")
                        loc_str = f"{lat:.5f}, {lon:.5f}" if lat is not None and lon is not None else "Not captured"
                        st.sidebar.text(f"Location: {loc_str}")
                        img_url = selected_data.get("image_url")
                        if img_url:
                             st.sidebar.image(img_url, caption=f"Image", use_column_width=True)
                        else:
                             st.sidebar.text("No image URL found.")

                        # Action Buttons
                        col1a, col2a = st.sidebar.columns(2)
                        current_status = selected_data.get('status', 'New')
                        status_options = ['New', 'Reviewed', 'Cleaned']
                        try: current_status_index = status_options.index(current_status)
                        except ValueError: current_status_index = 0 # Default to 'New' if status invalid

                        with col1a: # Update Status
                            new_status = st.selectbox("Set Status:", status_options, index=current_status_index, key=f"status_{selected_report_id}")
                            if st.button("Update Status", key=f"update_{selected_report_id}"):
                                if new_status != current_status:
                                    success, error = update_report_status(supabase_client, selected_report_id, new_status)
                                    if error: st.error(f"Update failed: {error}")
                                    else: st.success(f"Status updated to {new_status}."); time.sleep(0.5); st.experimental_rerun()
                                else: st.info("Status is already set to that value.")

                        with col2a: # Delete
                            # Add a confirmation step for delete
                            if f"confirm_delete_{selected_report_id}" not in st.session_state:
                                st.session_state[f"confirm_delete_{selected_report_id}"] = False

                            if st.session_state[f"confirm_delete_{selected_report_id}"]:
                                st.warning("Are you sure?")
                                if st.button("YES, DELETE", key=f"delete_confirm_{selected_report_id}"):
                                    with st.spinner("Deleting..."):
                                        logger.info(f"Attempting deletion for report {selected_report_id}...")
                                        meta_deleted, meta_err = delete_report_metadata(supabase_client, selected_report_id)
                                        if meta_err: st.error(f"Failed to delete DB record: {meta_err}")
                                        else:
                                            logger.info("DB record deleted. Attempting GCS delete...")
                                            gcs_path = selected_data.get("gcs_path")
                                            if gcs_path:
                                                photo_deleted, photo_err = delete_photo(gcs_bucket, gcs_path)
                                                if photo_err: st.error(f"Failed to delete image: {photo_err}")
                                                else: logger.info("Image deleted from storage.")
                                            else: st.warning("No GCS path in record.")
                                            st.success(f"Report deletion complete.")
                                            st.session_state[f"confirm_delete_{selected_report_id}"] = False # Reset confirm state
                                            time.sleep(1); st.experimental_rerun()
                                if st.button("Cancel", key=f"delete_cancel_{selected_report_id}"):
                                    st.session_state[f"confirm_delete_{selected_report_id}"] = False
                                    st.experimental_rerun()
                            else:
                                if st.button("🚨 Delete Report", key=f"delete_init_{selected_report_id}"):
                                    st.session_state[f"confirm_delete_{selected_report_id}"] = True
                                    st.experimental_rerun()

                    else:
                        st.sidebar.error("Selected report details not found (list might have refreshed).")


elif admin_password and admin_password != DEMO_PASSWORD:
    st.sidebar.error("Incorrect password.")

st.sidebar.markdown("---")
st.sidebar.info("TrashGuard")
st.sidebar.caption("SJ Data | Google Maps | Supabase DB | GCS Storage")