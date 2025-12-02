import streamlit as st
import requests
import json
import uuid
from datetime import datetime, timedelta, date
import pika
import base64

# -------------------------
# Load schema defaults
# -------------------------
with open("backfill.json", "r") as f:
    schema = json.load(f)

# Extract defaults for enrichment columns
vehicle_defaults = schema["enrich"][0]["select"].split(",")
people_defaults = schema["enrich"][1]["select"].split(",")

# -------------------------
# Helper: Fetch Socrata columns
# -------------------------
def get_socrata_columns(dataset_id, domain="data.cityofchicago.org", app_token=None):
    """Fetch column names from a Socrata dataset."""
    url = f"https://{domain}/api/views/{dataset_id}.json"
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    columns = [col['name'] for col in data['columns']]
    return columns

# -------------------------
# Streamlit Page UI
# -------------------------
st.title("📡 Data Fetcher")

# Load dynamic columns from Socrata
vehicles_cols = get_socrata_columns("68nd-jvt3")
people_cols = get_socrata_columns("u6pd-qa9d")

tabs = st.tabs(["Streaming", "Backfill"])

for tab_name, tab in zip(["streaming", "backfill"], tabs):
    with tab:
        st.subheader(f"{tab_name.title()} Mode")

        # CorrID (read-only, auto-generated)
        corr_id = st.text_input(
            "Correlation ID",
            value=f"corr_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            disabled=True,
            key=f"corr_id_{tab_name}"
        )

        # Time window controls
        if tab_name == "streaming":
            since_days = st.number_input(
                "Since days",
                min_value=1,
                max_value=365,
                value=30,
                key=f"since_days_{tab_name}"
            )
            date_range = {
                "field": "crash_date",
                "start": (date.today() - timedelta(days=since_days)).isoformat(),
                "end": date.today().isoformat()
            }
        else:  # backfill
            start_date = st.date_input(
                "Start Date",
                value=date.today() - timedelta(days=7),
                key=f"start_date_{tab_name}"
            )
            end_date = st.date_input(
                "End Date",
                value=date.today(),
                key=f"end_date_{tab_name}"
            )
            date_range = {
                "field": "crash_date",
                "start": start_date.strftime("%Y-%m-%dT00:00:00"),
                "end": end_date.strftime("%Y-%m-%dT00:00:00")
            }

        # Only keep defaults that actually exist in Socrata columns (case-insensitive)
        # Filter against Socrata columns if needed (case-insensitive)
        vehicles_cols_lower = [c.lower() for c in vehicles_cols]
        safe_vehicle_defaults = [col for col in vehicle_defaults if col.lower() in vehicles_cols_lower]

        people_cols_lower = [c.lower() for c in people_cols]
        safe_people_defaults = [col for col in people_defaults if col.lower() in people_cols_lower]

        vehicles_cols = [c.lower() for c in vehicles_cols]
        people_cols   = [c.lower() for c in people_cols]

        
        include_vehicles = st.checkbox(
            "✅ Include Vehicles",
            value=True,
            key=f"include_vehicles_{tab_name}"
        )

        selected_vehicle_cols = st.multiselect(
            "Vehicle columns",
            vehicles_cols,
            default=[c for c in vehicles_cols if c.lower() in [d.lower() for d in safe_vehicle_defaults]],
            key=f"vehicle_cols_{tab_name}"
        )

        include_people = st.checkbox(
            "✅ Include People",
            value=True,
            key=f"include_people_{tab_name}"
        )

        selected_people_cols = st.multiselect(
            "People columns",
            people_cols,
            default=[c for c in people_cols if c.lower() in [d.lower() for d in safe_people_defaults]],
            key=f"people_cols_{tab_name}"
        )

        

        # Build JSON payload
        payload = {
            "mode": tab_name,
            "source": "crash",
            "join_key": "crash_record_id",
            "date_range": date_range,
            "primary": schema["primary"],  # use directly from backfill.json
            "enrich": [],
            "batching": schema["batching"],
            "storage": schema["storage"]
        }

        if include_vehicles:
            payload["enrich"].append({
                "id": "68nd-jvt3",
                "alias": "vehicles",
                "select": ",".join(selected_vehicle_cols)
            })
        if include_people:
            payload["enrich"].append({
                "id": "u6pd-qa9d",
                "alias": "people",
                "select": ",".join(selected_people_cols)
            })

        # Preview JSON payload
        with st.expander("Preview JSON"):
            st.json(payload)

        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button(f"Publish {tab_name.title()} Job", key=f"publish_{tab_name}"):
                try:
                    # Encode payload as base64
                    payload_json = json.dumps(payload)
                    payload_b64 = base64.b64encode(payload_json.encode("utf-8")).decode("utf-8")

                    # RabbitMQ HTTP API URL
                    RABBIT_URL = "http://rabbitmq:15672/api/exchanges/%2F/amq.default/publish"

                    # Prepare POST data
                    data = {
                        "properties": {
                            "content_type": "application/json",
                            "delivery_mode": 2
                        },
                        "routing_key": "extract",
                        "payload": payload_b64,
                        "payload_encoding": "base64"
                    }

                    # Send POST request
                    resp = requests.post(RABBIT_URL, json=data, auth=("guest", "guest"))

                    if resp.ok:
                        st.success(f"{tab_name.title()} job published successfully!")
                    else:
                        st.error(f"Failed to publish job: {resp.status_code} {resp.text}")

                except Exception as e:
                    st.error(f"Failed to publish job: {e}")

        with col2:
            if st.button("Reset Form", key=f"reset_{tab_name}"):
                st.rerun()