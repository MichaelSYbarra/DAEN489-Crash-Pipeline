import streamlit as st
import pandas as pd
import joblib
import json
import os
import duckdb
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay, accuracy_score
import matplotlib.pyplot as plt
import io

# Promethus
from prometheus_client import Gauge, Summary
import time
import threading


# === Prometheus Metrics ===
import streamlit as st
from prometheus_client import Gauge, Summary


if "UPTIME_GAUGE" not in st.session_state:
    st.session_state["UPTIME_GAUGE"] = Gauge(
        "app_uptime_seconds", "Total uptime of the Streamlit app"
    )

if "ACCURACY_GAUGE" not in st.session_state:
    st.session_state["ACCURACY_GAUGE"] = Gauge(
        "model_accuracy", "Model accuracy on last run"
    )

if "PRECISION_GAUGE" not in st.session_state:
    st.session_state["PRECISION_GAUGE"] = Gauge(
        "model_precision", "Model precision on last run"
    )

if "RECALL_GAUGE" not in st.session_state:
    st.session_state["RECALL_GAUGE"] = Gauge(
        "model_recall", "Model recall on last run"
    )

if "LATENCY_SUMMARY" not in st.session_state:
    st.session_state["LATENCY_SUMMARY"] = Summary(
        "prediction_latency_seconds", "Prediction latency per batch"
    )



# Aliases for convenience
UPTIME_GAUGE = st.session_state["UPTIME_GAUGE"]
ACCURACY_GAUGE = st.session_state["ACCURACY_GAUGE"]
PRECISION_GAUGE = st.session_state["PRECISION_GAUGE"]
RECALL_GAUGE = st.session_state["RECALL_GAUGE"]
LATENCY_SUMMARY = st.session_state["LATENCY_SUMMARY"]


from sklearn.metrics import precision_score, recall_score, accuracy_score

# Update Prometheus metrics
#ACCURACY_GAUGE.set(accuracy)
#PRECISION_GAUGE.set(precision)
#RECALL_GAUGE.set(recall)





# ================================
# PAGE SETUP
# ================================
st.set_page_config(page_title="Model Predictions", layout="wide")
st.title("🚦 Crash Risk Prediction Tool")

MODEL_PATH = "artifacts/final_crash_model.pkl"
META_PATH = "artifacts/model_metadata.json"
GOLD_DB_PATH = "/data/gold/gold.duckdb"

   # ============================================================
# PERFORMANCE EVALUATION FUNCTION
# ============================================================
def run_performance_evaluation(results):
    st.markdown("---")
    st.header("📈 Model Performance Evaluation")

    # ---------------------------
    # CONFUSION MATRIX
    # ---------------------------
    st.subheader("🔢 Confusion Matrix")
    try:
        y_true = results["crash_type_binary"]
        y_pred = results["Predicted_Label"]

        cm = confusion_matrix(y_true, y_pred)
        disp = ConfusionMatrixDisplay(confusion_matrix=cm)

        fig, ax = plt.subplots()
        disp.plot(ax=ax, colorbar=False)
        st.pyplot(fig)

        st.write(f"**Accuracy:** {accuracy_score(y_true, y_pred):.3f}")

        accuracy = accuracy_score(y_true, y_pred)
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)

        # Update Prometheus metrics
        ACCURACY_GAUGE.set(accuracy)
        PRECISION_GAUGE.set(precision)
        RECALL_GAUGE.set(recall)

        # ---------------------------
        # DOWNLOAD BUTTON (PNG)
        # --------------------------
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)

        st.download_button(
            label="📥 Download Confusion Matrix (PNG)",
            data=buf,
            file_name="confusion_matrix.png",
            mime="image/png"
        )


    except Exception as e:
        st.error(f"⚠️ Could not compute confusion matrix: {e}")


def timed_predict(model, data):
    start = time.perf_counter()
    proba = model.predict_proba(data)[:, 1]
    latency = time.perf_counter() - start

    # Record to Prometheus
    LATENCY_SUMMARY.observe(latency)

    return proba, latency


# ================================
# LOADING FUNCTIONS
# ================================
@st.cache_resource
def load_model():
    if not os.path.exists(MODEL_PATH):
        st.error("❌ Model file not found. Please train and save it first.")
        st.stop()
    return joblib.load(MODEL_PATH)

@st.cache_resource
def load_metadata():
    if os.path.exists(META_PATH):
        with open(META_PATH, "r") as f:
            return json.load(f)
    return {"threshold": 0.5, "model_name": "RandomForestClassifier"}

@st.cache_resource
def load_gold_tables():
    if not os.path.exists(GOLD_DB_PATH):
        st.error("❌ Gold database not found.")
        return []
    conn = duckdb.connect(GOLD_DB_PATH, read_only=True)
    tables = conn.execute("SHOW TABLES").df()['name'].tolist()
    conn.close()
    return tables

@st.cache_resource
def load_gold_table(table_name):
    conn = duckdb.connect(GOLD_DB_PATH, read_only=True)
    df = conn.execute(f"SELECT * FROM {table_name}").df()
    conn.close()
    return df


model = load_model()
metadata = load_metadata()
threshold = metadata.get("threshold", 0.5)

# ================================
# SIDEBAR
# ================================
st.sidebar.header("⚙️ Model Info")
st.sidebar.write(f"**Model:** {metadata.get('model_name', 'RandomForestClassifier')}")
st.sidebar.write(f"**Decision threshold:** {threshold}")
st.sidebar.write(f"**Trained on:** {metadata.get('trained_on', 'N/A')}")
st.sidebar.divider()

# ================================
# TABS
# ================================
tab_overview, tab_upload, tab_gold = st.tabs(
    ["📘 Model Overview", "📂 Upload CSV", "🪙 Load From Gold Table"]
)

# ================================
# TAB 1 — MODEL OVERVIEW
# ================================
with tab_overview:
    st.header("📘 Model Overview")
    st.write("Below are the saved model metadata details:")

    st.json(metadata)

    st.markdown("---")
    st.subheader("🔍 Feature Importance (if available)")
    if hasattr(model, "feature_importances_"):
        try:
            fi = pd.DataFrame({
                "feature": metadata.get("feature_names", []),
                "importance": model.feature_importances_
            })
            st.dataframe(fi.sort_values("importance", ascending=False))
        except:
            st.info("Feature list unavailable.")
    else:
        st.info("Model does not expose `feature_importances_`.")


# ================================
# TAB 2 — UPLOAD CSV FOR PREDICTIONS
# ================================
with tab_upload:
    st.header("📂 Upload Data for Prediction")

    uploaded_file = st.file_uploader(
        "Upload a CSV file with the same columns used in training", 
        type=["csv"]
    )

    if uploaded_file:
        data = pd.read_csv(uploaded_file)
        st.success(f"✅ Loaded {data.shape[0]} rows, {data.shape[1]} columns")
        st.dataframe(data.head(), use_container_width=True)

        if st.button("🚀 Run Model Predictions", key="run_from_upload"):
            with st.spinner("Running model..."):
                proba = model.predict_proba(data)[:, 1]
                preds = (proba >= threshold).astype(int)

            results = data.copy()
            results["Predicted_Probability"] = proba
            results["Predicted_Label"] = preds

            

            st.subheader("📊 Prediction Results")
            st.dataframe(results.head(20), use_container_width=True)

            st.metric("Average predicted probability", f"{proba.mean():.3f}")
            st.metric("Predicted positive rate", f"{preds.mean():.2%}")

            run_performance_evaluation(results)

            csv = results.to_csv(index=False)
            st.download_button("💾 Download predictions as CSV", csv, "predictions.csv")



    else:
        st.info("👆 Upload a CSV file above to generate predictions.")


# ================================
# TAB 3 — GOLD TABLE + TEST DATA + UPLOAD OPTIONS
# ================================
# ================================
# TAB — GOLD TABLE SCORING ONLY
# ================================
with tab_gold:
    st.header("🪙 Gold Table Scoring")

    # Load available gold tables
    tables = load_gold_tables()

    if len(tables) == 0:
        st.error("❌ No gold tables found in database.")
        st.stop()

    # Select which gold table to load
    table_choice = st.selectbox("Select a gold table:", tables)

    # === DATE FILTERS ===
    st.markdown("### 📅 Filter by Date Range")
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input("Start date (optional)", None)
    with col2:
        end_date = st.date_input("End date (optional)", None)

    # === MAX ROWS ===
    st.markdown("### 🔢 Maximum Rows to Load")
    max_rows = st.number_input(
        "Maximum number of rows",
        min_value=100,
        max_value=250_000,
        value=5_000,
        step=500
    )

    # === FULL RESULT vs SAMPLE ===
    load_mode = st.radio(
        "Load mode:",
        ["Full Result", "Random Sample (after filtering)"]
    )

    # === LOAD BUTTON ===
    if st.button("📥 Load Data From Gold Table"):
        query = f"SELECT * FROM {table_choice}"

        conditions = []
        if start_date:
            conditions.append(f"crash_date >= '{start_date}'")
        if end_date:
            conditions.append(f"crash_date <= '{end_date}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" LIMIT {max_rows}"

        conn = duckdb.connect(GOLD_DB_PATH, read_only=True)
        df = conn.execute(query).df()
        conn.close()

        # Optional sampling
        if load_mode == "Random Sample (after filtering)" and len(df) > 0:
            df = df.sample(min(max_rows, len(df)), random_state=42)

        st.success(f"Loaded {len(df)} rows from `{table_choice}`.")
        st.dataframe(df.head(), use_container_width=True)

        # Save for prediction
        st.session_state["gold_df"] = df

    # ===========================================
    # RUN MODEL PREDICTIONS
    # ===========================================
    st.markdown("---")

    # ---------------------------
    # CATEGORY SLICE PERFORMANCE
    # ---------------------------
    st.markdown("---")
    st.subheader("📊 Performance by Category Slices")


    if 'results' in locals():
        # Categorical columns you want to slice on
        categorical_columns = ["lighting_condition"]  # you can add more later

        for col in categorical_columns:
            st.write(f"### 🔍 Category: **{col}**")

            if col not in results.columns:
                st.warning(f"⚠️ Column '{col}' not found — skipping.")
                continue

            categories = results[col].unique()

            for cat in categories:
                st.write(f"#### ➤ Slice = `{cat}`")

                slice_df = results[results[col] == cat]

                # Safety check for empty slice
                if slice_df.empty:
                    st.info(f"⚠️ No rows found for {col} = {cat}, skipping.")
                    continue

                try:
                    y_true_slice = slice_df["crash_type_binary"]
                    y_pred_slice = slice_df["Predicted_Label"]

                    cm_slice = confusion_matrix(y_true_slice, y_pred_slice)
                    disp_slice = ConfusionMatrixDisplay(confusion_matrix=cm_slice)

                    fig2, ax2 = plt.subplots()
                    disp_slice.plot(ax=ax2, colorbar=False)
                    ax2.set_title(f"{col}: {cat}")
                    st.pyplot(fig2)

                    st.write(f"**Accuracy:** {accuracy_score(y_true_slice, y_pred_slice):.3f}")

                except Exception as e:
                    st.warning(f"⚠️ Could not compute slice for {col}={cat}: {e}")

        st.success("✅ Finished generating slice analysis.")


    # ============================================================
    # RUN MODEL PREDICTIONS SECTION
    # ============================================================
    st.markdown("---")

    if "gold_df" in st.session_state:
        df = st.session_state["gold_df"]

        if st.button("🚀 Run Model Predictions"):
            with st.spinner("Running model..."):
                # Make predictions

                proba, latency = timed_predict(model, df)

                #proba = model.predict_proba(df)[:, 1]
                preds = (proba >= threshold).astype(int)


            # Attach predictions to dataframe
            results = df.copy()
            results["Predicted_Probability"] = proba
            results["Predicted_Label"] = preds

            # Display results
            st.subheader("📊 Prediction Results")
            st.dataframe(results.head(20), use_container_width=True)

            st.metric("Average predicted probability", f"{proba.mean():.3f}")
            st.metric("Predicted positive rate", f"{preds.mean():.2%}")

            # Download predictions
            csv = results.to_csv(index=False)
            st.download_button(
                label="💾 Download predictions as CSV",
                data=csv,
                file_name="predictions.csv",
                mime="text/csv"
            )

            # Run Performance Evaluation
            run_performance_evaluation(results)

    else:
        st.info("Please load your gold dataset in the other tab first.")
