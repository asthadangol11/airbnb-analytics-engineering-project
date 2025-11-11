import os, re, logging, shutil, numpy as np, pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "BDE_ASM_3",
    "start_date": datetime.now() - timedelta(days=6),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="airbnb_load_next_month",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    description="Load exactly one Airbnb month (earliest by filename) per run.",
)

AIRFLOW_DATA = "/home/airflow/gcs/data"
AIRBNB_DIR   = os.path.join(AIRFLOW_DATA, "airbnb")
ARCHIVE_DIR  = os.path.join(AIRBNB_DIR, "archive")
FNAME_RE     = re.compile(r"(?P<mm>\d{2})_(?P<yyyy>\d{4})\.csv$", re.IGNORECASE)

def _list_month_files_sorted():
    files = []
    for f in os.listdir(AIRBNB_DIR):
        if not f.lower().endswith(".csv"): continue
        m = FNAME_RE.search(f)
        if not m: continue
        files.append(((int(m.group("yyyy")), int(m.group("mm"))), f))
    files.sort()
    return [f for _, f in files]

def choose_next_file(**context):
    candidates = _list_month_files_sorted()
    if not candidates:
        logging.info("No monthly CSVs found in %s.", AIRBNB_DIR)
        raise AirflowSkipException("No files left.")
    context["ti"].xcom_push(key="next_file", value=candidates[0])
    logging.info("Next month selected: %s", candidates[0])

def load_one_month(**context):
    next_file = context["ti"].xcom_pull(key="next_file")
    if not next_file: raise AirflowSkipException("No file selected.")
    path = os.path.join(AIRBNB_DIR, next_file)
    if not os.path.exists(path): raise AirflowSkipException(f"Missing: {path}")

    # Figure out the month from the filename (MM_YYYY.csv)
    m = FNAME_RE.search(next_file)
    file_mm = int(m.group("mm")) if m else None

    df = pd.read_csv(path)
    df.columns = [c.strip().upper() for c in df.columns]

    # --- Minimal, robust date parsing that locks month to the filename's MM
    def parse_scraped(series, mm_expected: int):
        s = series.astype(str).str.strip().replace({"": np.nan, "NaT": np.nan, "NULL": np.nan})
        # Parse both ways
        d_dayfirst  = pd.to_datetime(s, errors="coerce", dayfirst=True)
        d_monthfirst= pd.to_datetime(s, errors="coerce", dayfirst=False)
        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

        if mm_expected:
            pick_d1 = (d_dayfirst.notna())   & (d_dayfirst.dt.month  == mm_expected)
            pick_d2 = (d_monthfirst.notna()) & (d_monthfirst.dt.month == mm_expected)
            out.loc[pick_d1] = d_dayfirst.loc[pick_d1]
            out.loc[~pick_d1 & pick_d2] = d_monthfirst.loc[~pick_d1 & pick_d2]

        # Any leftovers: try strict ISO
        still = out.isna()
        if still.any():
            iso_try = pd.to_datetime(s.loc[still], errors="coerce", format="%Y-%m-%d")
            out.loc[still & iso_try.notna()] = iso_try
        return out

    def parse_host_since(series):
        s = series.astype(str).str.strip().replace({"": np.nan, "NaT": np.nan, "NULL": np.nan})
        d = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if d.isna().any():
            d = d.fillna(pd.to_datetime(s, errors="coerce", format="%Y-%m-%d"))
        return d

    df["SCRAPED_DATE"] = parse_scraped(df["SCRAPED_DATE"], file_mm)
    df["HOST_SINCE"]   = parse_host_since(df["HOST_SINCE"])

    df["HOST_NAME"]          = df["HOST_NAME"].fillna("Unknown")
    df["HOST_SINCE"]         = df["HOST_SINCE"].fillna(pd.Timestamp("2000-01-01"))
    df["HOST_IS_SUPERHOST"]  = df["HOST_IS_SUPERHOST"].fillna("No")
    df["HOST_NEIGHBOURHOOD"] = df["HOST_NEIGHBOURHOOD"].fillna("Not specified")
    for c in ["REVIEW_SCORES_RATING","REVIEW_SCORES_ACCURACY","REVIEW_SCORES_CLEANLINESS",
              "REVIEW_SCORES_CHECKIN","REVIEW_SCORES_COMMUNICATION","REVIEW_SCORES_VALUE"]:
        df[c] = df[c].fillna(0)

    before = len(df)
    df = df[df["SCRAPED_DATE"].notna()].copy()
    logging.info("Dropped %s rows with invalid SCRAPED_DATE.", before - len(df))

    df["SCRAPED_DATE"] = df["SCRAPED_DATE"].dt.date
    df["HOST_SINCE"]   = df["HOST_SINCE"].dt.date

    col_names = [
        "LISTING_ID","SCRAPE_ID","SCRAPED_DATE","HOST_ID","HOST_NAME","HOST_SINCE","HOST_IS_SUPERHOST",
        "HOST_NEIGHBOURHOOD","LISTING_NEIGHBOURHOOD","PROPERTY_TYPE","ROOM_TYPE","ACCOMMODATES","PRICE",
        "HAS_AVAILABILITY","AVAILABILITY_30","NUMBER_OF_REVIEWS","REVIEW_SCORES_RATING","REVIEW_SCORES_ACCURACY",
        "REVIEW_SCORES_CLEANLINESS","REVIEW_SCORES_CHECKIN","REVIEW_SCORES_COMMUNICATION","REVIEW_SCORES_VALUE"
    ]
    safe_df = df[col_names].replace({pd.NaT: None, np.nan: None})
    rows = [tuple(x.values()) for x in safe_df.to_dict("records")]
    if not rows: raise ValueError(f"No loadable rows in {next_file} after cleaning.")

    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    insert_sql = """
        INSERT INTO bronze.airbnb_raw (
            listing_id, scrape_id, scraped_date, host_id, host_name, host_since, host_is_superhost,
            host_neighbourhood, listing_neighbourhood, property_type, room_type, accommodates, price,
            has_availability, availability_30, number_of_reviews, review_scores_rating, review_scores_accuracy,
            review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_value
        ) VALUES %s
        ON CONFLICT (listing_id, scraped_date) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=10000)
    conn.commit()

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    shutil.move(path, os.path.join(ARCHIVE_DIR, next_file))
    logging.info("Archived %s to %s", next_file, ARCHIVE_DIR)

choose_next = PythonOperator(
    task_id="choose_next_month_file",
    python_callable=choose_next_file,
    provide_context=True,
    dag=dag,
)

load_month = PythonOperator(
    task_id="load_selected_month",
    python_callable=load_one_month,
    provide_context=True,
    dag=dag,
)

choose_next >> load_month
