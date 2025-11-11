import os
import logging
import shutil
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException   # <-- added

# -----------------------------
# DAG defaults
# -----------------------------
dag_default_args = {
    'owner': 'BDE_ASM_3',
    'start_date': datetime.now() - timedelta(days=6),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='as-3-final',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5,
)

# -----------------------------
# Paths
# -----------------------------
AIRFLOW_DATA = "/home/airflow/gcs/data"
AIRBNB_DATA = os.path.join(AIRFLOW_DATA, "airbnb")
CENSUS_DATA = os.path.join(AIRFLOW_DATA, "census")
LGA_MAPPING_DATA = os.path.join(AIRFLOW_DATA, "mapping")

def archive_files(file_list, src_folder, archive_folder):
    os.makedirs(archive_folder, exist_ok=True)
    for file_name in file_list:
        shutil.move(os.path.join(src_folder, file_name),
                    os.path.join(archive_folder, file_name))

def _truncate(conn, table_fq):
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_fq};")
    conn.commit()
    logging.info("Truncated table %s", table_fq)

# ---- NEW: robust date parsing helper (small & focused) ----
def _parse_date_series(s: pd.Series) -> pd.Series:
    """
    Try dd/mm/yyyy, then yyyy-mm-dd, then a generic fallback.
    Returns datetime64[ns] with NaT where parsing fails.
    """
    out = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
    m = out.isna()
    if m.any():
        out.loc[m] = pd.to_datetime(s.loc[m], format="%Y-%m-%d", errors="coerce")
    m = out.isna()
    if m.any():
        out.loc[m] = pd.to_datetime(s.loc[m], errors="coerce")
    return out

# -----------------------------
# Load Airbnb (append, keep history, dedupe by (listing_id, scraped_date))
# -----------------------------
def import_load_airbnb_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()

    filelist = [f for f in os.listdir(AIRBNB_DATA) if f.lower().endswith(".csv")]
    if not filelist:
        logging.info("No CSV files found in AIRBNB_DATA.")
        return

    # Read all CSVs currently present
    frames = [pd.read_csv(os.path.join(AIRBNB_DATA, f)) for f in filelist]
    df = pd.concat(frames, ignore_index=True)

    # --- Cleaning & typing
    df['HOST_NAME'] = df['HOST_NAME'].fillna("Unknown")

    # Parse dates (now tolerant)
    df['SCRAPED_DATE'] = _parse_date_series(df['SCRAPED_DATE'])
    df['HOST_SINCE']   = _parse_date_series(df['HOST_SINCE'])

    # Default for HOST_SINCE; keep SCRAPED_DATE as parsed (we'll drop rows where it's missing)
    df['HOST_SINCE'] = df['HOST_SINCE'].fillna(pd.Timestamp('2000-01-01'))

    df['HOST_IS_SUPERHOST']  = df['HOST_IS_SUPERHOST'].fillna("No")
    df['HOST_NEIGHBOURHOOD'] = df['HOST_NEIGHBOURHOOD'].fillna("Not specified")

    review_cols = [
        "REVIEW_SCORES_RATING", "REVIEW_SCORES_ACCURACY", "REVIEW_SCORES_CLEANLINESS",
        "REVIEW_SCORES_CHECKIN", "REVIEW_SCORES_COMMUNICATION", "REVIEW_SCORES_VALUE"
    ]
    df[review_cols] = df[review_cols].fillna(0)

    # Drop rows that don't have a valid scraped_date
    before = len(df)
    df = df[df['SCRAPED_DATE'].notna()].copy()
    dropped = before - len(df)
    logging.info("Dropped %s rows with missing/invalid SCRAPED_DATE.", dropped)

    # If empty after parsing, don't archive (skip task)
    if df.empty:
        logging.warning("No valid Airbnb rows to insert. Not archiving source files.")
        raise AirflowSkipException("Airbnb: no rows after date parsing")

    # If your DB column is DATE, cast to date
    df['SCRAPED_DATE'] = df['SCRAPED_DATE'].dt.date
    df['HOST_SINCE']   = df['HOST_SINCE'].dt.date

    col_names = [
        "LISTING_ID","SCRAPE_ID","SCRAPED_DATE","HOST_ID","HOST_NAME","HOST_SINCE","HOST_IS_SUPERHOST",
        "HOST_NEIGHBOURHOOD","LISTING_NEIGHBOURHOOD","PROPERTY_TYPE","ROOM_TYPE","ACCOMMODATES","PRICE",
        "HAS_AVAILABILITY","AVAILABILITY_30","NUMBER_OF_REVIEWS","REVIEW_SCORES_RATING","REVIEW_SCORES_ACCURACY",
        "REVIEW_SCORES_CLEANLINESS","REVIEW_SCORES_CHECKIN","REVIEW_SCORES_COMMUNICATION","REVIEW_SCORES_VALUE"
    ]

    # Replace NaT/NaN with None -> SQL NULL
    safe_df = df[col_names].replace({pd.NaT: None, np.nan: None})
    rows = [tuple(x.values()) for x in safe_df.to_dict('records')]
    logging.info("Preparing to insert %s rows into bronze.airbnb_raw", len(rows))

    insert_sql = """
        INSERT INTO bronze.airbnb_raw (
            listing_id, scrape_id, scraped_date, host_id, host_name, host_since, host_is_superhost,
            host_neighbourhood, listing_neighbourhood, property_type, room_type, accommodates, price,
            has_availability, availability_30, number_of_reviews, review_scores_rating, review_scores_accuracy,
            review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_value
        )
        VALUES %s
        ON CONFLICT (listing_id, scraped_date) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=10000)
    conn.commit()

    archive_files(filelist, AIRBNB_DATA, os.path.join(AIRBNB_DATA, "archive"))
    logging.info("Archived %d files.", len(filelist))

# -----------------------------
# Load Census G01 (truncate then load)
# -----------------------------
def import_load_census_g01_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()

    census_files = [f for f in os.listdir(CENSUS_DATA) if f.lower().endswith(".csv") and "G01" in f]
    if not census_files:
        logging.info("No G01 census files.")
        return

    df = pd.concat([pd.read_csv(os.path.join(CENSUS_DATA, f)) for f in census_files], ignore_index=True)

    col_names = [
        "LGA_CODE_2016","Tot_P_M","Tot_P_F","Tot_P_P","Age_0_4_yr_M","Age_0_4_yr_F","Age_0_4_yr_P",
        "Age_5_14_yr_M","Age_5_14_yr_F","Age_5_14_yr_P","Age_15_19_yr_M","Age_15_19_yr_F","Age_15_19_yr_P",
        "Age_20_24_yr_M","Age_20_24_yr_F","Age_20_24_yr_P","Age_25_34_yr_M","Age_25_34_yr_F","Age_25_34_yr_P",
        "Age_35_44_yr_M","Age_35_44_yr_F","Age_35_44_yr_P","Age_45_54_yr_M","Age_45_54_yr_F","Age_45_54_yr_P",
        "Age_55_64_yr_M","Age_55_64_yr_F","Age_55_64_yr_P","Age_65_74_yr_M","Age_65_74_yr_F","Age_65_74_yr_P",
        "Age_75_84_yr_M","Age_75_84_yr_F","Age_75_84_yr_P","Age_85ov_M","Age_85ov_F","Age_85ov_P",
        "Counted_Census_Night_home_M","Counted_Census_Night_home_F","Counted_Census_Night_home_P",
        "Count_Census_Nt_Ewhere_Aust_M","Count_Census_Nt_Ewhere_Aust_F","Count_Census_Nt_Ewhere_Aust_P",
        "Indigenous_psns_Aboriginal_M","Indigenous_psns_Aboriginal_F","Indigenous_psns_Aboriginal_P",
        "Indig_psns_Torres_Strait_Is_M","Indig_psns_Torres_Strait_Is_F","Indig_psns_Torres_Strait_Is_P",
        "Indig_Bth_Abor_Torres_St_Is_M","Indig_Bth_Abor_Torres_St_Is_F","Indig_Bth_Abor_Torres_St_Is_P",
        "Indigenous_P_Tot_M","Indigenous_P_Tot_F","Indigenous_P_Tot_P","Birthplace_Australia_M",
        "Birthplace_Australia_F","Birthplace_Australia_P","Birthplace_Elsewhere_M","Birthplace_Elsewhere_F",
        "Birthplace_Elsewhere_P","Lang_spoken_home_Eng_only_M","Lang_spoken_home_Eng_only_F",
        "Lang_spoken_home_Eng_only_P","Lang_spoken_home_Oth_Lang_M","Lang_spoken_home_Oth_Lang_F",
        "Lang_spoken_home_Oth_Lang_P","Australian_citizen_M","Australian_citizen_F","Australian_citizen_P",
        "Age_psns_att_educ_inst_0_4_M","Age_psns_att_educ_inst_0_4_F","Age_psns_att_educ_inst_0_4_P",
        "Age_psns_att_educ_inst_5_14_M","Age_psns_att_educ_inst_5_14_F","Age_psns_att_educ_inst_5_14_P",
        "Age_psns_att_edu_inst_15_19_M","Age_psns_att_edu_inst_15_19_F","Age_psns_att_edu_inst_15_19_P",
        "Age_psns_att_edu_inst_20_24_M","Age_psns_att_edu_inst_20_24_F","Age_psns_att_edu_inst_20_24_P",
        "Age_psns_att_edu_inst_25_ov_M","Age_psns_att_edu_inst_25_ov_F","Age_psns_att_edu_inst_25_ov_P",
        "High_yr_schl_comp_Yr_12_eq_M","High_yr_schl_comp_Yr_12_eq_F","High_yr_schl_comp_Yr_12_eq_P",
        "High_yr_schl_comp_Yr_11_eq_M","High_yr_schl_comp_Yr_11_eq_F","High_yr_schl_comp_Yr_11_eq_P",
        "High_yr_schl_comp_Yr_10_eq_M","High_yr_schl_comp_Yr_10_eq_F","High_yr_schl_comp_Yr_10_eq_P",
        "High_yr_schl_comp_Yr_9_eq_M","High_yr_schl_comp_Yr_9_eq_F","High_yr_schl_comp_Yr_9_eq_P",
        "High_yr_schl_comp_Yr_8_belw_M","High_yr_schl_comp_Yr_8_belw_F","High_yr_schl_comp_Yr_8_belw_P",
        "High_yr_schl_comp_D_n_g_sch_M","High_yr_schl_comp_D_n_g_sch_F","High_yr_schl_comp_D_n_g_sch_P",
        "Count_psns_occ_priv_dwgs_M","Count_psns_occ_priv_dwgs_F","Count_psns_occ_priv_dwgs_P",
        "Count_Persons_other_dwgs_M","Count_Persons_other_dwgs_F","Count_Persons_other_dwgs_P"
    ]
    tuples = [tuple(x) for x in df[col_names].to_dict('split')['data']]

    _truncate(conn, "bronze.census_g01")

    insert_sql = """
        INSERT INTO bronze.census_g01 (
            LGA_CODE_2016, Tot_P_M, Tot_P_F, Tot_P_P, Age_0_4_yr_M, Age_0_4_yr_F, Age_0_4_yr_P,
            Age_5_14_yr_M, Age_5_14_yr_F, Age_5_14_yr_P, Age_15_19_yr_M, Age_15_19_yr_F, Age_15_19_yr_P,
            Age_20_24_yr_M, Age_20_24_yr_F, Age_20_24_yr_P, Age_25_34_yr_M, Age_25_34_yr_F, Age_25_34_yr_P,
            Age_35_44_yr_M, Age_35_44_yr_F, Age_35_44_yr_P, Age_45_54_yr_M, Age_45_54_yr_F, Age_45_54_yr_P,
            Age_55_64_yr_M, Age_55_64_yr_F, Age_55_64_yr_P, Age_65_74_yr_M, Age_65_74_yr_F, Age_65_74_yr_P,
            Age_75_84_yr_M, Age_75_84_yr_F, Age_75_84_yr_P, Age_85ov_M, Age_85ov_F, Age_85ov_P,
            Counted_Census_Night_home_M, Counted_Census_Night_home_F, Counted_Census_Night_home_P,
            Count_Census_Nt_Ewhere_Aust_M, Count_Census_Nt_Ewhere_Aust_F, Count_Census_Nt_Ewhere_Aust_P,
            Indigenous_psns_Aboriginal_M, Indigenous_psns_Aboriginal_F, Indigenous_psns_Aboriginal_P,
            Indig_psns_Torres_Strait_Is_M, Indig_psns_Torres_Strait_Is_F, Indig_psns_Torres_Strait_Is_P,
            Indig_Bth_Abor_Torres_St_Is_M, Indig_Bth_Abor_Torres_St_Is_F, Indig_Bth_Abor_Torres_St_Is_P,
            Indigenous_P_Tot_M, Indigenous_P_Tot_F, Indigenous_P_Tot_P, Birthplace_Australia_M,
            Birthplace_Australia_F, Birthplace_Australia_P, Birthplace_Elsewhere_M, Birthplace_Elsewhere_F,
            Birthplace_Elsewhere_P, Lang_spoken_home_Eng_only_M, Lang_spoken_home_Eng_only_F,
            Lang_spoken_home_Eng_only_P, Lang_spoken_home_Oth_Lang_M, Lang_spoken_home_Oth_Lang_F,
            Lang_spoken_home_Oth_Lang_P, Australian_citizen_M, Australian_citizen_F, Australian_citizen_P,
            Age_psns_att_educ_inst_0_4_M, Age_psns_att_educ_inst_0_4_F, Age_psns_att_educ_inst_0_4_P,
            Age_psns_att_educ_inst_5_14_M, Age_psns_att_educ_inst_5_14_F, Age_psns_att_educ_inst_5_14_P,
            Age_psns_att_edu_inst_15_19_M, Age_psns_att_edu_inst_15_19_F, Age_psns_att_edu_inst_15_19_P,
            Age_psns_att_edu_inst_20_24_M, Age_psns_att_edu_inst_20_24_F, Age_psns_att_edu_inst_20_24_P,
            Age_psns_att_edu_inst_25_ov_M, Age_psns_att_edu_inst_25_ov_F, Age_psns_att_edu_inst_25_ov_P,
            High_yr_schl_comp_Yr_12_eq_M, High_yr_schl_comp_Yr_12_eq_F, High_yr_schl_comp_Yr_12_eq_P,
            High_yr_schl_comp_Yr_11_eq_M, High_yr_schl_comp_Yr_11_eq_F, High_yr_schl_comp_Yr_11_eq_P,
            High_yr_schl_comp_Yr_10_eq_M, High_yr_schl_comp_Yr_10_eq_F, High_yr_schl_comp_Yr_10_eq_P,
            High_yr_schl_comp_Yr_9_eq_M, High_yr_schl_comp_Yr_9_eq_F, High_yr_schl_comp_Yr_9_eq_P,
            High_yr_schl_comp_Yr_8_belw_M, High_yr_schl_comp_Yr_8_belw_F, High_yr_schl_comp_Yr_8_belw_P,
            High_yr_schl_comp_D_n_g_sch_M, High_yr_schl_comp_D_n_g_sch_F, High_yr_schl_comp_D_n_g_sch_P,
            Count_psns_occ_priv_dwgs_M, Count_psns_occ_priv_dwgs_F, Count_psns_occ_priv_dwgs_P,
            Count_Persons_other_dwgs_M, Count_Persons_other_dwgs_F, Count_Persons_other_dwgs_P
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, tuples)
    conn.commit()

    archive_files(census_files, CENSUS_DATA, os.path.join(CENSUS_DATA, "archive"))

# -----------------------------
# Load Census G02 (truncate then load)
# -----------------------------
def import_load_census_g02_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()

    census_files = [f for f in os.listdir(CENSUS_DATA) if f.lower().endswith(".csv") and "G02" in f]
    if not census_files:
        logging.info("No G02 census files.")
        return

    df = pd.concat([pd.read_csv(os.path.join(CENSUS_DATA, f)) for f in census_files], ignore_index=True)

    col_names = [
        "LGA_CODE_2016","Median_age_persons","Median_mortgage_repay_monthly","Median_tot_prsnl_inc_weekly",
        "Median_rent_weekly","Median_tot_fam_inc_weekly","Average_num_psns_per_bedroom",
        "Median_tot_hhd_inc_weekly","Average_household_size"
    ]
    tuples = [tuple(x) for x in df[col_names].to_dict('split')['data']]

    _truncate(conn, "bronze.census_g02")

    insert_sql = """
        INSERT INTO bronze.census_g02 (
            LGA_CODE_2016, Median_age_persons, Median_mortgage_repay_monthly, Median_tot_prsnl_inc_weekly,
            Median_rent_weekly, Median_tot_fam_inc_weekly, Average_num_psns_per_bedroom,
            Median_tot_hhd_inc_weekly, Average_household_size
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, tuples)
    conn.commit()

    archive_files(census_files, CENSUS_DATA, os.path.join(CENSUS_DATA, "archive"))

# -----------------------------
# Load LGA Suburb (truncate then load)
# -----------------------------
def import_load_lga_suburb_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()

    lga_files = [f for f in os.listdir(LGA_MAPPING_DATA) if f.lower().endswith(".csv") and "SUBURB" in f]
    if not lga_files:
        logging.info("No LGA SUBURB files.")
        return

    df = pd.concat([pd.read_csv(os.path.join(LGA_MAPPING_DATA, f)) for f in lga_files], ignore_index=True)

    col_names = ["LGA_NAME","SUBURB_NAME"]
    tuples = [tuple(x) for x in df[col_names].to_dict('split')['data']]

    _truncate(conn, "bronze.nsw_lga_suburb")

    insert_sql = "INSERT INTO bronze.nsw_lga_suburb (LGA_NAME, SUBURB_NAME) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, tuples)
    conn.commit()

    archive_files(lga_files, LGA_MAPPING_DATA, os.path.join(LGA_MAPPING_DATA, "archive"))

# -----------------------------
# Load LGA Code (truncate then load)
# -----------------------------
def import_load_lga_code_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()

    lga_files = [f for f in os.listdir(LGA_MAPPING_DATA) if f.lower().endswith(".csv") and "CODE" in f]
    if not lga_files:
        logging.info("No LGA CODE files.")
        return

    df = pd.concat([pd.read_csv(os.path.join(LGA_MAPPING_DATA, f)) for f in lga_files], ignore_index=True)

    col_names = ["LGA_CODE","LGA_NAME"]
    tuples = [tuple(x) for x in df[col_names].to_dict('split')['data']]

    _truncate(conn, "bronze.nsw_lga_code")

    insert_sql = "INSERT INTO bronze.nsw_lga_code (LGA_CODE, LGA_NAME) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, tuples)
    conn.commit()

    archive_files(lga_files, LGA_MAPPING_DATA, os.path.join(LGA_MAPPING_DATA, "archive"))

# -----------------------------
# Tasks & dependencies
# -----------------------------
import_load_airbnb_task = PythonOperator(
    task_id="import_load_airbnb",
    python_callable=import_load_airbnb_func,
    provide_context=True,
    dag=dag,
)

import_load_census_g01_task = PythonOperator(
    task_id="import_load_census_g01",
    python_callable=import_load_census_g01_func,
    provide_context=True,
    dag=dag,
)

import_load_census_g02_task = PythonOperator(
    task_id="import_load_census_g02",
    python_callable=import_load_census_g02_func,
    provide_context=True,
    dag=dag,
)

import_load_lga_suburb_task = PythonOperator(
    task_id="import_load_lga_suburb",
    python_callable=import_load_lga_suburb_func,
    provide_context=True,
    dag=dag,
)

import_load_lga_code_task = PythonOperator(
    task_id="import_load_lga_code",
    python_callable=import_load_lga_code_func,
    provide_context=True,
    dag=dag,
)

# Flow
import_load_airbnb_task >> [import_load_census_g01_task, import_load_census_g02_task]
import_load_census_g01_task >> import_load_lga_suburb_task
import_load_census_g02_task >> import_load_lga_suburb_task
import_load_lga_suburb_task >> import_load_lga_code_task
