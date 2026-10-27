import os
import glob
import re
import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime as dt


def clean_datetime_and_time(val):
    """ฟังก์ชันตัดเวลา 0:00 หรือ 00:00:00 ออกจากทั้งข้อความ และ Datetime object"""
    if pd.isna(val):
        return val

    if isinstance(val, (pd.Timestamp, datetime.datetime, datetime.date)):
        return val.strftime('%d/%m/%Y')

    if isinstance(val, str):
        return re.sub(r'(\b\d{1,4}[/-]\d{1,2}[/-]\d{1,4})\s+\d{1,2}:\d{2}(:\d{2})?\b', r'\1', val)

    return val


def export_best_sheet_to_csv(file_path, output_folder, seen_jobs):
    try:
        base_name = os.path.basename(file_path)
        job_number = base_name.split(" ")[0]

        # -----------------------------------------------------------
        # ตรวจสอบว่า job_number นี้เคยเจอแล้วหรือยัง
        # -----------------------------------------------------------
        if job_number in seen_jobs:
            if base_name not in seen_jobs[job_number]:
                seen_jobs[job_number].append(base_name)
                suffix_index = len(seen_jobs[job_number]) - 1
                suffix = chr(ord('a') + suffix_index - 1)
                job_number = f"{job_number}{suffix}"
                print(
                    f"[DUPLICATE JOB] พบไฟล์ใหม่ของ Job {job_number}: {base_name}")
        else:
            seen_jobs[job_number] = [base_name]

        # -----------------------------------------------------------
        # กำหนดให้เซฟไฟล์ CSV ไปที่ output_folder
        # -----------------------------------------------------------
        csv_filename = os.path.join(output_folder, f"{job_number}.csv")

        if os.path.exists(csv_filename):
            print(
                f"[SKIP] ข้ามไฟล์: {base_name} (มีไฟล์ {job_number}.csv อยู่แล้ว)")
            return

        excel_file = pd.ExcelFile(file_path)

        best_sheet = None
        max_score = -1
        ignore_sheets = ['DESCRIPTION', 'VLOOKUP',
                         'CUSTOMS', 'PORT', 'EX', 'DUCATI (D)', 'DUCATI (D) (2)']

        for sheet in excel_file.sheet_names:
            if sheet.upper().strip() in ignore_sheets:
                continue

            df = pd.read_excel(file_path, sheet_name=sheet)
            score = df.notnull().sum().sum()

            cells_data = df.astype(str).values.flatten()
            has_recent_job = any(
                "24-" in str(v) or "25-" in str(v) or "SKY" in str(v) for v in cells_data)

            if has_recent_job:
                score += 500

            if score > max_score:
                max_score = score
                best_sheet = sheet

        if best_sheet:
            final_df = pd.read_excel(file_path, sheet_name=best_sheet)

            try:
                final_df = final_df.map(clean_datetime_and_time)
            except AttributeError:
                final_df = final_df.applymap(clean_datetime_and_time)

            final_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

            print(f"[SUCCESS] ไฟล์: {base_name}")
            print(
                f"   -> เลือก Sheet: '{best_sheet}' | บันทึกไปที่: {csv_filename}")
        else:
            print(
                f"[WARNING] ข้ามไฟล์: {base_name} (ไม่พบ Sheet ที่เข้าเงื่อนไข)")

    except Exception as e:
        print(f"[ERROR] เกิดข้อผิดพลาดกับไฟล์ {base_name}: {e}")


def run_excel_pipeline():
    input_folder = "/opt/airflow/raw_data"
    output_folder = "/opt/airflow/data"
    os.makedirs(output_folder, exist_ok=True)

    search_pattern = os.path.join(input_folder, "*.xlsx")
    excel_files = [f for f in glob.glob(
        search_pattern) if not os.path.basename(f).startswith("~")]

    seen_jobs = {}

    for file in excel_files:
        export_best_sheet_to_csv(file, output_folder, seen_jobs)

    for job, files in seen_jobs.items():
        if len(files) > 1:
            print(f"[SUMMARY] Job {job} มีหลายไฟล์: {', '.join(files)}")


# --- Airflow DAG ---
with DAG(
    dag_id="etl_invoice_to_csv",
    start_date=dt(2026, 8, 17),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    task_run_pipeline = PythonOperator(
        task_id="run_excel_pipeline",
        python_callable=run_excel_pipeline
    )
