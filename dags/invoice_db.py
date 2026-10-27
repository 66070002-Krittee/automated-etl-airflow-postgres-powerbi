import os
import glob
import pandas as pd
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def extract_company_before_date(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            if "DATE" in line:
                company_part = line.split("DATE")[0].strip()
                return company_part
    return None


def extract_address_fixed(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    line1 = ""
    line2 = ""

    for line in lines:
        if "JOB  NO." in line:
            line1 = line.split("JOB  NO.")[0].strip().replace(",", "")
        elif "CREDIT  TERM" in line:
            line2 = line.split("CREDIT  TERM")[0].strip().replace(",", "")

    return f"{line1} {line2}".strip() if line1 or line2 else None


def extract_vehicle_and_location(csv_file):
    vehicle_size = "-"
    location_names = ["-", "-"]
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            if "ค่าบริการรับ - ส่งสินค้า" in line:
                match_vehicle = re.search(
                    r"ค่าบริการรับ\s*-\s*ส่งสินค้า\s*(.+?)\s*\(", line)
                if match_vehicle:
                    vehicle_size = match_vehicle.group(1).strip()

                match_loc = re.search(r"\(([^)]+)\)", line)
                if match_loc:
                    loc_text = match_loc.group(1)
                    location_names = [x.strip() for x in loc_text.split("-")]
                    if len(location_names) == 1:
                        location_names.append("-")
                break

    return vehicle_size, location_names


def extract_advance_from_csv(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            if "รวมยอด" in line or "SUM" in line:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) > 9:
                    val = parts[9].strip()
                    try:
                        return float(val.replace(",", ""))
                    except ValueError:
                        return 0.0
    return 0.0


def extract_date(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            if "DATE" in line:
                parts = line.split(":")
                if len(parts) > 1:
                    raw_date = parts[-1].strip().replace(",", "")
                    try:
                        return pd.to_datetime(raw_date, dayfirst=True).date()
                    except Exception:
                        return None
    return None


def extract_job_no(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            if "JOB  NO." in line:
                job_no = line.split(":")[-1].strip()
                job_no = job_no.replace(",", "")
                return job_no
    return None


def extract_inv_no(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            if "BOOKING" in line.upper() and "NO" in line.upper():
                parts = [p.strip().replace('"', '') for p in line.split(",")]
                for i, p in enumerate(parts):
                    if "BOOKING" in p.upper() and "NO" in p.upper():
                        if i + 2 < len(parts):
                            val = parts[i + 2].strip()
                            if val:
                                return val
            elif "CUSTOMER" in line.upper():
                parts = [p.strip().replace('"', '') for p in line.split(",")]
                if len(parts) > 9:
                    val = parts[9].strip()
                    if val:
                        return val
    return None


def extract_tax_id(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            if "TAX  ID" in line.upper():
                parts = [p.strip() for p in line.split(",")]
                for i, p in enumerate(parts):
                    if "TAX  ID" in p.upper():
                        if i + 1 < len(parts):
                            raw_val = parts[i + 1].strip()
                            try:
                                if "E" in raw_val.upper():
                                    return str(int(float(raw_val)))
                                return raw_val
                            except Exception:
                                return raw_val
    return None


def extract_shipper_name(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            parts = [p.strip().replace('"', '') for p in line.split(",")]
            if "SHIPPING  NAME" in parts:
                idx = parts.index("SHIPPING  NAME")
                collected = []
                for p in parts[idx+1:]:
                    if "DECL  NO." in p:
                        break
                    if p and p != ":":
                        collected.append(p)
                if collected:
                    return " ".join(collected).strip()
            else:
                match = re.search(
                    r"SHIPPING\s*NAME\s*[:]\s*(.+)", line, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
    return None


def extract_direction(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            parts = [p.strip().replace('"', '') for p in line.split(",")]
            if "IN  BOUND / OUT  BOUND" in parts:
                idx = parts.index("IN  BOUND / OUT  BOUND")
                collected = []
                for p in parts[idx+1:]:
                    if "GWT." in p.upper():
                        break
                    if p and p != ":":
                        collected.append(p)
                if collected:
                    direction_part = " ".join(collected).strip().upper()
                    if "IN" in direction_part:
                        return "IN"
                    elif "OUT" in direction_part:
                        return "OUT"
            else:
                match = re.search(
                    r"IN\s*BOUND\s*/\s*OUT\s*BOUND\s*[:]\s*(.+)", line, re.IGNORECASE)
                if match:
                    direction_part = match.group(1).strip().upper()
                    if "GWT." in direction_part:
                        direction_part = direction_part.split("GWT.")[
                            0].strip()
                    if "IN" in direction_part:
                        return "IN"
                    elif "OUT" in direction_part:
                        return "OUT"
    return "-"


def extract_loading_type(csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for line in f:
            if "VOLUMN" in line:
                parts = [p.strip() for p in line.split(",")]
                idx = 9
                while idx < len(parts):
                    val = parts[idx]
                    if val in ["VOLUMN", ":"]:
                        idx += 1
                        continue
                    loading_type = re.sub(
                        r"\bEXPORT\b|\bIMPORT\b|\bIMP.\b", "", val, flags=re.IGNORECASE
                    ).strip()
                    return loading_type if loading_type else "-"
    return "-"


def etl_billing_from_csv():
    folder_path = "/opt/airflow/data"
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for csv_file in csv_files:
        file_name = os.path.basename(csv_file)

        cursor.execute(
            "SELECT 1 FROM warehouse.etl_file_log WHERE file_name = %s", (file_name,))
        if cursor.fetchone():
            print(f"[SKIP] ข้ามไฟล์ {file_name} (เคยโหลดแล้ว)")
            continue

        try:
            customer_name = extract_company_before_date(csv_file)
            if customer_name:
                customer_name = customer_name.replace(",", "").strip().upper()
            else:
                raise ValueError("⚠️ Customer name not found before DATE")

            address = extract_address_fixed(csv_file)
            if not address:
                raise ValueError("⚠️ Customer address not found")

            date = extract_date(csv_file)
            if not date:
                raise ValueError("⚠️ Date not found or invalid")

            job_no = extract_job_no(csv_file)
            if not job_no:
                raise ValueError("⚠️ Job number not found")

            inv_no = extract_inv_no(csv_file)
            if not inv_no:
                raise ValueError("⚠️ Invoice number not found")

            suffix = inv_no.strip()[-1]
            if suffix in ["A", "B", "C"]:
                job_no = f"{job_no}{suffix}"

            tax_id = extract_tax_id(csv_file)
            if not tax_id:
                raise ValueError("⚠️ Tax ID not found")

            shipper_name = extract_shipper_name(csv_file)
            if not shipper_name:
                raise ValueError("⚠️ Shipper name not found")

            direction = extract_direction(csv_file)
            if not direction:
                raise ValueError("⚠️ Direction not found")

            loading_type = extract_loading_type(csv_file)
            if not loading_type:
                raise ValueError("⚠️ Loading type not found")

            vehicle_size, location_names = extract_vehicle_and_location(
                csv_file)
            if not vehicle_size:
                raise ValueError("⚠️ Vehicle size not found")
            if not location_names or len(location_names) < 2:
                raise ValueError("⚠️ Pickup/Dropoff locations not found")

            advance = extract_advance_from_csv(csv_file)
            if advance is None:
                raise ValueError("⚠️ Advance amount not found")

            # --- Dimension: Date ---
            cursor.execute("""
                INSERT INTO warehouse.dim_date (date, day, month, quarter, year)
                VALUES (%s, EXTRACT(DAY FROM %s), EXTRACT(MONTH FROM %s),
                        EXTRACT(QUARTER FROM %s), EXTRACT(YEAR FROM %s))
                ON CONFLICT (date) DO NOTHING;
            """, (date, date, date, date, date))
            cursor.execute(
                "SELECT date_key FROM warehouse.dim_date WHERE date = %s;", (date,))
            date_key = cursor.fetchone()[0]

            # --- Dimension: Shipper ---
            cursor.execute("""
                INSERT INTO warehouse.dim_shipper (shipper_name)
                VALUES (%s)
                ON CONFLICT (shipper_name) DO NOTHING;
            """, (shipper_name,))
            cursor.execute(
                "SELECT shipper_key FROM warehouse.dim_shipper WHERE shipper_name = %s;", (shipper_name,))
            shipper_key = cursor.fetchone()[0]

            # --- Dimension: Customer ---
            cursor.execute("""
                INSERT INTO warehouse.dim_customer (customer_name, tax_id, address)
                VALUES (%s, %s, %s)
                ON CONFLICT (tax_id) DO UPDATE SET address = EXCLUDED.address;
            """, (customer_name, tax_id, address))
            cursor.execute(
                "SELECT customer_key FROM warehouse.dim_customer WHERE tax_id = %s;", (tax_id,))
            customer_key = cursor.fetchone()[0]

            # --- Dimension: Location ---
            location_keys = []
            for loc in location_names:
                cursor.execute("""
                    INSERT INTO warehouse.dim_location (location_name)
                    VALUES (%s)
                    ON CONFLICT (location_name) DO NOTHING;
                """, (loc,))
                cursor.execute(
                    "SELECT location_key FROM warehouse.dim_location WHERE location_name = %s;", (loc,))
                location_keys.append(cursor.fetchone()[0])

            pickup_key = location_keys[0] if len(location_keys) > 0 else None
            dropoff_key = location_keys[1] if len(location_keys) > 1 else None

            # --- Dimension: Shipment Config ---
            cursor.execute("""
                INSERT INTO warehouse.dim_shipment_config (direction, loading_type, vehicle_size)
                VALUES (%s, %s, %s)
                ON CONFLICT (direction, loading_type, vehicle_size) DO NOTHING;
            """, (direction, loading_type, vehicle_size))
            cursor.execute("""
                SELECT shipment_config_key FROM warehouse.dim_shipment_config
                WHERE direction = %s AND loading_type = %s AND vehicle_size = %s;
            """, (direction, loading_type, vehicle_size))
            shipment_config_key = cursor.fetchone()[0]

            # --- Insert Fact Billing ---
            cursor.execute("""
                INSERT INTO warehouse.fact_billing (
                    date_key, shipper_key, customer_key, shipment_config_key,
                    pickup_key, dropoff_key, job_no, inv_no, advance, revenue_total, grand_total
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_no) DO NOTHING;
            """, (date_key, shipper_key, customer_key, shipment_config_key,
                  pickup_key, dropoff_key, job_no, inv_no, advance, None, None))

            print(f"✅ Inserted Fact Billing: {job_no}")

            cursor.execute(
                "INSERT INTO warehouse.etl_file_log (file_name, status) VALUES (%s, %s)",
                (file_name, "SUCCESS")
            )
            conn.commit()

        except Exception as e:
            print(f"❌ Error in file {file_name}: {e}")
            conn.rollback()  # rollback transaction ที่ fail
            cursor.execute(
                "INSERT INTO warehouse.etl_file_log (file_name, status, error_message) VALUES (%s, %s, %s)",
                (file_name, "ERROR", str(e))
            )
            conn.commit()

    cursor.close()
    conn.close()


# --- Airflow DAG ---
with DAG(
    dag_id="etl_invoice_to_db",
    start_date=datetime(2026, 7, 18),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="etl_billing_task_csv",
        python_callable=etl_billing_from_csv
    )
