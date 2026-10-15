import pandas as pd
import glob
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def safe_parse_date(value):
    try:
        return pd.to_datetime(value, dayfirst=True).date()
    except Exception:
        return None


def etl_billing_to_db():
    input_folder = "/opt/airflow/billing_data/data"
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))

    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for file in csv_files:
        file_name = os.path.basename(file)

        # --- เช็กว่าไฟล์เคยโหลดแล้วหรือยัง ---
        cursor.execute(
            "SELECT 1 FROM warehouse.etl_file_log WHERE file_name = %s", (file_name,))
        if cursor.fetchone():
            print(f"[SKIP] ข้ามไฟล์ {file_name} (เคยโหลดแล้ว)")
            continue

        try:
            df = pd.read_csv(file)

            for _, row in df.iterrows():
                # --- Dimension: Date ---
                date = safe_parse_date(row["DATE"])
                date_key = None

                if date:
                    # ถ้า parse ได้ → insert/update dim_date
                    cursor.execute("""
                        INSERT INTO warehouse.dim_date (date, day, month, quarter, year)
                        VALUES (%s, EXTRACT(DAY FROM %s), EXTRACT(MONTH FROM %s),
                                EXTRACT(QUARTER FROM %s), EXTRACT(YEAR FROM %s))
                        ON CONFLICT (date) DO NOTHING;
                    """, (date, date, date, date, date))
                    cursor.execute(
                        "SELECT date_key FROM warehouse.dim_date WHERE date = %s;", (date,))
                    date_key = cursor.fetchone()[0]
                else:
                    # ถ้า parse ไม่ได้ → ไม่ update dim_date และไม่เปลี่ยนค่า date_key
                    print(
                        f"⚠️ Skipped DATE update for row JOB={row['JOB  NO.']} INV={row['INV  NO.']}")

                # --- Dimension: Shipper ---
                cursor.execute("""
                    INSERT INTO warehouse.dim_shipper (shipper_name)
                    VALUES (%s)
                    ON CONFLICT (shipper_name) DO NOTHING;
                """, (row["SHIPPER"],))
                cursor.execute(
                    "SELECT shipper_key FROM warehouse.dim_shipper WHERE shipper_name = %s;", (row["SHIPPER"],))
                shipper_key = cursor.fetchone()[0]

                # --- Dimension: Customer ---
                cursor.execute(
                    "SELECT customer_key FROM warehouse.dim_customer WHERE customer_name = %s;", (row["CUSTOMS"],))
                existing = cursor.fetchone()

                if existing:
                    customer_key = existing[0]
                else:
                    cursor.execute(
                        "INSERT INTO warehouse.dim_customer (customer_name) VALUES (%s) RETURNING customer_key;", (row["CUSTOMS"],))
                    customer_key = cursor.fetchone()[0]

                # --- Dimension: Shipment Config ---
                cursor.execute("""
                    INSERT INTO warehouse.dim_shipment_config (direction, loading_type, vehicle_size)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (direction, loading_type, vehicle_size) DO NOTHING;
                """, (row["BY"], row["VOLUMN"], row["SIZE"]))
                cursor.execute("""
                    SELECT shipment_config_key FROM warehouse.dim_shipment_config
                    WHERE direction = %s AND loading_type = %s AND vehicle_size = %s;
                """, (row["BY"], row["VOLUMN"], row["SIZE"]))
                shipment_config_key = cursor.fetchone()[0]

                # --- Dimension: Driver ---
                cursor.execute("""
                    INSERT INTO warehouse.dim_driver (driver_name)
                    VALUES (%s)
                    ON CONFLICT (driver_name) DO NOTHING;
                """, (row["DRIVER"],))
                cursor.execute(
                    "SELECT driver_key FROM warehouse.dim_driver WHERE driver_name = %s;", (row["DRIVER"],))
                driver_key = cursor.fetchone()[0]

                # --- Dimension: Location (pickup/dropoff) ---
                cursor.execute("""
                    INSERT INTO warehouse.dim_location (location_name)
                    VALUES (%s)
                    ON CONFLICT (location_name) DO NOTHING;
                """, (row["START"],))
                cursor.execute(
                    "SELECT location_key FROM warehouse.dim_location WHERE location_name = %s;", (row["START"],))
                pickup_key = cursor.fetchone()[0]

                cursor.execute("""
                    INSERT INTO warehouse.dim_location (location_name)
                    VALUES (%s)
                    ON CONFLICT (location_name) DO NOTHING;
                """, (row["END"],))
                cursor.execute(
                    "SELECT location_key FROM warehouse.dim_location WHERE location_name = %s;", (row["END"],))
                dropoff_key = cursor.fetchone()[0]

                # --- Fact Billing (OR logic) ---
                cursor.execute("""
                    SELECT fact_billing_key FROM warehouse.fact_billing
                    WHERE job_no = %s OR inv_no = %s;
                """, (row["JOB  NO."], row["INV  NO."]))
                existing = cursor.fetchone()

                if existing:
                    cursor.execute("""
                        UPDATE warehouse.fact_billing
                        SET date_key=%s, shipper_key=%s, customer_key=%s, shipment_config_key=%s,
                            driver_key=%s, pickup_key=%s, dropoff_key=%s,
                            job_no=%s, inv_no=%s,
                            service_fee=%s, service_cost=%s, service_commission=%s, service_revenue_net=%s,
                            transport_fee=%s, transport_cost=%s, transport_commission=%s, transport_revenue_net=%s,
                            advance=%s, revenue_total=%s, vat_7=%s, wh_3=%s, grand_total=%s
                        WHERE job_no=%s OR inv_no=%s;
                    """, (
                        date_key, shipper_key, customer_key, shipment_config_key,
                        driver_key, pickup_key, dropoff_key,
                        row["JOB  NO."], row["INV  NO."],
                        row["SERVICE"], row["COST"], row["COMMISSION"], row["รายได้ SERVICE"],
                        row["ค่าขนส่ง"], row["COST.1"], row["COMMISSION.1"], row["รายได้ขนส่ง"],
                        row["ADVANCE"], row["รวมรายได้"], row["VAT 7%"], row["WH -3%"], row["ยอดรวม"],
                        row["JOB  NO."], row["INV  NO."]
                    ))
                    print(
                        f"[UPDATE] Updated record for JOB={row['JOB  NO.']} or INV={row['INV  NO.']}")
                else:
                    cursor.execute("""
                        INSERT INTO warehouse.fact_billing (
                            date_key, shipper_key, customer_key, shipment_config_key,
                            driver_key, pickup_key, dropoff_key,
                            job_no, inv_no,
                            service_fee, service_cost, service_commission, service_revenue_net,
                            transport_fee, transport_cost, transport_commission, transport_revenue_net,
                            advance, revenue_total, vat_7, wh_3, grand_total
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,
                                %s,%s,%s,%s,
                                %s,%s,%s,%s,
                                %s,%s,%s,%s,%s);
                    """, (
                        date_key, shipper_key, customer_key, shipment_config_key,
                        driver_key, pickup_key, dropoff_key,
                        row["JOB  NO."], row["INV  NO."],
                        row["SERVICE"], row["COST"], row["COMMISSION"], row["รายได้ SERVICE"],
                        row["ค่าขนส่ง"], row["COST.1"], row["COMMISSION.1"], row["รายได้ขนส่ง"],
                        row["ADVANCE"], row["รวมรายได้"], row["VAT 7%"], row["WH -3%"], row["ยอดรวม"]
                    ))
                    print(
                        f"[INSERT] Added new record JOB={row['JOB  NO.']} INV={row['INV  NO.']}")

            # log ว่าไฟล์นี้โหลดสำเร็จ
            cursor.execute(
                "INSERT INTO warehouse.etl_file_log (file_name, status) VALUES (%s, %s)", (file_name, "SUCCESS"))
            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"❌ Error processing file {file_name}: {e}")
            cursor.execute(
                "INSERT INTO warehouse.etl_file_log (file_name, status, error_message) VALUES (%s, %s, %s)", (file_name, "ERROR", str(e)))
            conn.commit()

    cursor.close()
    conn.close()


# --- Airflow DAG ---
with DAG(
    dag_id="etl_billing_to_db",
    start_date=datetime(2026, 8, 30),
    schedule_interval="*/10 * * * *",  # ทุก 10 นาที
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="etl_billing_task",
        python_callable=etl_billing_to_db
    )
