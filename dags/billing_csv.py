import os
import glob
import re
import datetime
from datetime import datetime as dt
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

# ==========================================
# 1. SHIPPER MAP (ดึงชื่อย่อ -> ชื่อเต็ม )
# ==========================================
SHIPPER_MAP = {
    "3M": "3M INNOVATION (THAILAND) LIMITED",
    "A.T.E.Q.": "A.T.E.Q (THAILAND) CO. LTD.",
    "AAPICO": "AAPICO HITECH PUBLIC CO. LTD.",
    "ADLITE": "ADLITE & FAVBRIC PRINTING CO. LTD.",
    "ADVANCED": "ADVANCED COMPOSITE SYSTEM LTD.",
    "AGON": "AGON PACIFIC COMPANY LIMITED",
    "AKATHAVEE": "AKATHAVEE TOOLS CORPORATION LTD.",
    "ALCOM": "ALCOM EXTRUSION LTD.",
    "ALLIANCE": "ALLIANCE LAUNDRY (THAILAND) CO. LTD.",
    "AMAIR": "AMAIR CO. LTD.",
    "ANAN VISION": "ANAN VISION CO. LTD.",
    "ANT MA": "ANT MA (THAILAND) CO. LTD.",
    "ANTIFIRE": "ANTIFIRE CO. LTD.",
    "APTAR": "APTAR (THAILAND) LTD.",
    "AQUAFIL": "AQUAFIL ASIA PACIFIC CO. LTD.",
    "ASIA PRECISION": "ASIA PRECISION A.T. CO. LTD.",
    "ATCO": "ATCO CO. LTD.",
    "AVO": "AVO TURBOWORLD ASIA CO. LTD.",
    "AZAWA": "AZAWA TRADING (THAILAND) CO. LTD.",
    "AZIMUTH": "AZIMUTH ASIA PACIFIC LTD.",
    "BARBE": "BARBE (THAILAND) LTD.",
    "BAUME": "BAUME CHEMICAL CO. LTD.",
    "BESTRADE": "BESTRADE PRECISION LIMIED",
    "BORGWARNER": "BORGWARNER PDS (THAILAND) LIMITED",
    "CAPITAL": "CAPITAL TRICOT CO. LTD.",
    "CAR-LACK": "CAR-LACK (THAI-GERMAN) CO. LTD.",
    "CERAMICS": "CERAMICS WORLDWIDE CO. LTD.",
    "CHANG ANAN": "CHANG ANAN CO. LTD.",
    "CHEMFLEET": "CHEMFLEET CO. LTD.",
    "CHING CHENG": "CHING CHENG WIRE MATERIAL (THAILAND) CO. LTD.",
    "CHYUAN": "CHYUAN JAAN SHING INDUSTRIAL (THAILAND) CO. LTD.",
    "CITIZEN": "CITIZEN SEIMITSU (THAILAND) CO. LTD.",
    "COETERNITY": "COETERNITY CO. LTD.",
    "CORING": "CORING PRO CO. LTD.",
    "CYTAC": "CYTAC TECHNOLOGY (THAILAND) LIMITED",
    "DAE-A": "DAE-A ELECTRONICS (THAILAND) CO. LTD.",
    "DONG SEUNG": "DONG SEUNG (THAILAND) CO. LTD.",
    "E-TUK": "E- TUK FACTORY (THAILAND) CO. LTD.",
    "EFTEC": "EFTEC (THAILAND) CO. LTD.",
    "ENSHU": "ENSHU MOLDING PRODUCTS (THAILAND) CO. LTD.",
    "EX-KCRANES": "EX-KCRANES SERVICE COMPANY LIMITED",
    "FABRINET": "FABRINET CO. LTD.",
    "FAIRTEX": "FAIRTEX EQUIPMENT CO. LTD.",
    "FLOW ENGINERING": "FLOW ENGINEERING AND SERVICES CO. LTD.",
    "FLOWLUTION": "FLOWLUTION COMPANY LIMITED.",
    "FONGMAO": "FONGMAO HARDWARE (THAILAND) CO. LTD.",
    "FRIULAIR": "FRIULAIR (THAILAND) CO. LTD.",
    "FSK": "FSK (THAILAND) CO. LTD.",
    "GATES UNITTA": "GATES UNITTA (THAILAND) CO. LTD.",
    "GATEWAY": "GATEWAY EQUIPMENT LTD.",
    "GENERAL": "GENERAL STARCH LIMITED.",
    "GFCT": "GFCT CO. LTD.",
    "GPV": "GPV ASIA (THAILAND) CO. LTD.",
    "GREEN BEE": "GREEN BEE SHOP DROP SHIPPING LIMITED",
    "HAIYAN": "HAIYAN CABLE",
    "HITACHI": "HITACHI ASTEMO KORAT BRAKE SYSTEM LTD.",
    "HITOUCH": "HITOUCH AUDIO (THAILAND) CO. LTD.",
    "HOGAN": "HOGAN METAL PRODUCS (THAILAND) CO. LTD.",
    "INTRUSION": "INTRUSION PROOF CO. LTD.",
    "J.P.C.": "J.P.C. CO-BIZ (THAILAND) CO. LTD.",
    "JEAN NIEL": "JEAN NIEL ASIA LTD.",
    "JITPRASERT": "JITPRASERT INDUSTRIES CO. LTD.",
    "JP SUPPLY": "JP SUPPLY AND SUCCESS CO. LTD.",
    "JT": "JT INDUSTRY CO. LTD.",
    "K BLOCK": "K BLOCK TECHNOLOGY CO. LTD.",
    "K-TECH": "K-TECH INDUSTRIAL (THAILAND) CO. LTD.",
    "KCE": "KCE TECHNOLOGY CO. LTD.",
    "KING SHING": "KING SHING AUTOMOBILE PARTS CO. LTD.",
    "KISTLER": "KISTLER INSTRUMENT (THAILAND) CO. LTD.",
    "KLOECKNER": "KLOECKNER PENTAPLAST (THAILAND) CO. LTD.",
    "KOREA MOTORS": "KOREA MOTORS SALES CO. LTD.",
    "KRUGER": "KRUGER VENTILATION INDUSTRIES ASIA CO. LTD.",
    "LABTECH": "LABTECH ENGINEERING CO. LTD.",
    "LEEDEN": "LEEDEN (THAILAND) CO. LTD.",
    "LIGMAN": "LIGMAN LIGHTING COMPANY LIMITED",
    "LINXENS": "LINXENS (THAILAND) CO. LTD.",
    "LUX ROYAL": "LUX ROYAL (THAILAND) CO. LTD.",
    "MACHINE": "MACHINE AUTO PART CO. LTD.",
    "MAHLE": "MAHLE SIAM FILTER SYSTEMS CO. LTD.",
    "MARELLI": "MARELLI (THAILAND) CO. LTD.",
    "MAX X": "MAX X CO. LTD.",
    "MAXXIS": "MAXXIS INTERNATIONAL (THAILAND) CO. LTD.",
    "MCR": "MCR AUTO PARTS CO. LTD.",
    "MILWAUKEE": "MILWAUKEE TOOL (THAILAND) COMPANY LIMITED",
    "MMA": "MMA SECURITY SEALS (THAILAND) CO. LTD.",
    "MS.CHATCHAWAN": "MISS CHATCHAWAN BAUMGARTNER",
    "MULTI-FINELINE": "MULTI-FINELINE ELECTRONICS (THAILAND) CO. LTD.",
    "NEUMAN": "NEUMAN & ESSER SOUTH EAST ASIA LTD.",
    "NIEDAX": "NIEDAX (THAILAND) LTD.",
    "NIHON": "NIHON SUPERIOR (THAILAND) CO. LTD.",
    "NISSHO": "NISSHO SEIKO (THAILAND) LTD.",
    "OHLINS": "OHLINS ASIA CO. LTD.",
    "OHTL": "OHTL PUBLIC COMPANY LIMITED",
    "OMAL": "OMAL ASIA PACIFIC LTD.",
    "P & A": "P & A TECHNOLOGY CO. LTD.",
    "PAKCO": "PAKCO INTERNATIONAL CO. LTD.",
    "PARK PLUS": "PARK PLUS CO. LTD.",
    "POR RUNGROD": "POR.RUNGROD EXPORT CO. LTD.",
    "PORNPAWEE": "PORNPAWEE PRO CO. LTD.",
    "POWERTECH2004": "POWERTECH 2004 CO. LTD.",
    "POWERTECH2005": "POWERTECH 2004 CO. LTD.",
    "POWERTECH2006": "POWERTECH 2004 CO. LTD.",
    "PRM TOOLS": "PRM TOOLS CO. LTD.",
    "PROGUARD": "PROGUARD INDUSTRY CO. LTD.",
    "PROMINENT": "PROMINENT FLUID CONTROLS (THAILAND) CO. LTD.",
    "RATTANACHAI": "RATTANACHAI TAWEESUPCHAROEN LIMITED PARTNERSHIP",
    "RMA": "RMA AUTOMOTIVE CO. LTD.",
    "ROVITEX": "ROVITEX ASIA LTD.",
    "SAINT-GOBAIN": "SAINT-GOBAIN SEKURIT (THAILAND) CO. LTD.",
    "SANGETSU": "SANGETSU GOODRICH (THAILAND) CO. LTD.",
    "SHARP POINT": "SHARP POINT ASIA CO. LTD.",
    "SHIJI": "SHIJI (THAILAND) LIMITED",
    "SHUN THAI": "SHUN THAI CO. LTD.",
    "SIAM CHRISTMAS": "SIAM CHRISTMAS CO. LTD.",
    "SIG": "SIG COMBIBLOC LTD.",
    "SKY KINGDOM": "SKY KINGDOM CO. LTD.",
    "SPF DIANA": "SPF DIANA (THAILAND) CO. LTD.",
    "SRISUWAN": "SRISUWAN TECHNOLOGY CO. LTD.",
    "SUBKAEW": "SUBKAEW UNITRADE CO. LTD.",
    "SUPAVUT": "SUPAVUT INDUSTRY CO. LTD.",
    "SUPREME": "SUPREME  MACHINERY  MANUFATURING  CO. LTD.",
    "SUPER DRY": "SUPER DRY CO. LTD.",
    "SVI": "SVI PUBLIC COMPANY LIMITED",
    "T.RAD": "T.RAD (THAILAND) CO. LTD.",
    "TANATEX": "TANATEX CHEMICALS (THAILAND) CO. LTD.",
    "TAURUS": "TAURUS POZZOLANS CO. LTD.",
    "TAXACO": "TAXACO (THAILAND) CO. LTD.",
    "TENRYU": "TENRYU SAW (THAILAND) CO. LTD.",
    "THAI AKIBA": "THAI AKIBA CO. LTD.",
    "THAI AUSNOR": "THAI AUSNOR COMPANY LIMITED",
    "THAI GLASS": "THAI GLASS INDUSTRIES PUBLIC CO. LTD.",
    "THAI WHETRON": "THAI WHETRON ELECTRONICS CO. LTD.",
    "THAVORN": "THAVORN TRACTOR ENGINEERING CO. LTD.",
    "THONGSUEN": "THONGSUEN AND PATTARAVADEE CO. LTD.",
    "TIRAPOSIN": "TIRAPOSIN CO. LTD.",
    "TOFUJI": "TOFUJI (THAILAND) CO. LTD.",
    "TOKYO INK": "TOKYO INK (THAILAND) CO. LTD.",
    "TPCS": "TPCS PUBLIC COMPANY LIMITED",
    "TYROLIT": "TYROLIT THAI DIAMOND CO. LTD.",
    "ULTRA MARINE": "ULTRA MARINE CO. LTD.",
    "VICTRON": "VICTRON U.P.S. (THAILAND) CO. LTD.",
    "VOLCANO": "VOLCANO TEC (THAILAND) CO. LTD.",
    "WATERFRONT": "WATERFRONT HOTEL CO. LTD.",
    "WEALTHOCEAN": "WEALTHOCEANSUPPLY CO. LTD.",
    "WEFIC": "WEFIC OCEAN EQUIPMENT (THAILAND) CO. LTD.",
    "WELDING": "WELDING ALLOYS (THAILAND) CO. LTD.",
    "XAVI": "XAVI TECHNOLOGIES (THAILAND) CO. LTD.",
    "YOSHIDA": "YOSHIDA ENPLA THAI LTD.",
    "YUHARA": "YUHARA MANUFACTURING (THAILAND) CO. LTD.",
    "ZENFLOOR": "ZENFLOOR CO. LTD.",
    "ZIEHL-ABEGG": "ZIEHL-ABEGG (THAILAND) LTD.",
    "ABP": "A.B.P. STAINLESS FASTENER CO. LTD.",
    "LUCKY STAR": "LUCKY STAR WEAVING CO. LTD.",
    "ROECHLING": "ROECHLING AUTOMOTIVE CHONBURI CO. LTD.",
    "CHACHA": "CHACHA THAI LTD.",
    "DUCATI POWER": "DUCATI POWERTRAIN (THAILAND) CO. LTD.",
    "DUCATI MOTOR": "DUCATI MOTOR (THAILAND) CO. LTD.",
    "CBE": "CBE (THAILAND) CO. LTD.",
    "AUNDE": "AUNDE (THAILAND) CO. LTD.",
    "SCILUTION": "SCILUTION CO. LTD.",
    "KLINK": "KLINK ADVANCED TECHNOLOGY COMPANY LIMITED",
    "TURBINEAERO": "TURBINEAERO REPAIR LTD.",
}

# ==========================================
# 2. CUSTOMS MAP (ดึงชื่อย่อ -> ชื่อเต็ม ภาษาไทย ทั้งหมด)
# ==========================================
CUSTOMS_MAP = {
    "ABP": "บริษัท เอ.บี.พี.สเตนเลส ฟาสเทนเนอร์ จำกัด",
    "ALIBABA": "บริษัท อาลีบาบา โลจิสติกส์ (ไทยแลนด์) จำกัด",
    "BEST GLOBAL": "บริษัท เบสท์ โกลบอล โลจิสติกส์ จำกัด",
    "CAREMAX": "บริษัท แคร์แม็กซ์ เฟรท คาร์โก้ จำกัด",
    "CHACHA": "บริษัท ชาช่า ไทย จำกัด",
    "CHANG ANAN": "บริษัท ช้างอานันท์ จำกัด",
    "CHEMFLEET": "บริษัท เคมฟลีท จำกัด",
    "EARTH": "บริษัท เอิร์ท โลจิสติกส์ (ประเทศไทย) จำกัด",
    "ECU": "บริษัท เอ็กคู่ เวิลด์ไวด์ (ประเทศไทย) จำกัด",
    "FASTABLE": "บริษัท ฟาสท์เอเบิล โลจิสติกส์ จำกัด",
    "FIVE STAR": "บริษัท ไฟว์ สตาร์ โกลบอล โลจิสติกส์ เซอร์วิสเซส (ไทยแลนด์) จำกัด",
    "FLS": "บริษัท เอฟแอลเอส 1993 (ไทยแลนด์) จำกัด",
    "GREEN BEE": "ห้างหุ้นส่วนจำกัด กรีน บี ช็อป ดรอป ชิ้ปปิ้ง",
    "KILINK": "บริษัท เคลิงค์ แอดวานซ์ เทคโนโลยี จำกัด",
    "KLINK": "บริษัท เคลิงค์ แอดวานซ์ เทคโนโลยี จำกัด",
    "LA1": "บริษัท แอล.เอ.วัน จำกัด",
    "MACHINE": "บริษัท แมชชีน ออโต้ พาร์ท จำกัด",
    "MAX X": "บริษัท แม็กเอ็กซ์ จำกัด",
    "MCR": "บริษัท เอ็มซีอาร์ ออโต้พาร์ท จำกัด",
    "NIMBLE": "บริษัท นิมเบิล โลจิสติกส์ จำกัด",
    "PORNPAWEE": "บริษัท พรปวีณ์ โปร จำกัด",
    "QUALITY": "บริษัท ควอลิตี้ แอคเซส โกลบอล โลจิสติกส์ จำกัด",
    "RATTANACHAI": "ห้างหุ้นส่วนจำกัด รัตนชัยทวี ทรัพย์เจริญ",
    "SHIPCO": "บริษัท ชิปโก้ ทรานสปอร์ต (ประเทศไทย) จำกัด",
    "SLS": "บริษัท เอส แอล เอส อินเตอร์ ทรานสปอร์ต จำกัด",
    "SUBKAEW": "บริษัท ทรัพย์แก้ว ยูนิเทรด จำกัด",
    "SUPER DRY": "บริษัท ซูเปอร์ดราย จำกัด",
    "TARA": "บริษัท ตารา โลจิสติกส์ จำกัด (สำนักงานใหญ่)",
    "TAXACO": "บริษัท แทคซาโก (ประเทศไทย) จำกัด",
    "WEALTHOCEAN": "บริษัท เว็ลธโอเชียนซัฟพลาย จำกัด",
}


def clean_datetime_and_time(val):
    if pd.isna(val):
        return val
    if isinstance(val, (pd.Timestamp, datetime.datetime, datetime.date)):
        return val.strftime('%d/%m/%Y')
    if isinstance(val, str):
        return re.sub(
            r'(\b\d{1,4}[/-]\d{1,2}[/-]\d{1,4})\s+\d{1,2}:\d{2}(:\d{2})?\b',
            r'\1',
            val
        )
    return val


def process_billing_summary():
    input_folder = "/opt/airflow/billing_data"
    output_folder = os.path.join(input_folder, "data")
    os.makedirs(output_folder, exist_ok=True)

    excel_files = glob.glob(os.path.join(input_folder, "*.xlsx"))

    for file_name in excel_files:
        base_name = os.path.splitext(os.path.basename(file_name))[0]
        output_file = os.path.join(output_folder, f"{base_name}_processed.csv")

        if os.path.exists(output_file):
            print(f"[WARNING] ข้ามไฟล์: {file_name} เพราะเคยถูกแปลงแล้ว")
            continue

        match = re.search(r"(\d{4})", base_name)
        year_suffix = match.group(
            1)[-2:] if match else str(datetime.datetime.now().year)[-2:]

        # อ่านไฟล์ Excel
        df = pd.read_excel(file_name, usecols="A:X", header=3)
        df = df.iloc[:-2].reset_index(drop=True)
        df = df.ffill()

        def fill_with_suffix_local(series):
            group = (series != series.shift()).cumsum()
            cum_count = series.groupby(group).cumcount()
            suffixes = cum_count.apply(lambda n: "" if n == 0 else chr(64 + n))
            return series.astype(str) + suffixes

        raw_job_numbers = df.iloc[:, 0]
        df.iloc[:, 0] = raw_job_numbers.apply(
            lambda x: f"SKY{year_suffix}-{str(x).zfill(5)}"
            if pd.notna(x) and str(x).strip() != ""
            else x
        )

        df.iloc[:, 0] = fill_with_suffix_local(df.iloc[:, 0])

        if "INV  NO." in df.columns:
            df["INV  NO."] = fill_with_suffix_local(df["INV  NO."])

        df = df.applymap(clean_datetime_and_time)

        # ------------------------------------------
        #  Map คอลัมน์ SHIPPER จาก SHIPPER_MAP
        # ------------------------------------------
        if "SHIPPER" in df.columns:
            df["SHIPPER"] = df["SHIPPER"].apply(
                lambda x: SHIPPER_MAP.get(
                    str(x).strip(), x) if pd.notna(x) else x
            )

        # ------------------------------------------
        #  Map คอลัมน์ CUSTOMS จาก CUSTOMS_MAP
        # ------------------------------------------
        if "CUSTOMS" in df.columns:
            df["CUSTOMS"] = df["CUSTOMS"].apply(
                lambda x: CUSTOMS_MAP.get(
                    str(x).strip(), x) if pd.notna(x) else x
            )

        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"บันทึกไฟล์เรียบร้อย: {output_file}")


# --- Airflow DAG ---
with DAG(
    dag_id="etl_billing_to_csv",
    start_date=dt(2026, 8, 25),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="process_billing_summary_task",
        python_callable=process_billing_summary
    )
