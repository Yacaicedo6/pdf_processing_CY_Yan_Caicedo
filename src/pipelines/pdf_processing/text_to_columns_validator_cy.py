import argparse
import logging
from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------------------------------------------------------
# Logger Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("CY_Validator")

# ------------------------------------------------------------------------------
# 0 Parse Arguments
# ------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="CY Extractor Validator")
parser.add_argument(
    "--source_table",
    required=True,
    help="Spark table name for validation (e.g., logistics.default.truckr_loads_cy)"
)
args = parser.parse_args()
source_table = args.source_table

# ------------------------------------------------------------------------------
# 1 Spark Session
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("CY_Extractor_Validator").getOrCreate()

# ------------------------------------------------------------------------------
# 2 Schema Definition
# ------------------------------------------------------------------------------
fields = [
    "source_file",
    "broker_name", "broker_phone", "broker_email",
    "broker_address", "broker_city", "broker_state", "broker_zipcode",
    "broker_rep", "broker_rep_phone",
    "loadConfirmationNumber", "totalCarrierPay",
    "carrier_name", "carrier_usdot", "carrier_email",
    "pickup_facility_1", "pickup_address_1", "pickup_city_1",
    "pickup_state_1", "pickup_zipcode_1", "pickup_phone_1",
    "pickup_start_datetime_1", "pickup_end_datetime_1",
    "delivery_facility_1", "delivery_address_1", "delivery_city_1",
    "delivery_state_1", "delivery_zipcode_1", "delivery_phone_1",
    "delivery_start_datetime_1", "delivery_end_datetime_1",
]
schema = T.StructType([T.StructField(f, T.StringType()) for f in fields])

# ------------------------------------------------------------------------------
# 3 Ground Truth Record
# Source: 1679676366011_CY FL-GA.pdf (Load 28861101)
# Author: Yan Caicedo
# ------------------------------------------------------------------------------
truth_record = {
    "source_file": "dbfs:/Volumes/logistics/default/raw/txt/source=CY/1679676366011_CY FL-GA.txt",
    "broker_name": "Coyote Logistics, LLC",
    "broker_phone": "877-626-9683",
    "broker_email": "CarrierInvoices@coyote.com",
    "broker_address": "960 Northpoint Parkway Suite 150",
    "broker_city": "Alpharetta",
    "broker_state": "GA",
    "broker_zipcode": "30005",
    "broker_rep": "Tamaz Bazgadze",
    "broker_rep_phone": "+1 (423) 385 3805 x2246",
    "loadConfirmationNumber": "28861101",
    "totalCarrierPay": "600.00",
    "carrier_name": "GTT Freight Corp",
    "carrier_usdot": "3723304",
    "carrier_email": "gtt.expresscorp@gmail.com",
    "pickup_facility_1": "United Sugars",
    "pickup_address_1": "450 SONORA DRIVE GATE D",
    "pickup_city_1": "Clewiston",
    "pickup_state_1": "FL",
    "pickup_zipcode_1": "33440",
    "pickup_phone_1": "+1 (863) 902 2707",
    "pickup_start_datetime_1": "2023-03-29T08:00:00",
    "pickup_end_datetime_1": "2023-03-29T13:00:00",
    "delivery_facility_1": "Batory Foods",
    "delivery_address_1": "885 DOUGLAS HILLS RD",
    "delivery_city_1": "Lithia Springs",
    "delivery_state_1": "GA",
    "delivery_zipcode_1": "30122",
    "delivery_phone_1": "+1 (800) 282 3101",
    "delivery_start_datetime_1": "2023-03-30T09:30:00",
    "delivery_end_datetime_1": "2023-03-30T09:30:00",
}

truth_df = spark.createDataFrame([truth_record], schema=schema)

# ------------------------------------------------------------------------------
# 4 Load Target Table from Parameter
# ------------------------------------------------------------------------------
target_df = spark.table(source_table)

# ------------------------------------------------------------------------------
# 5 Normalize Data
# ------------------------------------------------------------------------------
def normalize(df):
    string_cols = [c for c, t in df.dtypes if t == "string"]
    return df.select(*[
        F.trim(F.lower(F.col(c))).alias(c) if c in string_cols else F.col(c)
        for c in df.columns
    ])

truth_df = normalize(truth_df)
target_df = normalize(target_df)

# ------------------------------------------------------------------------------
# 6 Compare Values Field-by-Field
# ------------------------------------------------------------------------------
load_id = truth_record["loadConfirmationNumber"]
target_rows = target_df.filter(F.col("loadConfirmationNumber") == load_id).collect()

results = []

if not target_rows:
    logger.error(f"No record found for loadConfirmationNumber={load_id}")
    for field in schema.fieldNames():
        results.append((field, "❌ Missing record", truth_record.get(field), None))
else:
    logger.info(f"Found record for loadConfirmationNumber={load_id}")
    target_values = target_rows[0].asDict()
    for field in schema.fieldNames():
        truth_val = truth_record.get(field)
        target_val = target_values.get(field)
        norm_truth = str(truth_val).strip().lower() if truth_val else None
        norm_target = str(target_val).strip().lower() if target_val else None
        status = "✅ Match" if norm_truth == norm_target else "❌ Mismatch"
        results.append((field, status, truth_val, target_val))

# ------------------------------------------------------------------------------
# 7 Log Results
# ------------------------------------------------------------------------------
logger.info(f"Validation results for loadConfirmationNumber={load_id}:")
for field, status, truth, target in results:
    if status == "✅ Match":
        logger.info(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")
    else:
        logger.error(f"{field:30} | {status:10} | truth='{truth}' | target='{target}'")

# ------------------------------------------------------------------------------
# 8 Fail Pipeline if Errors Detected
# ------------------------------------------------------------------------------
errors = [r for r in results if r[1].startswith("❌")]
if errors:
    logger.error(f"Validation failed for {len(errors)} fields")
    for field, status, truth, target in errors:
        logger.error(f"  - {field}: expected='{truth}' got='{target}'")
    raise ValueError(f"Validation failed for {len(errors)} fields")
else:
    logger.info("✅ All fields match perfectly.")
