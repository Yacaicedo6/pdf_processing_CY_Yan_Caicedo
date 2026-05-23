import os
import re
import logging
import argparse
from abc import ABC, abstractmethod
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import MapType, StringType

# ============================================================
# Logger
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# Base Extractor
# ============================================================
class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, text: str) -> dict:
        pass


# ============================================================
# CY Extractor
# ============================================================
class CYExtractor(BaseExtractor):
    """Extracts broker, carrier, pickup, and delivery data from Coyote Logistics load confirmation text files."""

    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"[ \t\u00A0]+", " ", text)
        text = re.sub(r"\r\n?", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = "\n".join([ln.strip() for ln in text.splitlines() if ln.strip()])
        return text.strip()

    def _parse_datetime(self, date_str: str, time_str: str) -> str:
        """Convert date and time strings to ISO format."""
        if not date_str or not time_str:
            return ""
        # Clean up time string — remove 'at', strip spaces
        time_str = re.sub(r"^at\s*", "", time_str.strip())
        for fmt in [
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y %I:%M %p",
            "%a %m/%d/%Y %H:%M",
        ]:
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        return ""

    def _extract_stop_blocks(self, text: str) -> list:
        """
        Extract stop blocks from CY text.
        Each block starts with 'Stop N:' and ends before the next 'Stop N+1:' or 'Charges'.
        Returns list of (stop_type, block_text) tuples.
        """
        pattern = re.compile(r"(Stop\s+\d+\s*:\s*(Pick\s*Up|Delivery))", re.I)
        markers = list(pattern.finditer(text))

        blocks = []
        for i, m in enumerate(markers):
            stop_label = m.group(2).strip().lower()
            start = m.end()
            end = markers[i + 1].start() if i + 1 < len(markers) else len(text)
            # Cut at Charges section
            sub = text[start:end]
            cutoff = re.search(r"\nCharges\b", sub, re.I)
            if cutoff:
                sub = sub[:cutoff.start()]
            stop_type = "pickup" if "pick" in stop_label else "delivery"
            blocks.append((stop_type, sub.strip()))

        return blocks

    def extract(self, text: str) -> dict:
        data = {f: "" for f in EXTRACTION_FIELDS}
        if not text:
            return data

        text = self._normalize(text)

        # -------- Broker Info (fixed for Coyote) --------
        data["broker_name"] = "Coyote Logistics, LLC"
        data["broker_address"] = "960 Northpoint Parkway Suite 150"
        data["broker_city"] = "Alpharetta"
        data["broker_state"] = "GA"
        data["broker_zipcode"] = "30005"
        data["broker_phone"] = "877-626-9683"
        data["broker_email"] = "CarrierInvoices@coyote.com"

        # -------- Load Confirmation Number --------
        if m := re.search(r"Load\s+(\d{7,9})", text, re.I):
            data["loadConfirmationNumber"] = m.group(1).strip()

        # -------- Total Carrier Pay --------
        if m := re.search(r"Total\s+USD\s+\$?([\d,]+\.\d{2})", text, re.I):
            data["totalCarrierPay"] = m.group(1).replace(",", "").strip()

        # -------- Carrier Info --------
        if m := re.search(r"Carrier\s+([A-Za-z0-9 &\.\-]+)\n", text, re.I):
            data["carrier_name"] = m.group(1).strip()

        if m := re.search(r"USDOT\s+(\d+)", text, re.I):
            data["carrier_usdot"] = m.group(1).strip()

        if m := re.search(r"Email\s+([\w\.\-]+@[\w\.\-]+\.\w+)", text, re.I):
            data["carrier_email"] = m.group(1).strip()

        # -------- Rep Info --------
        if m := re.search(r"Rep\s+([A-Za-z ]+)\n", text, re.I):
            data["broker_rep"] = m.group(1).strip()

        if m := re.search(r"Rep.*?\nPhone\s+(\+?[\d\s\-x]+)\n", text, re.I | re.S):
            data["broker_rep_phone"] = m.group(1).strip()

        # -------- Stop Blocks --------
        blocks = self._extract_stop_blocks(text)

        pickup_idx = 1
        delivery_idx = 1

        for stop_type, block in blocks:

            # Facility name
            facility = ""
            if fm := re.search(r"Facility\s+(.+?)(?:\n|Address)", block, re.I | re.S):
                facility = re.sub(r"\s+", " ", fm.group(1)).strip()

            # Address
            address = ""
            if am := re.search(r"Address\s+(.+?)(?:\n(?:Contact|Phone|Facility Notes|SLIC|Driver Work)|$)", block, re.I | re.S):
                address = re.sub(r"\s+", " ", am.group(1)).strip()

            # City, State, Zipcode — parse from address
            city, state, zipcode = "", "", ""
            if czm := re.search(r"([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)", address):
                city = czm.group(1).strip()
                state = czm.group(2).strip()
                zipcode = czm.group(3).strip()
                # Clean address to remove city/state/zip
                address = address[:czm.start()].strip()

            # Phone
            phone = ""
            if pm := re.search(r"Phone\s+(\+?[\d\s\(\)\-]+)", block, re.I):
                phone = pm.group(1).strip()
                if phone.lower() == "none":
                    phone = ""

            # Date and time
            date_str, time_start, time_end = "", "", ""

            # "Appointment Scheduled For\nDDD MM/DD/YYYY\nat HH:MM"
            appt_m = re.search(
                r"(?:Appointment\s+)?Scheduled\s+For\s*\n"
                r"(?:\w+\s+)?(\d{2}/\d{2}/\d{4})\s*\n"
                r"(?:from\s+(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})|at\s+(\d{2}:\d{2}))",
                block, re.I
            )
            if appt_m:
                date_str = appt_m.group(1)
                if appt_m.group(2):  # from HH:MM - HH:MM
                    time_start = appt_m.group(2)
                    time_end = appt_m.group(3)
                else:  # at HH:MM
                    time_start = appt_m.group(4)
                    time_end = appt_m.group(4)

            start_dt = self._parse_datetime(date_str, time_start)
            end_dt = self._parse_datetime(date_str, time_end)

            if stop_type == "pickup" and pickup_idx <= 3:
                i = pickup_idx
                data[f"pickup_facility_{i}"] = facility
                data[f"pickup_address_{i}"] = address
                data[f"pickup_city_{i}"] = city
                data[f"pickup_state_{i}"] = state
                data[f"pickup_zipcode_{i}"] = zipcode
                data[f"pickup_phone_{i}"] = phone
                data[f"pickup_start_datetime_{i}"] = start_dt
                data[f"pickup_end_datetime_{i}"] = end_dt
                pickup_idx += 1

            elif stop_type == "delivery" and delivery_idx <= 3:
                i = delivery_idx
                data[f"delivery_facility_{i}"] = facility
                data[f"delivery_address_{i}"] = address
                data[f"delivery_city_{i}"] = city
                data[f"delivery_state_{i}"] = state
                data[f"delivery_zipcode_{i}"] = zipcode
                data[f"delivery_phone_{i}"] = phone
                data[f"delivery_start_datetime_{i}"] = start_dt
                data[f"delivery_end_datetime_{i}"] = end_dt
                delivery_idx += 1

        return data


# ============================================================
# Schema fields
# ============================================================
EXTRACTION_FIELDS = [
    # Broker
    "broker_name", "broker_phone", "broker_email",
    "broker_address", "broker_city", "broker_state", "broker_zipcode",
    "broker_rep", "broker_rep_phone",
    # Load
    "loadConfirmationNumber", "totalCarrierPay",
    # Carrier
    "carrier_name", "carrier_usdot", "carrier_email",
    # Pickups (up to 3)
    *[f"{p}_{i}" for p in [
        "pickup_facility", "pickup_address", "pickup_city",
        "pickup_state", "pickup_zipcode", "pickup_phone",
        "pickup_start_datetime", "pickup_end_datetime"
    ] for i in range(1, 4)],
    # Deliveries (up to 3)
    *[f"{p}_{i}" for p in [
        "delivery_facility", "delivery_address", "delivery_city",
        "delivery_state", "delivery_zipcode", "delivery_phone",
        "delivery_start_datetime", "delivery_end_datetime"
    ] for i in range(1, 4)],
    "processed_at"
]


# ============================================================
# Spark UDF wrapper
# ============================================================
def extract_fields_udf():
    extractor = CYExtractor()

    def _extract(text):
        result = extractor.extract(text)
        result["processed_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        return result

    return udf(_extract, MapType(StringType(), StringType()))


# ============================================================
# Main (parameterized)
# ============================================================
def main(p):
    spark = SparkSession.builder.appName("CY Extraction").getOrCreate()
    logger.info("Starting CY extraction process")

    input_path = os.path.join(p["source_path"], "*.txt")
    df = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.txt")
        .option("recursiveFileLookup", "false")
        .load(input_path)
        .select(col("_metadata.file_path").alias("source_file"), col("content"))
    )

    df = df.withColumn("text", col("content").cast("string")).drop("content")
    logger.info(f"Files detected: {df.count()}")

    extract_udf = extract_fields_udf()
    df = df.withColumn("extracted", extract_udf(col("text")))

    for field in EXTRACTION_FIELDS:
        df = df.withColumn(field, col("extracted").getItem(field))

    df = df.drop("text", "extracted")

    logger.info(f"Writing {df.count()} records to {p['target_table']}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(p["target_table"])
    )

    logger.info("CY extraction completed successfully.")


# ============================================================
# CLI entry
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CY PDF Extraction Parameters")
    parser.add_argument("--source_path")
    parser.add_argument("--target_table")
    args = parser.parse_args()

    params = {"source_path": args.source_path, "target_table": args.target_table}
    main(params)
