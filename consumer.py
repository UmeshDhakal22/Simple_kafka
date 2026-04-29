from kafka import KafkaConsumer
import json
from db import engine

TOPIC = "user_topic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest"
)

for msg in consumer:
    data = msg.value

    # -------------------------------
    # 1. ACCOUNT MASTER INSERT
    # -------------------------------
    account_query = """
    INSERT INTO account_master (
        account_number,
        mobile_number,
        device_model,
        device_brand,
        device_os_version,
        device_age_days,
        total_memory_gb,
        battery_level,
        is_emulator
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    account_values = (
        data["mobileNumber"],
        data["mobileNumber"],
        data["deviceMetadata"]["model"],
        data["deviceMetadata"]["brand"],
        data["deviceMetadata"]["os_version"],
        data["deviceMetadata"]["device_age_days"],
        int(data["deviceMetadata"]["total_memory_gb"]),
        data["deviceMetadata"]["battery_level"],
        data["deviceMetadata"]["is_emulator"]
    )

    # -------------------------------
    # 2. TRANSACTIONS INSERT
    # -------------------------------
    transaction_query = """
    INSERT INTO transactions (
        mobile_number,
        to_mobile_number,
        to_bank_name,
        tran_date,
        description,
        lcy_amount,
        dc_indicator
    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    transaction_values = []
    for txn in data["transactions"]:
        transaction_values.append((
            data["mobileNumber"],
            txn["mobile_number"],
            txn["bank_name"],
            txn["tran_date"],
            txn["description"],
            float(txn["lcy_amount"]),
            txn["dc_indicator"]
        ))

    # -------------------------------
    # 3. STATS INSERT
    # -------------------------------
    stats_query = """
    INSERT INTO stats (
        mobile_number,
        alert_detected,
        total_sms_selected,
        successfully_parsed,
        failed_to_parse
    ) VALUES (%s,%s,%s,%s,%s)
    """

    stats_values = (
        data["mobileNumber"],
        data["smsFeatures"]["alert_detected"],
        data["parseStats"]["total_sms_selected"],
        data["parseStats"]["successfully_parsed"],
        data["parseStats"]["failed_to_parse"]
    )

    # -------------------------------
    # EXECUTION
    # -------------------------------
    with engine.begin() as conn:
        conn.execute(account_query, account_values)
        conn.execute(transaction_query, transaction_values)
        conn.execute(stats_query, stats_values)

    print("✅ Successfully parsed all JSON values:")
    print(f"Mobile Number: {data['mobileNumber']}")
    print(f"Device Model: {data['deviceMetadata']['model']}")
    print(f"Transactions Count: {len(data['transactions'])}")
    print(f"Alert Detected: {data['smsFeatures']['alert_detected']}")
    print(f"Total SMS Selected: {data['parseStats']['total_sms_selected']}")
    # print("✅ Inserted full record")