from kafka import KafkaConsumer
import psycopg2
import json

consumer = KafkaConsumer(
    "semiconductor-events",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

conn = psycopg2.connect(
    host="localhost",
    database="semiconductor",
    user="admin",
    password="admin"
)

cursor = conn.cursor()

for msg in consumer:

    event = msg.value

    cursor.execute(
        '''
        INSERT INTO semiconductor_events (
            timestamp,
            wafer_id,
            lot_id,
            step,
            equipment_id,
            equipment_health,
            temperature,
            pressure,
            gas_flow,
            rf_power,
            vibration,
            particle_count,
            defect_probability,
            label
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''',
        (
            event.get("timestamp"),
            event.get("wafer_id"),
            event.get("lot_id"),
            event.get("step"),
            event.get("equipment_id"),
            event.get("equipment_health"),
            event.get("temperature"),
            event.get("pressure"),
            event.get("gas_flow"),
            event.get("rf_power"),
            event.get("vibration"),
            event.get("particle_count"),
            event.get("defect_probability"),
            event.get("label"),
        )
    )

    conn.commit()

    print("Inserted:", event["wafer_id"])