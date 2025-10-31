#!/usr/bin/env python3
import time
import random
import json

def generate_reading(sensor_id: int):
    return {
        "sensor_id": sensor_id,
        "timestamp": int(time.time()),
        "temperature": round(20 + random.random() * 10, 2),
        "vibration": round(random.random() * 5, 3)
    }

if __name__ == "__main__":
    # Simple local emitter to stdout (adapt to Kafka producer later)
    for i in range(5):
        print(json.dumps(generate_reading(sensor_id=random.randint(1,10))))
        time.sleep(1)