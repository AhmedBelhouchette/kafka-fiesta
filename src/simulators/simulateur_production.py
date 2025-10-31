#!/usr/bin/env python3
import time
import random
import json

def generate_batch(batch_id: int):
    return {
        "batch_id": batch_id,
        "timestamp": int(time.time()),
        "units_produced": random.randint(50, 200),
        "defects": random.randint(0, 5)
    }

if __name__ == "__main__":
    for i in range(3):
        print(json.dumps(generate_batch(batch_id=i+1)))
        time.sleep(1)