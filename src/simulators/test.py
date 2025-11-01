from kafka import KafkaConsumer
import json

def test_consumer():
    print("🧪 Testing Kafka Consumers...")
    
    # Test sensor data consumer
    try:
        sensor_consumer = KafkaConsumer(
            'donnees-capteurs',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        print("✅ Connected to 'donnees-capteurs' topic")
        
        # Read one message to test
        message = next(sensor_consumer)
        print(f"📨 Sample sensor data: {message.value}")
        
        sensor_consumer.close()
        
    except Exception as e:
        print(f"❌ Sensor consumer error: {e}")
    
    # Test production plan consumer
    try:
        production_consumer = KafkaConsumer(
            'plan-de-production',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        print("✅ Connected to 'plan-de-production' topic")
        
        # Read one message to test
        message = next(production_consumer)
        print(f"📨 Sample production data: {message.value}")
        
        production_consumer.close()
        
    except Exception as e:
        print(f"❌ Production consumer error: {e}")

if __name__ == "__main__":
    test_consumer()