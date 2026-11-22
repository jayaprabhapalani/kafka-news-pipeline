# Watches for breaking news. Demonstrates at-least-once + at-most-once.

import json
from kafka import KafkaConsumer
from config import KAFKA_BROKER, TOPICS

def create_consumer(group_id,auto_commit):
    return KafkaConsumer(
        TOPICS["raw"],
        bootstrap_servers=KAFKA_BROKER,
        group_id=group_id,
        auto_offset_reset='earliest',  # start from beginning if new group
        enable_auto_commit=auto_commit, # auto commit → at-most-once
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None    
    )
    
def proccess_alert(event):
    """Simulate an alert - could be email, SMS, push notification"""
    print(f"\n🚨 BREAKING NEWS ALERT!")
    print(f"   Source  : {event['source']}")
    print(f"   Category: {event['category']}")
    print(f"   Title   : {event['title']}")
    print(f"   URL     : {event['url']}\n")
    

if __name__ == "__main__":
    # Using at-least-once (manual commit)
    # Means: if we crash after processing but before commit → alert fires again on restart
    # For alerts, at-least-once is safer (better to alert twice than miss)
    consumer=create_consumer(
    group_id="alert-group", 
    auto_commit=False
    )
    
    print("🚨 Alert Consumer started - watching for BREAKING NEWS...")
    
    for message in consumer:
        event=message.value
        
        if event.get("is_breaking"):
            proccess_alert(event)
            
        # Always commit, even if not breaking (we still processed the message)
        consumer.commit()   
    