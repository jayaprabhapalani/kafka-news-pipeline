# Reads raw-news, filters by category, re-publishes to specific topics.

import json
from kafka import KafkaConsumer,KafkaProducer
from config import KAFKA_BROKER,TOPICS

# consumer should always exist in a consuer group
def create_consumer(group_id):
    return KafkaConsumer(
        TOPICS["raw"],
        bootstrap_servers=KAFKA_BROKER,
        group_id=group_id,
        auto_offset_reset='earliest',  # start from beginning if new group
        enable_auto_commit=False,      # manual commit → at-least-once (for understanding purpose)
        value_deserializer= lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None    
    )
    
    
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    

if __name__=="__main__":
    consumer=create_consumer(group_id="filter-group")
    producer=create_producer()
    
    print("Filter Consumer started - watching raw-news... ")

    for message in consumer:
        event=message.value
        category=event.get("category")
        
        try:
            # ── FILTER LOGIC ──────────────────────────────────────
            if category=="technology":
                producer.send(TOPICS["tech"], key=category, value=event)
                print(f"Published to technology topic: {event['title'][:60]}")
            
            elif category=="sports":
                producer.send(TOPICS["sports"],key=category,value=event)
                print(f"Published to sports topic: {event['title'][:60]}")
            
            # other categories are dropped (not forwarded)
            else:
                print(f"⏭️  Skipped [{category}]: {event['title'][:40]}")

            producer.flush()
            
            # Manual commit AFTER successful processing
            # This is AT-LEAST-ONCE: if crash happens before commit,
            # message will be reprocessed on restart
            consumer.commit()
        
        except Exception as e:
            print(f"Error processing message: {e}")
            # Don't commit → message will be retried
            # In production → send to DLQ here


                
             
            