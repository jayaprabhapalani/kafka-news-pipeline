# Joins news with weather data — this is the most interesting one.

import json
import requests
from kafka import KafkaConsumer, KafkaProducer
from config import KAFKA_BROKER,TOPICS,WEATHER_API_KEY,WEATHER_API_URL


# Source cities to enrich (map news source → city for weather lookup)
SOURCE_CITY_MAP = {
    "BBC News": "London",
    "CNN": "New York",
    "Reuters": "New York",
    "TechCrunch": "San Francisco",
    "ESPN": "New York",
}


def fetch_weather(city):
    """Fetch current weather for a city"""
    try:
        response=requests.get(WEATHER_API_URL,params={
            "q":city,
            "appid":WEATHER_API_KEY,
            "units":"metric"
        }) 
        data = response.json()
        return {
            "city":city,
            "temp_c":data["main"]["temp"],
            "condition":data["weather"][0]["description"]
        }
    except:
        return {
            "city":city,
            "temp_c":None,
            "condition":"unknown"
        }
        

if __name__=="__main__":
    consumer=KafkaConsumer(
        TOPICS["raw"],
        bootstrap_servers=KAFKA_BROKER,
        group_id="enriched-group",    # different group → independent offset tracking
        auto_offset_reset='earliest',  # start from beginning if new group
        enable_auto_commit=False,
        value_deserializer=lambda x:json.loads(x.decode('utf-8')) if x else None   
    )
    
    producer=KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    
    print("🌤️  Enriched Consumer started - joining news + weather...")
    
    for message in consumer:
        event=message.value
        source=event.get("source","")
        
        # ── JOIN: news event + weather data ──────────────────────
        city=SOURCE_CITY_MAP.get(source,"New York")  #default to NY
        weather=fetch_weather(city)
        enriched_event={
            **event,            # all original news fields
            "weather":weather   # + joined weather data
            }
        
        producer.send(
            topic=TOPICS["enriched"],
            key=event["category"].encode('utf-8'),  # preserve category partitioning
            value=enriched_event
        )
        print(f"📡 Published enriched event: {event['title'][:50]}..."
              f"| Weather in {city}: {weather['temp_c']}°C, {weather['condition']}")
        
        consumer.commit()

