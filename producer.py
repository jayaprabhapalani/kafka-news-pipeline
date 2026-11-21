import json
import time
import requests
from kafka import KafkaProducer
from config import KAFKA_BROKER, TOPICS, NEWS_API_KEY, NEWS_API_URL, CATEGORIES

# ─── Producer Setup ───────────────────────────────────────────────────────────
def create_producer(acks='all'):
    """
    acks='all'  → waits for all replicas to confirm (safest, slower)
    acks=1      → only leader confirms (balanced)
    acks=0      → fire and forget (fastest, can lose messages)
    
    Try changing this and see the speed difference!
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        acks=acks,
        retries=3,
        value_serializer=lambda v:json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    

# ─── Fetch News ───────────────────────────────────────────────────────────────
def fetch_news(category):
    """ Fetch top headlines for a category from NewsAPI """
    try:
        response=requests.get(NEWS_API_URL,params={
            "apiKey":NEWS_API_KEY,
            "category":category,
            "country":"us",
            "pageSize":5       # 5 articles per category per fetch
        })
        data=response.json()
        return data.get("articles",[])
    except Exception as e:
        print(f"Error fetching news for {category}: {e}")
        return []
    
# ─── Publish to Kafka ─────────────────────────────────────────────────────────
def publish_articles(producer,articles,category):
    for article in articles:
        if not article.get("title"):
            continue
        
        event={
            "title": article["title"],
            "source": article["source"]["name"],
            "category": category,
            "url": article["url"],
            "published_at": article["publishedAt"],
            "description": article.get("description", ""),
            "is_breaking": "breaking" in article["title"].lower()  # simulate breaking news flag
        }
        # KEY = category → same category always goes to same partition
        # This guarantees ordering within a category
        producer.send(
            topic=TOPICS["raw"],
            key=category,
            value=event
        )
        print(f" Published [{category}]: {article['title'][:60]}...")
        
    producer.flush()  # Ensure all messages are sent


# ─── Main Loop ────────────────────────────────────────────────────────────────

if __name__ =="__main__":
    print("Starting producer with acks='all'")
    producer=create_producer(acks='all')
    
    fetch_count=0
    try:
        while True:
            fetch_count+=1
            print(f"\n--- Fetch #{fetch_count} ---")
        
        
            for category in CATEGORIES:
                articles=fetch_news(category)
                publish_articles(producer, articles, category)
            print(f" Sleeping 30 seconds...")
            time.sleep(30) # NewsAPI free tier: 100 req/day, so be careful
            
    except KeyboardInterrupt:
        print("\n Stopping producer...")
        producer.close()

           