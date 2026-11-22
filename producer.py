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
    """
    newsdata.io API params:
    - apikey  : your key
    - category: technology / sports / business / health
    - language: en
    - country : us
    """
    try:
        response=requests.get(NEWS_API_URL,params={
            "apikey":NEWS_API_KEY,
            "category":category,
           "language": "en",
            "country": "us",
        })
        data=response.json()
        
        if data.get("status") != "success":
            print(f"⚠️  API error for {category}: {data.get('message')}")
            return []
        
        return data.get("results", [])
    except Exception as e:
        print(f"Error fetching news for {category}: {e}")
        return []
    
# ─── Publish to Kafka ─────────────────────────────────────────────────────────
def publish_articles(producer,articles,category):
    for article in articles:
        if not article.get("title"):
            continue
        
        event={
            "title":        article.get("title", ""),
            "source":       article.get("source_id", "unknown"),   
            "category":     category,
            "url":          article.get("link", ""),               
            "published_at": article.get("pubDate", ""),            
            "description":  article.get("description", ""),
            "country":      article.get("country", []),            
            "is_breaking":  "breaking" in (article.get("title") or "").lower()  # simulate breaking news flag
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
                
            
            # newsdata.io free = 200 req/day
            # 4 categories × 1 req = 4 req per loop
            # sleep 60s → 4 × 24 × 60/60 = 96 req/day → safely under limit    
            print(f" Sleeping 30 seconds...")
            time.sleep(30) # NewsAPI free tier: 100 req/day, so be careful
            
    except KeyboardInterrupt:
        print("\n Stopping producer...")
        producer.close()

           