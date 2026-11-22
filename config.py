import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER=os.getenv("KAFKA_BROKER", "localhost:9092")

# Topics -- topics are set of events / category
TOPICS={
    "raw":"raw-news",
    "tech":"tech-news",
    "sports":"sport-news",
    "enriched":"enriched-news",
    "alerts":"breaking-alerts",
    "dlq":"dead-letter-news",
}

# Partition config
PARTITIONS={
    "raw-news": 6,
    "tech-news": 3,
    "sports-news": 3,
    "enriched-news": 3,
    "breaking-alerts": 1,
    "dead-letter-news": 1,
}

# Free APIs - no key needed for these
# newsdata.io (free, no credit card)
NEWS_API_KEY =os.getenv("NEWS_API_KEY")
NEWS_API_URL = "https://newsdata.io/api/1/news"

# OpenWeatherMap - free key
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Categories we care about
CATEGORIES = ["technology", "sports", "business", "health"]