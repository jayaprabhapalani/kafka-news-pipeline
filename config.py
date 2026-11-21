KAFKA_BROKER="localhost:9092"

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
# newsapi.org needs a free key → sign up at newsapi.org (free)
NEWS_API_KEY = "your_newsapi_key_here"
NEWS_API_URL = "https://newsapi.org/v2/top-headlines"

# OpenWeatherMap - free key
WEATHER_API_KEY = "your_openweather_key_here"
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# Categories we care about
CATEGORIES = ["technology", "sports", "business", "health"]