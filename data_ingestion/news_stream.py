import json
import time
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer


class NewsDataStream:
    def __init__(self, api_key, kafka_producer, topic, keywords, languages=["en"]):
        self.api_key = api_key
        self.producer = kafka_producer
        self.topic = topic
        self.keywords = keywords
        self.languages = languages
        self.base_url = "https://newsapi.org/v2/everything"
        self.last_request_time = None
        print(f"NewsStream initialized with topic: {topic}")
        

    def get_news(self):
        try:
            # Calculate time window
            current_time = datetime.utcnow()
            from_time = (current_time - timedelta(days=7)).strftime('%Y-%m-%d')  # Format date correctly
            to_time = current_time.strftime('%Y-%m-%d')
            
            # Create query from keywords
            query = ' OR '.join(self.keywords)
            
            params = {
                'q': query,
                'language': ','.join(self.languages),
                'from': from_time,
                'to': to_time,
                'sortBy': 'publishedAt',
                'apiKey': self.api_key
            }
            
            print(f"\nMaking API request...")
            print(f"Query: {query}")
            print(f"URL: {self.base_url}")
            print(f"Parameters: {params}")
            
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            print(f"\nAPI Response Status: {data.get('status')}")
            print(f"Total Results: {data.get('totalResults', 0)}")
            
            if 'articles' in data:
                articles = data['articles']
                print(f"Retrieved {len(articles)} articles")
                return articles
            else:
                print(f"Unexpected API response: {data}")
                return []
                
        except Exception as e:
            print(f"Error fetching news: {e}")
            print(f"Full error: {str(e)}")
            return []

    def process_news(self, articles):
        """Process news data before sending articles to Kafka"""
        if not articles:
            print("No articles to process")
            return
            
        processed_count = 0
        for article in articles:
            try:
                if 'url' not in article or 'title' not in article or 'source' not in article:
                    print("Skipping article due to missing required fields")
                    continue
            
                # Safely get fields with defaults
                article_data = {
                    'id': hash(article['url']),
                    'title': article['title'],
                    'description': article.get('description', ''),
                    'content': article.get('content', ''),
                    'source': article['source'].get('name', 'Unknown'),
                    'url': article['url']
                }
                
                print(f"\nSending article to Kafka:")
                print(f"Topic: {self.topic}")
                print(f"Title: {article_data['title'][:50]}...")
                
                self.producer.send(
                    self.topic,
                    json.dumps(article_data)
                )
                processed_count += 1
                
            except Exception as e:
                print(f"Error processing article: {e}")
                
        print(f"\nProcessed {processed_count} articles")


    def stream(self, update_interval=60):
        """Main streaming loop"""
        print(f"Starting news stream with {update_interval} second intervals...")
        
        while True:
            try:
                print("\n--- Fetching news update ---")
                articles = self.get_news()
                self.process_news(articles)
                
                print(f"\nWaiting {update_interval} seconds until next update...")
                time.sleep(update_interval)
                
            except Exception as e:
                print(f"Stream error: {e}")
                time.sleep(5)