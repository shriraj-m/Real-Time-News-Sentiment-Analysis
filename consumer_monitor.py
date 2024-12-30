from kafka import KafkaConsumer
import json
import yaml
from datetime import datetime

def load_config(config_path='config/config.yaml'):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def monitor_sentiment_results():
    # Load configuration
    config = load_config()
    
    # Create consumer
    consumer = KafkaConsumer(
        'news_sentiment',
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    
    print("\n=== Starting Sentiment Analysis Monitor ===")
    print("Waiting for messages...\n")
    
    try:
        for message in consumer:
            news_item = message.value
            
            # Print formatted results
            print("\n" + "="*50)
            print(f"Title: {news_item.get('title', 'N/A')}")
            print(f"Published: {news_item.get('publishedAt', 'N/A')}")
            print("\nSentiment Analysis Results:")
            print(f"Sentiment: {news_item['sentiment_analysis']['sentiment']}")
            print(f"Confidence: {news_item['sentiment_analysis']['confidence']:.2%}")
            print(f"Analyzed at: {news_item['sentiment_analysis']['analyzed_at']}")
            print("="*50 + "\n")
            
    except KeyboardInterrupt:
        print("\nMonitor stopped by user")
    except Exception as e:
        print(f"\nError: {str(e)}")
    finally:
        consumer.close()

if __name__ == "__main__":
    monitor_sentiment_results()