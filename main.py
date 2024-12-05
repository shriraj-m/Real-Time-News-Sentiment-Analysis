import yaml
import threading
import os

from data_ingestion.news_stream import NewsDataStream
from preprocessing.news_preprocessor import NewsPreprocessor
from data_ingestion.kafka_producer import create_producer

def get_absolute_path(relative_path):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, relative_path)

def run_news_stream(config_path):
    try:
        print("Loading configuration...")
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        print("Configuration loaded:", config)  # Add this to verify config content
        
        # Create kafka producer
        producer = create_producer(config_path)
        
        # Create and start news stream
        print(f"Creating NewsStream with keywords: {config['news']['keywords']}")
        news_stream = NewsDataStream(
            api_key=config['news']['api_key'],
            kafka_producer=producer,
            topic=config['kafka']['raw_topic'],
            keywords=config['news']['keywords'],
            languages=config['news']['languages'],
        )

        print("Starting News Stream.......")
        news_stream.stream(update_interval=config['news']['update_interval'])
    except Exception as e:
        print(f"Error starting news stream: {e}")
        import traceback
        print(traceback.format_exc())

def main():
    config_path = get_absolute_path('config/config.yaml')
    print(f"Using config: {config_path}")
    
    # run stream in a separate thread
    stream_thread = threading.Thread(
        target=run_news_stream, 
        args=(config_path,)
    )
    stream_thread.start()

    preprocessor = NewsPreprocessor(config_path)
    preprocessor.preprocess_news()


if __name__ == "__main__":
    main()

