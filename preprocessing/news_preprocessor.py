import json
import re
import yaml
from kafka import KafkaConsumer, KafkaProducer


class NewsPreprocessor:
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)

        self.consumer = KafkaConsumer(
            self.config['kafka']['raw_topic'],
            bootstrap_servers = self.config['kafka']['bootstrap_servers'],
            value_deserializer = lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset = 'earliest'
        )

        self.producer = KafkaProducer(
            bootstrap_servers = self.config['kafka']['bootstrap_servers'],
            value_serializer = lambda x: json.dumps(x).encode('utf=8'),
        )

    def clean_text(self, text):
        """ Clean the tweet text based on the config """
        if not text:
            return ""
        if self.config['preprocessing']['remove_urls']:
            text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        if self.config['preprocessing']['remove_special_characters']:
            text = re.sub(r'[^\w\s]', ' ', text)

        if self.config['preprocessing']['remove_extra_spaces']:
            text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def preprocess_news(self):
        """ Preprocess the news """
        print("Starting news preprocessing.......")

        for message in self.consumer:
            article = message.value
            try:
                processed_article = {
                    'id': article['id'],
                    'title': self.clean_text(article['title']),
                    'description': self.clean_text(article['description']),
                    'content': self.clean_text(article['content']),
                    'source': article['source'],
                    'url': article['url'],
                    'combined_text': ''
                }

                processed_article['combined_text'] = ' '.join([
                    processed_article['title'], 
                    processed_article['description'], 
                    processed_article['content']
                ]).strip()

                self.producer.send(
                    self.config['kafka']['cleaned_topic'],
                    processed_article
                )
                print(f"Preprocessed News: {processed_article['title'][:50]}...")
                
            except Exception as ex:
                print(f"Error processing article: {ex}")
