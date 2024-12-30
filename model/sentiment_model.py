from kafka import KafkaConsumer, KafkaProducer
import torch
import torch.nn as nn
from datetime import datetime
from transformers import BertTokenizer, BertModel
import json
import logging
import yaml

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_path='../config/config.yaml'):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# Sentiment Analysis Model:
class SentimentAnalysis(nn.Module):
    def __init__(self):
        super(SentimentAnalysis, self).__init__()
        self.bert = BertModel.from_pretrained('bert-base-uncased')
        self.dropout = nn.Dropout(0.1)
        self.fc = nn.Linear(768, 3) #3 classe because of pos, neg, neutral
    
    def forward(self, input_ids, attention_mask):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        pooled_output = outputs[1]
        x = self.dropout(pooled_output)
        return self.fc(x)
    
# Kafka Consumer for News Analysis
class NewsAnalysis:
    def __init__(self, config_path='../config/config.yaml'):
        self.config=load_config(config_path)
        self.consumer = KafkaConsumer(
            self.config['kafka']['cleaned_topic'],
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id="news_sentiment_group"

        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.output_topic = "news_sentiment"
        
        # Create model components
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        logger.info(f"Using device: {self.device}")

        self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        self.model = SentimentAnalysis().to(self.device)

        # self.model.load_state_dict(torch.load('sentiment_model.pth'))
        self.model.eval()
    
    def analyze_sentiment(self, text):
        try:
            encoded = self.tokenizer.encode_plus(
                text,
                max_length=512,
                truncation=True,
                padding='max_length',
                return_tensors='pt'
            )

            with torch.no_grad():
                input_ids=encoded['input_ids'].to(self.device)
                attention_mask=encoded['attention_mask'].to(self.device)
                outputs=self.model(input_ids, attention_mask)
                probs = torch.softmax(outputs, dim=1)
                _, predicted = torch.max(outputs, 1)
            
            sentiment_map = {0: 'negative', 1: 'neutral', 2: 'positive'}
            confidence = probs[0][predicted.item()].item()
            return {
                "sentiment": sentiment_map[predicted.item()],
                "confidence": round(confidence, 4)
            }
        except Exception as ex:
            logger.error(f"Error in sentiment analysis: {str(ex)}")
            return {'sentiment': 'neutral', 'confidence': 0.0}

    def process_message(self, news_item):
        """Process newsAPI messages"""
        try:
            title = news_item.get('title', '')
            description = news_item.get('description', '')
            content = news_item.get('content', '')


            text_to_analyze = f"{title} {description} {content}".strip()
            sentiment_result = self.analyze_sentiment(text_to_analyze)

            enriched_item = {
                **news_item,
                'sentiment_analysis': {
                    'sentiment': sentiment_result['sentiment'],
                    'confidence': sentiment_result['confidence'],
                    'analyzed_at': datetime.utcnow().isoformat(),
                }
            }

            return enriched_item
        except Exception as ex:
            logger.error(f"Error processing news item: {str(ex)}")
            return None
    
    def run(self):
        logger.info(f"Starting NewsAPI Sentiment Analysis Consumer on topic: {self.config['kafka']['cleaned_topic']}")
        try:
            for message in self.consumer:
                try:
                    news_item = message.value
                    logger.info(f"Received message: {news_item.get('title', 'No title')}")

                    enriched_item = self.process_message(news_item)
                    if enriched_item:
                        self.producer.send(
                            self.output_topic,
                            enriched_item
                        )
                    self.producer.flush()
                    logger.info(f"Processed and sent item {enriched_item.get('title', 'No title')}")

                except Exception as ex:
                    logger.error(f"Error processing message: {str()}")
                    continue
        except Exception as ex:
            logger.error(f"Fatal error in consumer: {str(ex)}")
            raise


def main():
    analyzer = NewsAnalysis()
    analyzer.run()

if __name__ == "__main__":
    main()

