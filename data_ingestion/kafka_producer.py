import yaml
from kafka import KafkaProducer

def create_producer(config_path):
    # creates kafka producer
    print(f"Loading config from: {config_path}")
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        producer = KafkaProducer(
            bootstrap_servers=config['kafka']['bootstrap_servers'],
            value_serializer=lambda x: x.encode('utf-8')
        )
        print("Successfully created Kafka producer")
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        raise
