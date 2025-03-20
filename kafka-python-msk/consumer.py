from kafka import KafkaConsumer
from kafka.errors import KafkaError
import socket
import os
import dotenv
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka.sasl.oauth import AbstractTokenProvider

dotenv.load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")

if not AWS_REGION or not BOOTSTRAP_SERVERS:
    raise ValueError("Missing AWS_REGION or BOOTSTRAP_SERVERS environment variables")

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_authentication_token(
            region=AWS_REGION,
            aws_debug_creds=True
        )
        return token

tp = MSKTokenProvider()

try:
    consumer = KafkaConsumer(
        "stream-data",  
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=tp,
        client_id=socket.gethostname(),
        group_id="consumer-group-1", 
        auto_offset_reset="earliest"  
    )
except KafkaError as e:
    print(f"Failed to create Kafka consumer: {e}")
    exit(1)

print("Kafka Consumer started. Listening for messages...")

try:
    for message in consumer:
        print(f"Received: {message}")

except KeyboardInterrupt:
    print("\nInterrupted by user")

except Exception as e:
    print("Error consuming messages:", e)

finally:
    consumer.close()
    print("Kafka Consumer closed.")