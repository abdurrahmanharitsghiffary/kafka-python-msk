from kafka import KafkaProducer
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
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=tp,
        client_id=socket.gethostname(),
    )
except KafkaError as e:
    print(f"Failed to create Kafka producer: {e}")
    exit(1)

# Kafka Topic
topic = "stream-data"

# Start Producing Messages
try:
    while True:
        inp = input("> ")
        if inp.lower() == "exit":
            print("Exiting...")
            break

        producer.send(topic, inp.encode())
        producer.flush()
        print("Produced!")

except KeyboardInterrupt:
    print("\nInterrupted by user")

except Exception as e:
    print("Failed to send message:", e)

finally:
    producer.close()
    print("Kafka Producer closed.")
