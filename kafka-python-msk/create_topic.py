from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import socket
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import os
import dotenv

dotenv.load_dotenv()


AWS_REGION = os.getenv("AWS_REGION")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")

if not AWS_REGION or not BOOTSTRAP_SERVERS:
    raise ValueError("Missing AWS_REGION or BOOTSTRAP_SERVERS environment variables")

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION, aws_debug_creds=True)
        return token

tp = MSKTokenProvider()

# Example Usage
topic = NewTopic(
    name="steam-data",
    num_partitions=1,
    replication_factor=1
)

admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS.split(","), security_protocol="SASL_SSL", sasl_mechanism="OAUTHBEARER", sasl_oauth_token_provider=tp, client_id=socket.gethostname())

response = admin_client.create_topics([topic])


print("Response: ",  response)