from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import socket
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import os
import  dotenv

dotenv.load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION, aws_debug_creds=True)
        return token

tp = MSKTokenProvider()

# Example Usage
topic = NewTopic(
    name="my_topic",
    num_partitions=1,
    replication_factor=1
)

admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS.split(","), security_protocol="SASL_SSL", sasl_mechanism="OAUTHBEARER", sasl_oauth_token_provider=tp, client_id=socket.gethostname())

metadata = admin_client.list_topics()

print("Available Topics:", metadata)