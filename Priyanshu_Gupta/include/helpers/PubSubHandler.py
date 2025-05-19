
import logging
import json
from google.cloud import pubsub_v1
from include.helpers.StorageClients import MinioClient, GCSClient
from airflow.hooks.base import BaseHook
import os



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class PubSubHandler:
    """
    Description:
    A class to send data to pubsub and store it in GCS storage.
    """

    airflow_connection_id = 'google_cloud_default'

    def __init__(self, project_id):

        self.project_id = project_id
        self.connection = self._get_client_connection()
        self.publisher = self._get_pubsub_client()

    def _get_client_connection(self):
        """
        Retrieve GCP connection credentials from Airflow.
        """
        connection = BaseHook.get_connection(self.airflow_connection_id)
        extra = connection.extra_dejson
        if 'project' not in extra:
            raise ValueError("Missing GCP project in Airflow connection extras.")
        
        extra = connection.extra_dejson
        keyfile_dict = json.loads(extra.get('keyfile_dict'))

        if not keyfile_dict:
            raise ValueError("GCP service account key is not configured properly in Airflow.")

        credentials_path = "/tmp/gcp_key.json"
        with open(credentials_path, "w") as f:
            json.dump(keyfile_dict, f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        logger.info("GCP connection is retrieved successfully.")
        return connection
    

    def _get_pubsub_client(self):
        """
        Create and return a GCS client using the service account key.
        """
        

        publisher = pubsub_v1.PublisherClient()

        logger.info("Google Cloud Storage Client Initialized Successfully.")
        return publisher



    def publish_to_pubsub(self, topic_id,  data) -> dict: 
        """
        Publishes fetched event data to a Pub/Sub topic.
        
        Parameters:
        data and atrributes of the message
        """

        data = json.dumps(data).encode('utf-8')

        try:
            # Publish the message to the topic
            topic_path = self.publisher.topic_path(self.project_id, topic_id)
            future = self.publisher.publish(topic_path, data)
            logger.info(f"Published message to {topic_path}, Message ID: {future.result()}")
        except Exception as e:
            logger.error(f"Error publishing message to Pub/Sub: {e}")
            raise
        
        # Wait for the message to be successfully published
        future.result()

        return future


    def fetch_pubsub_message(self, subscription_id):
       

        # pulling the message from pubsub
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(self.project_id, subscription_id)

        response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 100,
        },
        timeout=60
        )

        if not response.received_messages:
            logger.info("No messages found in the subscription.")
            return None
        
        received_message = response.received_messages[0]
        

        message_data = received_message.message.data.decode("utf-8")
        json_data = json.dumps(json.loads(message_data))
        
        attributes = dict(received_message.message.attributes)
        print("Message pulled with:", attributes)

        # Acknowledge the message so it's not redelivered
        subscriber.acknowledge(
            request={
                "subscription": subscription_path,
                "ack_ids": [received_message.ack_id],
            }
        )

        #logger.info(f"Received message ID: {message.message_id}")
        logger.info(f"Message acknowledged.")
