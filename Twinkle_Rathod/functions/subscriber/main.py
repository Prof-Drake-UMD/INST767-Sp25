from transform_subscriber import transform_from_pubsub

#pubsub triggered cloud function entry point
def main(event, context):
    transform_from_pubsub(event, context)