import logging
import time

from azure.eventhub import EventHubClient, EventData

##########################################################
###### this script works with azure-eventhub==1.3.2 ######
##########################################################

logger = logging.getLogger("azure")

# Address can be in either of these formats:
# "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<mynamespace>.servicebus.windows.net/myeventhub"
# "amqps://<mynamespace>.servicebus.windows.net/myeventhub"
# For example:

ADDRESS = 'amqps://ML-Lab.servicebus.windows.net/mllab-sample-topic'

# ADDRESS = 'Endpoint=sb://ml-lab.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=s1aIs1ku9Krvg98Zm5VrJ8tRKZUyM4Fq7MOGVqRXqmg=' 
# "amqps://<EVENTHUBS NAMESPACE NAME>.servicebus.windows.net/<EVENTHUB NAME>"

# SAS policy and key are not required if they are encoded in the URL
USER = "RootManageSharedAccessKey"
KEY = "s1aIs1ku9Krvg98Zm5VrJ8tRKZUyM4Fq7MOGVqRXqmg="

try:
    if not ADDRESS:
        raise ValueError("No EventHubs URL supplied.")

    # Create Event Hubs client
    client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
    sender = client.add_sender(partition="0")
    client.run()
    try:
        start_time = time.time()
        for i in range(100):
            print("Sending message: {}".format(i))
            message = "Message {}".format(i)
            sender.send(EventData(message))
    except:
        raise
    finally:
        end_time = time.time()
        client.stop()
        run_time = end_time - start_time
        logger.info("Runtime: {} seconds".format(run_time))

except KeyboardInterrupt:
    pass
