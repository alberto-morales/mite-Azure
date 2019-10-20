import logging
import time

from azure.eventhub import EventHubClient, EventData

logger = logging.getLogger("azure")

from miteazure.eventHubs.configuration import EVENT_HUB_CONNECTION_STR, \
                                              EVENT_HUB_PATH

try:
    if not EVENT_HUB_CONNECTION_STR:
        raise ValueError("No EventHubs CONNECTION STRING supplied.")

    # Create Event Hubs client
    client = EventHubClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STR, event_hub_path=EVENT_HUB_PATH, debug=False)
    producer = client.create_producer(partition_id="0")
    try:
        start_time = time.time()
        for i in range(3):
            print("Sending message: {}".format(i))
            message = "Message {}".format(i)
            producer.send(EventData(message))
            print(f"Message [[[{message}]]] sent.")
    except:
        raise
    finally:
        end_time = time.time()
        run_time = end_time - start_time
        print(f"Runtime: {run_time} seconds")
        producer.close()
        client.close()

except KeyboardInterrupt:
    pass
