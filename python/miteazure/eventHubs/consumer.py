import asyncio
from azure.eventhub.aio import EventHubClient
from azure.eventhub.aio.eventprocessor import EventProcessor, PartitionProcessor
from azure.storage.blob.aio import ContainerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobPartitionManager

from miteazure.eventHubs.configuration import EVENT_HUB_CONNECTION_STR, \
                                              STORAGE_CONTAINER_CONNECTION_STR, \
                                              STORAGE_CONTAINER_PATH

class MyPartitionProcessor(PartitionProcessor):
    async def process_events(self, events, partition_context):
        print("entrara por lo menos una vez en el if events")
        if events:
            for event in events:
                #  code to process events
                print(f"Procesado ==> {event}")
                # sleep is not necesary, only for debug purpose
                # await asyncio.sleep(1)
                # saving checkpoint to the data store (one event at a time)
                await partition_context.update_checkpoint(event.offset, event.sequence_number)
            # saving checkpoint to the data store (whole block)
            # await partition_context.update_checkpoint(events[-1].offset, events[-1].sequence_number)

async def main():
    eventhub_client = EventHubClient.from_connection_string(EVENT_HUB_CONNECTION_STR, receive_timeout=5, retry_total=3)
    storage_container_client = ContainerClient.from_connection_string(STORAGE_CONTAINER_CONNECTION_STR, STORAGE_CONTAINER_PATH)
    partition_manager = BlobPartitionManager(storage_container_client)  # use the BlobPartitonManager to save
    event_processor = EventProcessor(eventhub_client, "mllab-consumers-group", MyPartitionProcessor, partition_manager)
    print("Starting...")
    async with storage_container_client:
        asyncio.ensure_future(event_processor.start())
        await asyncio.sleep(20)  # run for a while
        await event_processor.stop()

if __name__ == '__main__':
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    asyncio.run(main())
