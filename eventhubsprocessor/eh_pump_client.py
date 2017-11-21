# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# -----------------------------------------------------------------------------------

import sys
import logging
import time
import asyncio
import multiprocessing as mp
from eventhubs import EventHubClient, Receiver, Offset

class EventHubPumpClient:
    """
    A wrapper of the event hub client that runs in a sub proc. Has a async recieve method for 
    pulling from the event client running in a sub process
    """
    def __init__(self, address):
        self.address = address 
        self.client_process = None
        self.message_queue = None
        self.partition_id = None
        self.eh = None
        
    def __run_client(self, address, consumer_group, offset, partition_id, message_queue):
        """
        Starts a new client in the partitions and pushes messages to the message queue
        """
        # from eventhubs import EventHubClient
        self.eh = EventHubClient(address).subscribe(EventHubPumpReceiver(partition_id, message_queue, self.eh),
                                                    consumer_group, partition_id, offset)
        self.eh.run()

    async def subscribe(self, consumer_group, partition_id, offset):
        """
        Subscribes to a given partition, offset and consumer group 
        inits message queue
        """
        self.message_queue = mp.Queue()
        self.partition_id = partition_id
        self.client_process = mp.Process(target=self.__run_client, args=(self.address,), 
                                         kwargs={'consumer_group':consumer_group,
                                                 'offset': offset, 
                                                 'partition_id': partition_id,
                                                 'message_queue': self.message_queue})

    async def run(self):
        """
        starts the processor thread
        """
        self.client_process.start()

    async def stop(self):
        """
        Stops the background process and shuts down
        Todo: find cleaner way to shut down client in sub_proc
        """
        self.eh.stop()
        self.client_process.terminate()

    async def receive(self, n):
        """
        Recieves n messges from the message queue and returns them to the 
        pump. 
        """
        message_count = 0
        messages = []
        while message_count < n:
            message = self.message_queue.get() 
            messages.append(message) 
            message_count += 1

        return messages

class EventHubPumpReceiver(Receiver):
    
    def __init__(self, partition, message_queue, client):
        super(EventHubPumpReceiver, self).__init__()
        self.partition = partition
        self.message_queue = message_queue

    def on_event_data(self, event_data):
        """
        Take event serializie it and add it to the message queue 
        """
        message = {'offset': event_data.offset,
                   'sequence_number': event_data.sequence_number,
                   'body': event_data.body}
        self.message_queue.put(message)
