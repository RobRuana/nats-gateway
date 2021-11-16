import asyncio
import json
import nats
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


class NATSClient:

    def __init__(self, loop=None):
        self.loop = loop if loop else asyncio.new_event_loop()

    def arun(self, coro):
        if self.loop.is_running():
            task = self.loop.create_task(coro)
            while not task.done() and not task.cancelled():
                self.loop._run_once()
            return task.result()
        else:
            return self.loop.run_until_complete(coro)

    def is_running(self):
        return self.loop.is_running()

    def run_forever(self):
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            if not self.client.is_closed:
                self.loop.run_until_complete(self.client.close())
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)

    def connect(self, server):
        self.client = self.arun(nats.connect(servers=[server]))

    def close(self):
        self.arun(self.client.close())

    def get_response_subject(self, subject, transaction_id):
        return f"{subject}_response"
        # Use this if we want per-transaction subjects...
        # return f"{subject}_response_{transaction_id}"

    def publish_message(self, subject, message):
        """
        Publish message without waiting for response.
        """
        payload = json.dumps({"message": message}).encode()
        self.arun(self.client.publish(subject, payload))

    def send_request(self, subject, message, response_subject=None, transaction_id=None):
        """
        Request/Response, blocking, response returned by function.
        """
        if not transaction_id:
            transaction_id = str(uuid.uuid4())
        if not response_subject:
            response_subject = self.get_response_subject(subject, transaction_id)
        payload = json.dumps({
            "message": message,
            "transaction_id": transaction_id,
            "subject": subject,
            "response_subject": response_subject,
        }).encode()

        future = self.loop.create_future()

        async def handle_response(payload):
            data = json.loads(payload.data)
            if data['transaction_id'] == transaction_id:
                future.set_result(payload)

        subscription = self.arun(self.client.subscribe(response_subject, cb=handle_response))
        self.arun(self.client.publish(subject, payload))

        while not future.done() and not future.cancelled():
            self.loop._run_once()

        self.arun(subscription.unsubscribe())

        result = future.result()
        return json.loads(result.data)

    def add_request_handler(self, subject, callback):
        """
        For services to add request handlers
        """
        self.arun(self.client.subscribe(subject, cb=self.wrap_request_handler(callback)))

    def wrap_request_handler(self, callback):
        async def _request_handler(payload):
            data = json.loads(payload.data)
            transaction_id = data['transaction_id']

            result = callback(data)

            response_subject = self.get_response_subject(payload.subject, transaction_id)
            response_payload = json.dumps({
                "message": result,
                "subject": payload.subject,
                "transaction_id": transaction_id,
                "response_subject": response_subject,
            }).encode()
            await self.client.publish(response_subject, response_payload)

        return _request_handler

class EchoService:
    def __init__(self, nats_url, subject="echo"):
        self.nats_client = NATSClient()
        self.nats_url = nats_url
        self.subject = subject

    def is_running(self):
        return self.nats_client.is_running()

    def run(self):
        self.nats_client.connect(self.nats_url)
        self.nats_client.add_request_handler(self.subject, self.handle_request)
        self.nats_client.run_forever()

    def stop(self):
        self.nats_client.stop()

    def handle_request(self, payload):
        print(f"Echoing: {payload['message']}")
        # Do work here...
        return payload['message']


def main():
    NATS_URL = "nats://localhost:4222"
    NATS_SUBJECT = "echo"

    # Run service in separate thread...
    echo_service = EchoService(NATS_URL, NATS_SUBJECT)
    executor = ThreadPoolExecutor()
    echo_service_task = executor.submit(echo_service.run)


    # Wait until service is running...
    while not echo_service.is_running():
        time.sleep(0.1)


    # Send some messages...
    nats_client = NATSClient(asyncio.get_event_loop())
    nats_client.connect(NATS_URL)

    for i in range(4):
        print(f"Sending: HELLO {i}")
        response = nats_client.send_request(NATS_SUBJECT, f"HELLO {i}")
        print(f"Received: {response}")
        time.sleep(1)


    # Clean up lingering NATSClient tasks...
    nats_client.close()
    tasks = asyncio.Task.all_tasks()
    for t in [t for t in tasks if not (t.done() or t.cancelled())]:
        t.cancel()


    # Stop EchoService...
    echo_service.stop()
    echo_service_task.result()  # Probably not needed?



if __name__ == "__main__":
    main()
