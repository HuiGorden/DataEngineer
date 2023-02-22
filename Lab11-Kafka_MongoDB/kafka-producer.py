# This Script acts as Kafka producer. It pulls data from Wiki Media API and send the retrieved data to Kafa cluster.
# When pulls data from Wiki Media API, it uses sseclient SDK
# When sends data to Kafka cluster, it uses kafka SDK
import json
import argparse
from sseclient import SSEClient
from confluent_kafka import Producer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

if __name__ == "__main__":

    # parse command line arguments
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')
    parser.add_argument('--bootstrap_server', default='localhost:9092', help='Kafka bootstrap broker(s) (host[:port])', type=str)
    parser.add_argument('--topic_name', default='wikipedia', help='Destination topic name', type=str)
    parser.add_argument('--acks', default='all', help='Kafka Producer acknowledgment', type=str)
    parser.add_argument('--events_to_produce', help='Kill producer after n events have been produced', type=int, default=100)
    args = parser.parse_args()

    # init Kafka producer
    producer = Producer({
         "bootstrap.servers": args.bootstrap_server
    })

    messages_count = 0
    for event in SSEClient("https://stream.wikimedia.org/v2/stream/recentchange"):
        producer.poll(0)
        if event.event == 'message':
            try:
                json.loads(event.data)
                producer.produce(args.topic_name, event.data.encode('utf-8'), on_delivery=delivery_report)

                messages_count += 1
            except:
                pass

        if messages_count >= args.events_to_produce:
            print(f'Producer will be killed as {args.events_to_produce} events were producted')
            exit(0)
