import pika
import requests


class RabbitCleaner(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('10.224.91.104'))
        self.channel = self.connection.channel()

    def clear_queues(self):
        total = 0
        for queue in self.all_test_queues():
            if 'test' in queue.lower():
                print('deleting ', queue)
                self.channel.queue_delete(queue)
                total += 1
        print("deleted queues = %s" % total)

    def all_test_queues(self):
        for page in self.all_queue_pages():
            items = page.get('items')
            for item in items:
                yield item.get('name')

    def all_queue_pages(self):
        for page in range(1, 100):
            response = requests.get(
            "http://RABBIT-HOSTNAME:PORT/api/queues?page="+ str(page) +"&page_size=100&name=&use_regex=false&pagination=false",
            auth=('guest', 'guest'))
            if response.status_code == 200:
                yield response.json()
            else:
                break


if __name__ == '__main__':
    rc = RabbitCleaner()
    rc.clear_queues()
