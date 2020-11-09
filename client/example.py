import requests
import time
import logging


class TaskConsumer:
    server_url: str
    queue_name: str

    def __init__(self, server_url):
        self.server_url = server_url

    def peek(self, queue_name, cursor=None, size=1, timeout=5):
        res = requests.post(
            f'{self.server_url}/task/peek',
            json={
                'queue': queue_name,
                'cursor': cursor,
                'size': size,
                'timeout': timeout,
            },
            timeout=timeout + 5,
        )
        data = res.json()['data']
        cursor = res.json()['cursor']
        return data, cursor

    def acquire(self, queue_name, peek_data, prt=30):
        res = requests.post(
            f'{self.server_url}/task/acquire',
            json={
                'queue': queue_name,
                'ids': [i['id'] for i in peek_data],
                'promise_reply_time': prt,
            })
        return res.json()['data']

    def reply(self, queue_name, id, info, content):
        res = requests.post(
            f'{self.server_url}/task/reply',
            json={
                'queue': queue_name,
                'id': id,
                'info': info or {},
                'content': content or '',
            }
        )
        return res.json()['data']

    def mainloop(self):
        queue_name = self.queue_name
        cursor = None
        while True:
            try:
                # peek
                data, cursor = self.peek(queue_name, cursor)
                if not data:
                    continue
                data = [i for i in data if self.accept(i)]
                data = self.acquire(queue_name, data)
                for i in data:
                    try:
                        info, content = self.process(i)
                        self.reply(queue_name, i['id'], info, content)
                    except Exception as e:
                        logging.exception(e)
            except Exception as e:
                logging.exception(e)
                time.sleep(5)
        return

    def accept(self, data):
        return True

    def process(self, data):
        return {"msg": "ok"}, ''


if __name__ == "__main__":
    tc_endpoint = "http://127.0.0.1:8000"
    c = TaskConsumer(tc_endpoint)
    c.queue_name = 'test'
    c.mainloop()
