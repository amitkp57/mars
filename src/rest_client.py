import json

import requests as requests


def get(hostname, service_url):
    URL = f'http://{hostname}/{service_url}'
    response = requests.get(URL)
    if response.status_code not in [200, 201]:
        raise Exception(response)
    return response.json()


def post(hostname, service_url, data):
    URL = f'http://{hostname}/{service_url}'
    response = requests.post(URL, json.dumps(data))
    if response.status_code not in [200, 201]:
        raise Exception(response)
    return response.json()


def put(hostname, service_url, data):
    URL = f'http://{hostname}/{service_url}'
    response = requests.put(URL, json.dumps(data))
    if response.status_code not in [200, 201]:
        raise Exception(response)
    return response.json()


if __name__ == '__main__':
    print(put('localhost:3441', '/messageQueue/message', {}))
    print(get('localhost:3441', '/messageQueue/message'))
    print(post('localhost:3441', '/heartbeats/heartbeat'))
