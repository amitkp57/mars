import json

import requests as requests


def get(hostname, service_url, timeout=10):
    URL = f'http://{hostname}/{service_url}'
    response = requests.get(URL, timeout=timeout)
    if response.status_code not in [200, 201]:
        raise Exception(response)
    return response.json()


def post(hostname, service_url, data, timeout=10):
    URL = f'http://{hostname}/{service_url}'
    response = requests.post(URL, json.dumps(data), timeout=timeout)
    if response.status_code not in [200, 201]:
        raise Exception(response)
    return response.json()


def put(hostname, service_url, data, timeout=10):
    URL = f'http://{hostname}/{service_url}'
    response = requests.put(URL, json.dumps(data), timeout=timeout)
    if response.status_code not in [200, 201]:
        raise Exception(response)
    return response.json()


if __name__ == '__main__':
    print(put('localhost:3441', '/messageQueue/message', {}))
    print(get('localhost:3441', '/messageQueue/message'))
    print(post('localhost:3441', '/heartbeats/heartbeat'))
