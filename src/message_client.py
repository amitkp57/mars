import argparse
import json

import requests as requests

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("host", type=str, help="host of the message queue server")
    parser.add_argument("port", type=str, help="port of the message queue server")
    args = parser.parse_args()

    URL = f'http://{args.host}:{args.port}/messageQueue/message'
    message = {'name': 'kyle', 'email': 'chard@uchicago.edu'}

    response = requests.put(URL, json.dumps(message))
    print(f'Response: {response.status_code}')

    response = requests.get(URL)
    print(f'Response: {response.status_code}')
    print(f'Response: {response.json()}')

    response = requests.get(URL)
    print(f'Response: {response.status_code}')
