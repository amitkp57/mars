import argparse
import json

from flask import Flask
from flask import Response
from flask import request

app = Flask(__name__)

queue = []


# TODO: topics
@app.route('/messageQueue/message', methods=['GET'])
def get_message():
    """
    Removes the first message in the queue and returns it to client (FIFO). If queue is empty, 403-not found response is
    returned.
    :return:
    """
    if queue:
        return Response(json.dumps(queue.pop(0)), status=200, mimetype='application/json')
    else:
        return Response(status=403, mimetype='application/json')


@app.route('/messageQueue/message', methods=['PUT'])
def put_message():
    """
    Adds the message to end of the queue.
    :return:
    """
    message = json.loads(request.get_data().decode('utf-8'))
    queue.append(message)
    return Response(status=201, mimetype='application/json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("host", type=str, help="host of the message queue server")
    parser.add_argument("port", type=str, help="port of the message queue server")
    args = parser.parse_args()
    app.run(host=args.host, port=args.port)
