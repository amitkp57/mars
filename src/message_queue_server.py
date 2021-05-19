import argparse
import json

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from flask import Response
from flask import request

from src import raft, rest_client

SCHEDULER_INTERVAL = 1
app = Flask(__name__)
node = None
queue = []

SUCCESS_RESPONSE = json.dumps({'status': 'succeeded'})
FAIL_RESPONSE = json.dumps({'status': 'succeeded'})

scheduler = BackgroundScheduler()


def run_background_tasks():
    """
    1. if node is leader, it needs to send heartbeats periodically.
    2. if node is follower and didn't receive any heartbeat with the timeout period, it starts leader election process.
    """
    if node.is_leader():
        send_heartbeats()

    if node.check_heartbeat_timeout():
        initiate_leader_election()

    return


def send_heartbeats():
    """
    Send heartbeats to follower nodes. Ignore failures.
    :return:
    """
    print(f'sending heartbeats to follower nodes.')
    for sibling_server in node.sibling_nodes:
        try:
            response = rest_client.post(sibling_server, '/heartbeats/heartbeat', {})
        except Exception as e:
            print(f'sending heartbeat to {sibling_server} failed with: {e}')
    return


def initiate_leader_election():
    print(f'initiating leader election!')
    votes = 1  # vote for itself

    # change to candidate role
    node.transition_to_new_role(raft.Role.CANDIDATE)
    for sibling_server in node.sibling_nodes:
        try:
            response = rest_client.post(sibling_server, '/election/vote', {})
            votes += 1
        except Exception as e:
            print(f'vote call to {sibling_server} failed with: {e}')

    print(f'received {votes}/{node.total_nodes} votes.')
    if votes > node.total_nodes / 2:
        node.transition_to_new_role(raft.Role.LEADER)
    else:
        node.transition_to_new_role(raft.Role.FOLLOWER)
    return


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
        return Response(FAIL_RESPONSE, status=403, mimetype='application/json')


@app.route('/messageQueue/message', methods=['PUT'])
def put_message():
    """
    Adds the message to end of the queue.
    :return:
    """
    message = json.loads(request.get_data().decode('utf-8'))
    queue.append(message)
    return Response(SUCCESS_RESPONSE, status=201, mimetype='application/json')


@app.route('/heartbeats/heartbeat', methods=['POST'])
def post_heartbeat():
    """
    Leader calls this endpoint to send periodic heartbeats.
    :return:
    """
    node.reset_last_heartbeat() # reset heartbeat timer of the node
    return Response(SUCCESS_RESPONSE, status=200, mimetype='application/json')


@app.route('/logs/sync', methods=['POST'])
def sync_logs():
    """
    Leader sends logs data for syncing.
    :return:
    """
    return Response(SUCCESS_RESPONSE, status=200, mimetype='application/json')


@app.route('/election/vote', methods=['POST'])
def vote_leader():
    """
    A candidate calls this api for requesting votes for leader election.
    :return:
    """
    return Response(SUCCESS_RESPONSE, status=200, mimetype='application/json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("host", type=str, help="host of the message queue server")
    parser.add_argument("port", type=str, help="port of the message queue server")
    parser.add_argument("sibling_nodes", nargs='+', help="other nodes addresses")
    args = parser.parse_args()
    node = raft.Node(args.sibling_nodes)
    scheduler.add_job(func=run_background_tasks, trigger="interval", seconds=SCHEDULER_INTERVAL)
    scheduler.start()
    app.run(host=args.host, port=args.port)
