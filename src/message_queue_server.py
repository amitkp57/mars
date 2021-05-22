import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from flask import Flask
from flask import Response
from flask import request

from src import raft, rest_client
from src.log import LogEntry
from src.raft import Role

SCHEDULER_INTERVAL = 0.01  # 100 milliseconds
app = Flask(__name__)
node = None
queue = []

SUCCESS_RESPONSE = json.dumps({'status': 'succeeded'})
FAIL_RESPONSE = json.dumps({'status': 'succeeded'})

executor = ThreadPoolExecutor(max_workers=1)


def run_background_tasks():
    """
    1. if node is leader, it needs to send heartbeats and log entries for syncing followers' logs with its own.
    2. if node is follower and didn't receive any heartbeat with the timeout period, it starts leader election process.
    3. Apply committed logs to the state machines
    4. Leaders need to increase it's committed index once more than half of the nodes replicate a log entry.
    """
    while True:
        if node.is_leader():
            append_entries()
            update_committed_index()

        if node.check_heartbeat_timeout():
            initiate_leader_election()

        apply_state_machine()
        time.sleep(SCHEDULER_INTERVAL)
    return


def apply_state_machine():
    """
    Apply commands to the state machine i.e. get/put messages from/into queue
    :return:
    """
    with node.thread_lock:
        if node.committed_index > node.last_applied:
            node.increment_last_applied()
            # TODO update queues
    return


def append_entries():
    """
    Send requests for appending log entries to follower nodes. Ignore failures.
    :return:
    """
    print(f'Sending append messages to the follower nodes.')
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for sibling_server in node.sibling_nodes:
            data = {
                'term': node.term,
                'leaderId': node.index,
                'prevLogTerm': -1,
                'prevLogIndex': -1,
                'entry': None,
                'leaderCommit': node.committed_index
            }
            curr_index = node.next_index[sibling_server]
            if curr_index > 0:
                prev_index = curr_index - 1
                data['prevLogTerm'] = node.logs.entries[prev_index].term
                data['prevLogIndex'] = prev_index
                if curr_index < node.logs.log_size:
                    data['entry'] = node.logs.entries[curr_index].json_encode()
            promise = executor.submit(rest_client.post, sibling_server, 'logs/append', data)
            futures[promise] = sibling_server
        for promise in as_completed(futures):
            try:
                response = promise.result()
                server = futures[promise]
                curr_index = node.next_index[server]
                if response['term'] > node.term:  # received response from node with higher term
                    update_term_return_to_follower(response['term'])
                    break
                if response['success']:
                    node.match_index[server] = curr_index - 1
                    if curr_index < node.logs.log_size:
                        node.next_index[server] += 1
                else:
                    node.next_index[server] -= 1
            except Exception as e:
                print(f'Sending heartbeats to {sibling_server} failed with: {e}.')
    return


def update_committed_index():
    """
    Update committed index.
    :return:
    """
    if node.logs.log_size - 1 == node.committed_index:
        return
    count = 1
    for value in node.match_index.values():
        if value > node.committed_index:
            count += 1
    if count > (node.total_nodes / 2):
        node.committed_index += 1
    return


def update_term_return_to_follower(term):
    """
    Return to follower state and update term.
    :param term:
    :return:
    """
    node.term = term
    node.transition_to_new_role(Role.FOLLOWER)
    print(f'Received response from a higher term node. Returning to follower state.')
    return


def initiate_leader_election():
    """
    Initiates leader election. Votes for itself and sends requests other nodes fro vote. If it receives more than half
    of the votes, it promotes itself to leader.
    :return:
    """
    print(f'Initiating leader election.')
    votes = 1  # vote for itself
    # change to candidate role
    node.transition_to_new_role(raft.Role.CANDIDATE)
    node.increment_term()
    node.voted_for = None

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        data = {
            'term': node.term,
            'candidateId': node.index,
            'lastLogTerm': node.logs.entries[-1].term,
            'lastLogIndex': node.logs.log_size - 1
        }
        for sibling_server in node.sibling_nodes:
            future = executor.submit(
                rest_client.post, sibling_server, 'election/vote', data)
            futures[future] = sibling_server

        while futures and not node.check_heartbeat_timeout():
            retries = {}
            for future in as_completed(futures):
                try:
                    response = future.result()
                    if response['vote']:
                        votes += 1
                    elif response['term'] > node.term:
                        update_term_return_to_follower(response['term'])
                        return
                except Exception as e:
                    print(f'Vote call to {sibling_server} failed with: {e}.')
                    retry = executor.submit(rest_client.post, futures[future], 'election/vote', data)
                    retries[retry] = sibling_server
            print(f'Received {votes}/{node.total_nodes} votes.')
            if votes > node.total_nodes / 2 and node.role == Role.CANDIDATE:
                node.transition_to_new_role(raft.Role.LEADER)
                return
            futures = retries

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


#
# @app.route('/heartbeats/heartbeat', methods=['POST'])
# def post_heartbeat():
#     """
#     Leader calls this endpoint to send periodic heartbeats.
#     :return:
#     """
#     message = json.loads(request.get_data().decode('utf-8'))
#     requester_term = message['term']
#     if requester_term >= node.term:
#         node.term = requester_term
#         node.voted_for = None
#         output = {'term': node.term}
#         node.transition_to_new_role(Role.FOLLOWER)  # Received response from leader. Go back to follower state.
#         node.reset_last_heartbeat()  # reset heartbeat timer of the node
#         return Response(json.dumps(output), status=200, mimetype='application/json')
#     else:
#         output = {'term': node.term}  # requesting node has lower term. Inform it to go back to follower state.
#         return Response(json.dumps(output), status=200, mimetype='application/json')


@app.route('/logs/append', methods=['POST'])
def sync_logs():
    """
    Leader sends logs data for syncing.
    :return:
    """
    message = json.loads(request.get_data().decode('utf-8'))
    term = message['term']
    leader_id = message['leaderId']
    prev_log_term = message['prevLogTerm']
    prev_log_index = message['prevLogIndex']
    entry = LogEntry.json_decode(message['entry'])
    leader_commit = message['leaderCommit']
    output = {'term': node.term}
    if term < node.term:
        output['success'] = False
        return output
    node.leader = leader_id
    update_term_return_to_follower(term)
    if prev_log_index != -1 and (
            node.logs.log_size <= prev_log_index or node.logs.entries[prev_log_index].term != prev_log_term):
        output['success'] = False
        return output
    if node.logs.log_size > prev_log_index + 1 and node.logs.entries[prev_log_index + 1] != entry:
        node.logs.delete_entries_from(prev_log_index + 1)
    if entry:
        node.logs.append(entry)
    if leader_commit > node.committed_index:
        node.committed_index = min(leader_commit, node.logs.log_size - 1)
    output['success'] = True
    return output


@app.route('/election/vote', methods=['POST'])
def vote_leader():
    """
    A candidate calls this api for requesting votes for leader election.
    :return:
    """
    message = json.loads(request.get_data().decode('utf-8'))
    requester_term = message['term']
    candidate_id = message['candidateId']
    last_log_idx = message['lastLogIndex']
    last_log_term = message['lastLogTerm']

    # Log inconsistency
    if node.logs.entries[-1].term > last_log_term or (
            node.logs.entries[-1].term == last_log_term and node.logs.log_size > last_log_idx + 1):
        output = {'vote': False, 'term': node.term}
        return Response(json.dumps(output), status=200, mimetype='application/json')

    # update term and move to follower state
    if requester_term > node.term:
        update_term_return_to_follower(requester_term)
        node.voted_for = candidate_id
        output = {'vote': True, 'term': node.term}
        return Response(json.dumps(output), status=200, mimetype='application/json')
    elif requester_term == node.term and (not node.voted_for or node.voted_for == candidate_id):
        update_term_return_to_follower(requester_term)
        node.voted_for = candidate_id
        output = {'vote': True, 'term': node.term}
        return Response(json.dumps(output), status=200, mimetype='application/json')

    # vote no
    output = {'vote': False, 'term': node.term}
    return Response(json.dumps(output), status=200, mimetype='application/json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("path_to_config", type=str, help="path to server config file")
    parser.add_argument("index", type=int, help="index of the server to start")
    args = parser.parse_args()
    with open(args.path_to_config, 'r') as config_file:
        server_config = json.load(config_file)['addresses']
        nodes = [(server['ip'].removeprefix('http://'), server['port']) for server in server_config]
        sibling_nodes = [f'{node[0]}:{node[1]}' for idx, node in enumerate(nodes) if idx != args.index]
        node = raft.Node(args.index, sibling_nodes)
        executor.submit(run_background_tasks())
        print(
            f'Starting server {args.index} on {nodes[args.index][0]}:{nodes[args.index][1]} with sibling nodes as '
            f'{sibling_nodes}.')
        app.run(host=nodes[args.index][0], port=nodes[args.index][1])
