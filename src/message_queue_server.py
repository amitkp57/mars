import argparse
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from flask import Flask
from flask import Response
from flask import request

from src import raft, rest_client
from src.log import LogEntry, Operation, Command
from src.raft import Role

SCHEDULER_INTERVAL = 0.01  # 10 milliseconds
app = Flask(__name__)
node = None
topic_queues = dict()
results = dict()


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


def apply_put_topic(command):
    topic = command.message
    id = command.id
    if topic in topic_queues.keys():
        results[id] = {'success': False}
    else:
        topic_queues[topic] = []
        results[id] = {'success': True}
    return


def apply_get_topic(command):
    results[command.id] = {'success': True, 'topics': list(topic_queues.keys())}
    return


def apply_put_message(command):
    data = json.loads(command.message)
    topic = data['topic']
    message = data['message']
    id = command.id
    if topic not in topic_queues.keys():
        results[id] = {'success': False}
    topic_queues[topic].append(message)
    results[id] = {'success': True}
    return


def apply_get_message(command):
    topic = command.message
    id = command.id
    if topic not in topic_queues.keys() or not topic_queues[topic]:
        results[id] = {'success': False}
    results[id] = {'success': True, 'message': topic_queues[topic].pop(0)}
    return


def apply_state_machine():
    """
    Apply commands to the state machine i.e. get/put messages from/into queue
    :return:
    """
    with node.thread_lock:
        if node.committed_index > node.last_applied:
            try:
                next_idx = node.last_applied + 1
                log_entry = node.logs.entries[next_idx]
                command = log_entry.command
                if command.operation is Operation.PUT_TOPIC:
                    apply_put_topic(command)
                elif command.operation is Operation.GET_TOPICS:
                    apply_get_topic(command)
                elif command.operation is Operation.PUT_MESSAGE:
                    apply_put_message(command)
                elif command.operation is Operation.GET_MESSAGE:
                    apply_get_message(command)
                else:
                    raise Exception(f'Unknown command: {command.operation}.')
            except Exception as e:
                results[command.id] = {'error_stack': str(e)}
                print(f'Error occurred while applying {command.operation}. Error: {e}.')
            node.last_applied += 1
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
            promise = executor.submit(rest_client.post, sibling_server, 'logs/append', data, timeout=0.1)
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
    node.reset_last_heartbeat()
    node.term = term
    if node.role != Role.FOLLOWER:
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
            'lastLogTerm': -1 if not node.logs.entries else node.logs.entries[-1].term,
            'lastLogIndex': node.logs.log_size - 1
        }
        for sibling_server in node.sibling_nodes:
            future = executor.submit(
                rest_client.post, sibling_server, 'election/vote', data, timeout=0.01)
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
                    for sibling_server in node.sibling_nodes:
                        node.next_index[sibling_server] = node.logs.log_size
                        node.match_index[sibling_server] = -1
                    return
            futures = retries

    # special case: 1 node system
    if votes > node.total_nodes / 2 and node.role == Role.CANDIDATE:
        node.transition_to_new_role(raft.Role.LEADER)
        for sibling_server in node.sibling_nodes:
            node.next_index[sibling_server] = node.logs.log_size
            node.match_index[sibling_server] = -1
    else:
        node.transition_to_new_role(raft.Role.FOLLOWER)
    return


def get_uuid():
    return str(uuid.uuid4())


@app.route('/topic', methods=['PUT'])
def put_topic():
    """
    Creates a new topic.
    :return boolean: True if topic was created, False if topic exists already or not created.
    """
    body = json.loads(request.get_data().decode('utf-8'))
    topic = body['topic']
    id = get_uuid()
    log_entry = LogEntry(node.term, Command(id, Operation.PUT_TOPIC, topic))
    node.logs.append(log_entry)
    log_index = node.logs.log_size - 1
    while node.committed_index < log_index:
        time.sleep(0.01)
    return results[id]


@app.route('/topic', methods=['GET'])
def get_topics():
    """
    Returns the list of topics
    :return list:
    """
    id = get_uuid()
    log_entry = LogEntry(node.term, Command(id, Operation.GET_TOPICS, ''))
    node.logs.append(log_entry)
    log_index = node.logs.log_size - 1
    while node.committed_index < log_index:
        time.sleep(0.01)
    return results[id]


@app.route('/message', methods=['PUT'])
def put_message():
    """
    Adds the message to end of the queue.
    :return:
    """
    body = request.get_data().decode('utf-8')
    id = get_uuid()
    log_entry = LogEntry(node.term, Command(id, Operation.PUT_MESSAGE, body))
    node.logs.append(log_entry)
    log_index = node.logs.log_size - 1
    while node.committed_index < log_index:
        time.sleep(0.01)
    return results[id]


@app.route('/message/<topic>', methods=['GET'])
def get_message(topic):
    """
    Returns first message from the topic
    :return:
    """
    id = get_uuid()
    log_entry = LogEntry(node.term, Command(id, Operation.GET_MESSAGE, topic))
    node.logs.append(log_entry)
    log_index = node.logs.log_size - 1
    while node.committed_index < log_index:
        time.sleep(0.01)
    return results[id]


@app.route('/status', methods=['GET'])
def get_status():
    """
    Returns current node role and term information
    :return:
    """
    return {'role': node.role.value, 'term': node.term}


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
    node.reset_last_heartbeat()
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
    if node.logs.entries and (node.logs.entries[-1].term > last_log_term or (
            node.logs.entries[-1].term == last_log_term and node.logs.log_size > last_log_idx + 1)):
        output = {'vote': False, 'term': node.term}
        return Response(json.dumps(output), status=200, mimetype='application/json')

    # found current leader
    if node.term == requester_term and node.is_leader():
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
        print(
            f'Starting server {args.index} on {nodes[args.index][0]}:{nodes[args.index][1]} with sibling nodes as '
            f'{sibling_nodes}.')
        background_thread = threading.Thread(target=run_background_tasks, daemon=True)
        background_thread.start()
        app.run(host=nodes[args.index][0], port=nodes[args.index][1])
