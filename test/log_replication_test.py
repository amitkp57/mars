import json

import pytest

import src.message_queue_server as mq_server
from src import raft
from src.log import LogEntry, Command

node = None


@pytest.fixture
def app():
    app = mq_server.app
    mq_server.node = node
    return app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(scope='session', autouse=True)
def set_up():
    global node
    node = raft.Node(0, ['localhost:01', 'localhost:02'])
    node.term = 3
    node.logs.append(LogEntry(1, None))
    node.logs.append(LogEntry(1, None))
    node.logs.append(LogEntry(2, None))
    node.logs.append(LogEntry(2, None))
    node.logs.append(LogEntry(3, None))


def test_sync_logs_failures(client):
    headers = {'content-type': 'application/json'}
    # term < follower term
    message = {
        'term': 2,
        'leaderId': 3,
        'prevLogTerm': 2,
        'prevLogIndex': 2,
        'entry': None,
        'leaderCommit': 5
    }
    response = client.post('/logs/append', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['success'] == False
    assert data['term'] == 3

    # prevLogIndex > follower last index
    message['term'] = 3
    message['prevLogIndex'] = 5
    response = client.post('/logs/append', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['success'] == False
    assert data['term'] == 3

    # value at prevLogIndex mismatch
    message['term'] = 3
    message['prevLogIndex'] = 1
    response = client.post('/logs/append', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['success'] == False
    assert data['term'] == 3


def test_sync_logs_success(client):
    headers = {'content-type': 'application/json'}
    # heartbeat message
    message = {
        'term': 3,
        'leaderId': 3,
        'prevLogTerm': 3,
        'prevLogIndex': 4,
        'entry': None,
        'leaderCommit': 5
    }
    response = client.post('/logs/append', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['success'] == True
    assert data['term'] == 3
    assert node.logs.log_size == 5
    assert node.logs.entries[4].term == 3
    assert node.committed_index == 4

    # trim and append log entries
    message = {
        'term': 3,
        'leaderId': 3,
        'prevLogTerm': 2,
        'prevLogIndex': 2,
        'entry': LogEntry(3, Command(1, '', '')).json_encode(),
        'leaderCommit': 5
    }
    response = client.post('/logs/append', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['success'] == True
    assert data['term'] == 3
    assert node.logs.log_size == 4
    assert node.logs.entries[3].term == 3
    assert node.committed_index == 3
