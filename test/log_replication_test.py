import json

import pytest
import requests_mock

import src.message_queue_server as mq_server
from src import raft
from src.log import LogEntry, Command
from src.raft import Role

node = None


@pytest.fixture
def app():
    app = mq_server.app
    mq_server.node = node
    return app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def set_up():
    global node
    node = raft.Node(0, ['localhost:91', 'localhost:92', 'localhost:93'])
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


def test_append_entries(client):
    node.next_index['localhost:91'] = 5
    node.next_index['localhost:92'] = 3
    node.next_index['localhost:93'] = 2
    with requests_mock.Mocker() as mocker:
        mocker.post('http://localhost:91/logs/append', json={'success': True, 'term': 3}, status_code=200)
        mocker.post('http://localhost:92/logs/append', json={'success': True, 'term': 3}, status_code=200)
        mocker.post('http://localhost:93/logs/append', json={'success': False, 'term': 3}, status_code=200)
        mq_server.append_entries()
        assert node.next_index['localhost:91'] == 5
        assert node.next_index['localhost:92'] == 4
        assert node.next_index['localhost:93'] == 1
        assert node.match_index['localhost:91'] == 4
        assert node.match_index['localhost:92'] == 2

    with requests_mock.Mocker() as mocker:
        mocker.post('http://localhost:91/logs/append', json={'success': False, 'term': 4}, status_code=200)
        mocker.post('http://localhost:92/logs/append', json={'success': True, 'term': 3}, status_code=200)
        mocker.post('http://localhost:93/logs/append', json={'success': False, 'term': 3}, status_code=200)
        mq_server.append_entries()
        assert node.role == Role.FOLLOWER
        assert node.term == 4
