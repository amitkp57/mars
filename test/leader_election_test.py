import json

import pytest
import requests_mock

import src.message_queue_server as mq_server
from src import raft
from src.log import LogEntry
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


@pytest.fixture(scope='session', autouse=True)
def set_up():
    global node
    node = raft.Node(0, ['localhost:90', 'localhost:91', 'localhost:92'])
    node.term = 3
    node.logs.append(LogEntry(1, None))
    node.logs.append(LogEntry(1, None))
    node.logs.append(LogEntry(2, None))
    node.logs.append(LogEntry(2, None))
    node.logs.append(LogEntry(3, None))


def test_log_inconsistency(client):
    # last log entry term is less than follower
    headers = {'content-type': 'application/json'}
    message = {
        'term': 4,
        'candidateId': 3,
        'lastLogIndex': 2,
        'lastLogTerm': 2
    }
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['vote'] == False
    assert data['term'] == 3

    # candidate's log last index is less than follower
    message['lastLogTerm'] = 3
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['vote'] == False
    assert data['term'] == 3


def test_obsolete_term(client):
    # candidate's term is less than follower
    headers = {'content-type': 'application/json'}
    message = {
        'term': 2,
        'candidateId': 3,
        'lastLogIndex': 4,
        'lastLogTerm': 3
    }
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['vote'] == False
    assert data['term'] == 3

    # follower voted for some other in current term
    message['term'] = 3
    node.voted_for = 1
    message['lastLogTerm'] = 3
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['vote'] == False
    assert data['term'] == 3


def test_vote_leader_success(client):
    # candidate's term is greater than follower
    headers = {'content-type': 'application/json'}
    message = {
        'term': 4,
        'candidateId': 3,
        'lastLogIndex': 4,
        'lastLogTerm': 3
    }
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert node.role == Role.FOLLOWER
    assert node.term == 4
    assert data['vote'] == True
    assert data['term'] == 4

    # haven't voted anyone in this term
    node.voted_for = None
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert node.role == Role.FOLLOWER
    assert node.term == 4
    assert data['vote'] == True
    assert data['term'] == 4

    # voted for same leader in current term
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert node.role == Role.FOLLOWER
    assert node.term == 4
    assert data['vote'] == True
    assert data['term'] == 4


def test_initiate_leader_election():
    # received majority votes
    node.term = 1
    with requests_mock.Mocker() as mocker:
        mocker.post('http://localhost:90/election/vote', json={'vote': True, 'term': 1}, status_code=200)
        mocker.post('http://localhost:91/election/vote', json={}, status_code=403)
        mocker.post('http://localhost:92/election/vote', json={'vote': True, 'term': 1}, status_code=200)
        mq_server.node = node
        mq_server.initiate_leader_election()
        assert node.role == Role.LEADER
        assert node.term == 2

    # update node term and go to follower state
    with requests_mock.Mocker() as mocker:
        mocker.post('http://localhost:90/election/vote', json={'vote': True, 'term': 1}, status_code=200)
        mocker.post('http://localhost:91/election/vote', json={'vote': False, 'term': 4}, status_code=200)
        mocker.post('http://localhost:92/election/vote', json={'vote': False, 'term': 1}, status_code=200)
        mq_server.node = node
        mq_server.initiate_leader_election()
        assert node.role == Role.FOLLOWER
        assert node.term == 4
