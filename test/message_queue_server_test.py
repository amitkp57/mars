import json
from unittest.mock import Mock, call

import pytest
import requests_mock

import src.message_queue_server as mq_server
from src.raft import Role

node = Mock()


@pytest.fixture
def app():
    app = mq_server.app
    mq_server.node = node
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def test_empty_queue(client):
    response = client.get('/messageQueue/message')
    assert response.status_code == 403


def test_put_and_get_queue(client):
    headers = {'content-type': 'application/json'}
    message = json.dumps({'name': 'Amit', 'email': 'pradhanak@uchicago.edu'})
    response = client.put('/messageQueue/message', data=message, headers=headers)
    assert response.status_code == 201

    response = client.get('/messageQueue/message')
    assert response.status_code == 200
    assert response.data.decode('ascii') == message

    response = client.get('/messageQueue/message')
    assert response.status_code == 403


def test_vote_leader(client):
    node.term = 0
    headers = {'content-type': 'application/json'}
    message = json.dumps({'term': 1})
    response = client.post('/election/vote', data=message, headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['vote']
    assert data['term'] == 1

    node.term = 5
    message = json.dumps({'term': 2})
    response = client.post('/election/vote', data=message, headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert not data['vote']
    assert data['term'] == 5


def test_post_heartbeat(client):
    node.term = 0
    headers = {'content-type': 'application/json'}
    message = json.dumps({'term': 1})
    response = client.post('/heartbeats/heartbeat', data=message, headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    node.transition_to_new_role.assert_called_once_with(Role.FOLLOWER)
    node.reset_last_heartbeat.assert_called_once()
    assert response.status_code == 200
    assert data['term'] == 1

    node.reset_mock()
    node.term = 5
    message = json.dumps({'term': 1})
    response = client.post('/heartbeats/heartbeat', data=message, headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    node.transition_to_new_role.assert_not_called()
    node.reset_last_heartbeat.assert_not_called()
    assert response.status_code == 200
    assert data['term'] == 5


def test_initiate_leader_election():
    node.reset_mock()
    node.term = 1
    node.total_nodes = 4
    node.sibling_nodes = ['localhost:90', 'localhost:91', 'localhost:92']
    with requests_mock.Mocker() as mocker:
        mocker.post('http://localhost:90/election/vote', json={'vote': True, 'term': 1}, status_code=200)
        mocker.post('http://localhost:91/election/vote', json={}, status_code=403)
        mocker.post('http://localhost:92/election/vote', json={'vote': True, 'term': 1}, status_code=200)
        mq_server.node = node
        mq_server.initiate_leader_election()
        node.transition_to_new_role.assert_has_calls([call(Role.CANDIDATE), call(Role.LEADER)])
        node.incremenet_term.assert_called_once()

    node.reset_mock()
    node.term = 1
    node.total_nodes = 4
    node.sibling_nodes = ['localhost:90', 'localhost:91', 'localhost:92']
    with requests_mock.Mocker() as mocker:
        mocker.post('http://localhost:90/election/vote', json={'vote': True, 'term': 1}, status_code=200)
        mocker.post('http://localhost:91/election/vote', json={'vote': False, 'term': 1}, status_code=200)
        mocker.post('http://localhost:92/election/vote', json={'vote': True, 'term': 1}, status_code=200)
        mq_server.node = node
        mq_server.initiate_leader_election()
        node.transition_to_new_role.assert_has_calls([call(Role.CANDIDATE), call(Role.FOLLOWER)])
    node.incremenet_term.assert_called_once()

    node.reset_mock()
    node.term = 1
    node.total_nodes = 4
    node.sibling_nodes = ['localhost:90', 'localhost:91', 'localhost:92']
    with requests_mock.Mocker() as mocker:
        mocker.post('http://localhost:90/election/vote', json={'vote': True, 'term': 1}, status_code=500)
        mocker.post('http://localhost:91/election/vote', json={'vote': False, 'term': 1}, status_code=403)
        mocker.post('http://localhost:92/election/vote', json={'vote': True, 'term': 1}, status_code=200)
        mq_server.node = node
        mq_server.initiate_leader_election()
        node.transition_to_new_role.assert_has_calls([call(Role.CANDIDATE), call(Role.FOLLOWER)])
    node.incremenet_term.assert_called_once()
