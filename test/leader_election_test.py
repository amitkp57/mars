import json

import pytest

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
    node = raft.Node(0, ['localhost:01', 'localhost:02'])
    node.term = 3
    node.logs.append(LogEntry(1, None))
    node.logs.append(LogEntry(1, None))
    node.logs.append(LogEntry(2, None))
    node.logs.append(LogEntry(2, None))
    node.logs.append(LogEntry(3, None))


def test_log_inconsistency(client):
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

    message['lastLogTerm'] = 3
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['vote'] == False
    assert data['term'] == 3


def test_obsolete_term(client):
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

    message['term'] = 3
    node.voted_for = 1
    message['lastLogTerm'] = 3
    response = client.post('/election/vote', data=json.dumps(message), headers=headers)
    data = json.loads(response.data.decode('utf-8'))
    assert response.status_code == 200
    assert data['vote'] == False
    assert data['term'] == 3


def test_vote_leader_success(client):
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

# def test_post_heartbeat(client):
#     node.term = 0
#     headers = {'content-type': 'application/json'}
#     message = json.dumps({'term': 1})
#     response = client.post('/heartbeats/heartbeat', data=message, headers=headers)
#     data = json.loads(response.data.decode('utf-8'))
#     node.transition_to_new_role.assert_called_once_with(Role.FOLLOWER)
#     node.reset_last_heartbeat.assert_called_once()
#     assert response.status_code == 200
#     assert data['term'] == 1
#
#     node.reset_mock()
#     node.term = 5
#     message = json.dumps({'term': 1})
#     response = client.post('/heartbeats/heartbeat', data=message, headers=headers)
#     data = json.loads(response.data.decode('utf-8'))
#     node.transition_to_new_role.assert_not_called()
#     node.reset_last_heartbeat.assert_not_called()
#     assert response.status_code == 200
#     assert data['term'] == 5
#
#
# def test_initiate_leader_election():
#     node.reset_mock()
#     node.term = 1
#     node.total_nodes = 4
#     node.sibling_nodes = ['localhost:90', 'localhost:91', 'localhost:92']
#     with requests_mock.Mocker() as mocker:
#         mocker.post('http://localhost:90/election/vote', json={'vote': True, 'term': 1}, status_code=200)
#         mocker.post('http://localhost:91/election/vote', json={}, status_code=403)
#         mocker.post('http://localhost:92/election/vote', json={'vote': True, 'term': 1}, status_code=200)
#         mq_server.node = node
#         mq_server.initiate_leader_election()
#         node.transition_to_new_role.assert_has_calls([call(Role.CANDIDATE), call(Role.LEADER)])
#         node.incremenet_term.assert_called_once()
#
#     node.reset_mock()
#     node.term = 1
#     node.total_nodes = 4
#     node.sibling_nodes = ['localhost:90', 'localhost:91', 'localhost:92']
#     with requests_mock.Mocker() as mocker:
#         mocker.post('http://localhost:90/election/vote', json={'vote': True, 'term': 1}, status_code=200)
#         mocker.post('http://localhost:91/election/vote', json={'vote': False, 'term': 1}, status_code=200)
#         mocker.post('http://localhost:92/election/vote', json={'vote': True, 'term': 1}, status_code=200)
#         mq_server.node = node
#         mq_server.initiate_leader_election()
#         node.transition_to_new_role.assert_has_calls([call(Role.CANDIDATE), call(Role.FOLLOWER)])
#     node.incremenet_term.assert_called_once()
#
#     node.reset_mock()
#     node.term = 1
#     node.total_nodes = 4
#     node.sibling_nodes = ['localhost:90', 'localhost:91', 'localhost:92']
#     with requests_mock.Mocker() as mocker:
#         mocker.post('http://localhost:90/election/vote', json={'vote': True, 'term': 1}, status_code=500)
#         mocker.post('http://localhost:91/election/vote', json={'vote': False, 'term': 1}, status_code=403)
#         mocker.post('http://localhost:92/election/vote', json={'vote': True, 'term': 1}, status_code=200)
#         mq_server.node = node
#         mq_server.initiate_leader_election()
#         node.transition_to_new_role.assert_has_calls([call(Role.CANDIDATE), call(Role.FOLLOWER)])
#     node.incremenet_term.assert_called_once()
#
#
# def test_send_heartbeats():
#     node.reset_mock()
#     node.term = 1
#     node.sibling_nodes = ['localhost:90', 'localhost:91', 'localhost:92']
#     with requests_mock.Mocker() as mocker:
#         mocker.post('http://localhost:90/heartbeats/heartbeat', json={'term': 1}, status_code=200)
#         mocker.post('http://localhost:91/heartbeats/heartbeat', json={}, status_code=403)
#         mocker.post('http://localhost:92/heartbeats/heartbeat', json={'term': 1}, status_code=200)
#         mq_server.node = node
#         mq_server.append_entries()
#         node.transition_to_new_role.assert_not_called()
#
#     node.reset_mock()
#     node.term = 1
#     node.sibling_nodes = ['localhost:90', 'localhost:91', 'localhost:92']
#     with requests_mock.Mocker() as mocker:
#         mocker.post('http://localhost:90/heartbeats/heartbeat', json={'term': 4}, status_code=200)
#         mocker.post('http://localhost:91/heartbeats/heartbeat', json={}, status_code=403)
#         mocker.post('http://localhost:92/heartbeats/heartbeat', json={'term': 1}, status_code=200)
#         mq_server.node = node
#         mq_server.append_entries()
#         node.transition_to_new_role.assert_called_once_with(Role.FOLLOWER)
