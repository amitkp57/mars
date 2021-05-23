import json

import pytest

import src.message_queue_server as mq_server
from src import raft
from src.raft import Role

node = None
topic_queues = {}


@pytest.fixture()
def app():
    app = mq_server.app
    mq_server.node = node
    mq_server.topic_queues = topic_queues
    return app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def set_up():
    global node, topic_queues
    node = raft.Node(0, ['localhost:91', 'localhost:92', 'localhost:93'])
    topic_queues.clear()


def test_put_topic(client):
    # new topic
    headers = {'content-type': 'application/json'}
    response = client.put('/topic', data=json.dumps({'topic': 'topic1'}), headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == True

    # existing topic
    response = client.put('/topic', data=json.dumps({'topic': 'topic1'}), headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == False


def test_get_topic(client):
    headers = {'content-type': 'application/json'}
    # no existing topic
    response = client.get('/topic', headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == True
    assert data['topics'] == []

    # new topic
    response = client.put('/topic', data=json.dumps({'topic': 'topic1'}), headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == True

    # get topics
    response = client.get('/topic', headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == True
    assert data['topics'] == ['topic1']


def test_put_message(client):
    mq_server.topic_queues['topic2'] = []
    headers = {'content-type': 'application/json'}
    # unknwon topic
    response = client.put('/message', data=json.dumps({'topic': 'topic1', 'message': 'msg1'}), headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == False

    # knwon topic
    response = client.put('/message', data=json.dumps({'topic': 'topic2', 'message': 'msg1'}), headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == True


def test_get_message(client):
    mq_server.topic_queues['topic1'] = ['msg1']
    mq_server.topic_queues['topic2'] = []
    headers = {'content-type': 'application/json'}
    # unknown topic
    response = client.get('/message/topic3', headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == False

    # no message in topic
    response = client.get('/message/topic2', headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == False

    # message in topic
    response = client.get('/message/topic1', headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == True
    assert data['message'] == 'msg1'

    # known topic
    response = client.put('/message', data=json.dumps({'topic': 'topic2', 'message': 'msg1'}), headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['success'] == True


def test_status(client):
    node.term = 1
    node.transition_to_new_role(Role.CANDIDATE)
    headers = {'content-type': 'application/json'}
    response = client.get('/status', headers=headers)
    data = json.loads(response.data.decode('ascii'))
    assert response.status_code == 200
    assert data['term'] == 1
    assert data['role'] == 'Candidate'
