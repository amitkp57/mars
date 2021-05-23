import threading

import pytest

import src.message_queue_server as mq_server
from src import raft, rest_client

node = None
topic_queues = {}


@pytest.fixture(autouse=True)
def set_up():
    global node, topic_queues
    node = raft.Node(0, [])
    topic_queues.clear()
    app = mq_server.app
    mq_server.node = node
    mq_server.topic_queues = topic_queues
    background_thread = threading.Thread(target=mq_server.run_background_tasks, daemon=True)
    background_thread.start()
    flask_thread = threading.Thread(target=app.run, args=('localhost', 9543), daemon=True)
    flask_thread.start()


def test_put_topic():
    # new topic
    response = rest_client.put('localhost:9543', 'topic', {'topic': 'topic1'})
    assert response['success'] == True

    # existing topic
    response = rest_client.put('localhost:9543', 'topic', {'topic': 'topic1'})
    assert response['success'] == False


def test_get_topic():
    # no existing topic
    response = rest_client.get('localhost:9543', 'topic')
    assert response['success'] == True
    assert response['topics'] == []

    # new topic
    response = rest_client.put('localhost:9543', 'topic', {'topic': 'topic1'})
    assert response['success'] == True

    # get topics
    response = rest_client.get('localhost:9543', 'topic')
    assert response['success'] == True
    assert response['topics'] == ['topic1']


def test_put_message():
    # unknown topic
    response = rest_client.put('localhost:9543', 'message', {'topic': 'topic1', 'message': 'msg1'})
    assert response['success'] == False

    # known topic
    response = rest_client.put('localhost:9543', 'topic', {'topic': 'topic2'})
    assert response['success'] == True
    response = rest_client.put('localhost:9543', 'message', {'topic': 'topic2', 'message': 'msg1'})
    assert response['success'] == True


def test_get_message():
    # unknown topic
    response = rest_client.get('localhost:9543', 'message/topic3')
    assert response['success'] == False

    # no message in topic
    response = rest_client.put('localhost:9543', 'topic', {'topic': 'topic2'})
    assert response['success'] == True
    response = rest_client.get('localhost:9543', 'message/topic2')
    assert response['success'] == False

    # message in topic
    response = rest_client.put('localhost:9543', 'topic', {'topic': 'topic1'})
    assert response['success'] == True
    response = rest_client.put('localhost:9543', 'message', {'topic': 'topic1', 'message': 'msg1'})
    assert response['success'] == True
    response = rest_client.get('localhost:9543', 'message/topic1')
    assert response['success'] == True
    assert response['message'] == 'msg1'


def test_status():
    response = rest_client.get('localhost:9543', 'status')
    assert response['term'] == 0
    assert response['role'] == 'Leader'
