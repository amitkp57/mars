import json

import pytest

import src.message_queue_server as mq_server

service_url = '/messageQueue/message'


@pytest.fixture
def app():
    app = mq_server.app
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def test_empty_queue(client):
    response = client.get(service_url)
    assert response.status_code == 403


def test_put_and_get_queue(client):
    headers = {'content-type': 'application/json'}
    message = json.dumps({'name': 'Amit', 'email': 'pradhanak@uchicago.edu'})
    response = client.put(service_url, data=message, headers=headers)
    assert response.status_code == 201

    response = client.get(service_url)
    assert response.status_code == 200
    assert response.data.decode('ascii') == message

    response = client.get(service_url)
    assert response.status_code == 403
