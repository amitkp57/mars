import json

import requests_mock

from src import rest_client


def match_input(request):
    assert json.loads(request.text) == {'b': 'c'}
    return True


def test_get():
    with requests_mock.Mocker() as mocker:
        mocker.get('http://test.com/path', json={'a': 'b'}, status_code=200)
        res = rest_client.get('test.com', 'path')
        assert res == {'a': 'b'}

        mocker.get('http://test.com/path', json={'a': 'b'}, status_code=201)
        res = rest_client.get('test.com', 'path')
        assert res == {'a': 'b'}

        mocker.get('http://test.com/path', json={'a': 'b'}, status_code=403)
        try:
            res = rest_client.get('test.com', 'path')
            assert False
        except Exception:
            assert True


def test_post():
    with requests_mock.Mocker() as mocker:
        mocker.post('http://test.com/path', json={'a': 'b'}, status_code=200, additional_matcher=match_input)
        res = rest_client.post('test.com', 'path', {'b': 'c'})
        assert res == {'a': 'b'}

        mocker.post('http://test.com/path', additional_matcher=match_input, json={'a': 'b'},
                    status_code=403)
        try:
            res = rest_client.post('test.com', 'path', {'b': 'c'})
            assert False
        except Exception:
            assert True


def test_put():
    with requests_mock.Mocker() as mocker:
        mocker.put('http://test.com/path', json={'a': 'b'}, status_code=200, additional_matcher=match_input)
        res = rest_client.put('test.com', 'path', {'b': 'c'})
        assert res == {'a': 'b'}

        mocker.put('http://test.com/path', additional_matcher=match_input, json={'a': 'b'},
                   status_code=403)
        try:
            res = rest_client.put('test.com', 'path', {'b': 'c'})
            assert False
        except Exception:
            assert True
