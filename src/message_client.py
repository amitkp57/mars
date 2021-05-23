import json

from src import rest_client


def find_leader(hosts):
    for host in hosts:
        try:
            response = rest_client.get(host, 'status', timeout=1)
            if response['role'] == 'Leader':
                return host
        except Exception as e:
            print(f'Node {host} failed with {e}.')
    raise Exception('No leader found!')


def create_topic(host, topic):
    message = {'topic': topic}
    response = rest_client.put(host, 'topic', message, timeout=1)
    print(response)
    return


def get_topics(host):
    response = rest_client.get(host, 'topic', timeout=1)
    print(response)
    return


def put_message(host, topic, message):
    message = {'topic': topic, 'message': message}
    response = rest_client.put(host, 'message', message, timeout=1)
    print(response)
    return


def get_message(host, topic):
    response = rest_client.get(host, 'message/' + topic, timeout=1)
    print(response)
    return


if __name__ == '__main__':
    hosts = []
    with open('../config/server_config.json', 'r') as config_file:
        server_config = json.load(config_file)['addresses']
        hosts = [server['ip'].removeprefix('http://') + ':' + str(server['port']) for server in server_config]

    print('hosts: ', hosts)
    leader = find_leader(hosts)
    print('leader: ', leader)

    create_topic(leader, 'topic1')
    create_topic(leader, 'topic2')
    create_topic(leader, 'topic3')
    create_topic(leader, 'topic2')
    create_topic(leader, 'topic1')

    get_topics(leader)

    put_message(leader, 'topic9', 'msg1')
    put_message(leader, 'topic1', 'msg2')
    put_message(leader, 'topic2', 'msg3')
    put_message(leader, 'topic3', 'msg4')
    put_message(leader, 'topic1', 'msg5')

    get_message(leader, 'topic1')
    get_message(leader, 'topic2')
    get_message(leader, 'topic3')
    get_message(leader, 'topic4')
