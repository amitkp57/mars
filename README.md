# MARS

## RAFT REST MESSAGE QUEUE (RRMQ)
The aim of this project is to create a distributed message queue.
1. The system will be accessed through a REST API.
2. State will be kept consistent between the nodes with the Raft consensus
algorithm.

The Raft REST Message Queue (RRMQ) will allow users to add (PUT) and
consume (GET) messages from a specific topic.
1. PUT(topic, message) the message will be appended to the end of the
topic’s message queue
2. GET(topic) will pop the first message from the topic’s queue and return
the message
3. GET(topics) will return the list of topics
4. PUT(topic) will create a new topic

Message queues will follow a first-in-first-out (FIFO) mode.
Messages will be consumed (GET) only once by only one consumer. This implies
that the nodes have to work together to ensure that not only is the queue
kept consistent when messages are added (PUT), but also when messages are
consumed (GET).
RRMQ combines distribution,
consistency, replication, fault tolerance, consensus, and messaging. 

### Start a node
    python src\node.py config\server_config.json 0
    
### Client 
    python src\message_client.py 
    
### Test and code coverage
    coverage run --source src -m pytest
    coverage html

