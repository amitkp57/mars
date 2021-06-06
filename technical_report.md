## Implementation Details

### Message Queue
I have used python in-memory dictionary data structure to store messages for each topic. The dictionary contains 
topic-messages pairs. Flask library is used to implement REST endpoints. The below REST endpoints are implemented:

#### Topic
The topic endpoint is used to create a topic and get a list of topics.

##### PUT /topic

Used to create a new topic.

Flask endpoint

    @app.route('/topic', methods=['PUT'])
Body:

    {'topic' : str}
Returns:

    {'success' : bool}

Returns True if the topic was created, False if the topic already exists or was
not created for another reason.

##### GET /topic

Used to get a list of topics

Flask endpoint

    @app.route('/topic', methods=['GET'])

Returns:
    
    {'success' : bool, 'topics' : [str]}

If there are no topics it returns an empty list.

#### Message
The message endpoint is to add a message to a topic and get a message from a
topic.
##### PUT /message

Flask endpoint

    @app.route('/message', methods=['PUT'])
Body:

    {'topic' : str, 'message' : str}
Returns:
    
    {'success' : bool}
returns failure if topic does not exists
##### GET /message
Used to pop a message from the topic. Notice that the topic name is included in
the URL.

Flask endpoint
    
    @app.route('/message/<topic>', methods=['GET'])
Returns:
    
    {'success' : bool, 'message' : str}
It returns False if:
1. the topic does not exist
2. there are no messages in the topic that haven’t been already consumed


#### Status
##### GET /status
The status endpoint is used for testing the leader election algorithm.

Flask endpoint
    
    @app.route('/status', methods=['GET'])
Returns:
    
    {'role' : str, 'term' : int}
    
Role is a string with one of three options:
1. Leader
2. Candidate
3. Follower

Term return the term that this node is currently in as INT (to the best of its
knowledge)

### Election

Each node starts as a follower. Every node has a timer with timeout value picked randomly from uniform distribution 
(500, 1000) milliseconds. When node timers time out and it does not know who is the leader, it starts election process.

Flask endpoint

    @app.route('/election/vote', methods=['POST'])

The follower after starting election process moves to candidate role and increases current election term value. It calls
the above REST endpoint for each node in the list. It votes for itself and wait for votes from others. If it receives
votes from majoirty of nodes it moves to leader state. While during election process, if it finds out the current leader
or any node with term greter than its term value, it moves back to follower state. A leader must have most updated logs. 

### Log Replication

Flask endpoint

    @app.route('/logs/append', methods=['POST'])

After a node becomes leader, it syncs its own logs with other nodes by calling the above endpoint. Only leader can
communicate with the clients. When the leader receives a request, it appends an entry to its own logs. Then it tries
to send append entry messages to other nodes to sync logs. Once more than half of the nodes append the entry to
their logs, leader commits the entry in its logs. After leader commits, other nodes start committing the entry. 
Committed entries are applied to state machine asynchronously. After a entry is applied to leader's state machine,
leader replies back to the client. 

This implementation is almost similar to what has been proposed on raft paper.<br/> 

References:<br/>
1. https://raft.github.io/raft.pdf <br/>
2. http://cs.brown.edu/courses/csci1380/s16/lectures/11raft.pdf <br/>
