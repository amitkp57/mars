#### Tests
To run the tests, run the below command from the base project directory i.e. /project-mars-1
    
    pytest
To tun a particular test file

    pytest rest_client_test.py
To run a particular test

    pytest message_queue_test.py::test_create_topic

I have written the below tests for unit testing the code: 

1. rest_client.py<br/>
Tests rest client methods i.e. get, post and put methods.

2. leader_election_test.py<br/>
Tests 'election/vote' endpoint and 'initiate_leader_election()' method which triggers the election

3. log_replication_test.py<br/>
Tests '/logs/append' method and 'append_entries()' method called by leader node to send log entries to follower nodes.

4. message_queue_server_test.py<br/>
Tests the below rest endpoints. It starts a single node to test the functionalities.

    GET '/topic'<br/>
    PUT '/topic'<br/>
    GET '/message/<topic>'<br/>
    PUT '/message'<br/>
    GET '/status'<br/>

5. The below 3 swarm tests are taken from https://github.com/mpcs-52040/2021-project-tests <br/>
    election_test.py<br/>
    replication_test.py<br/>
    message_queue_test.py<br/>