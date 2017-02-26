#Project Title:
Replicated Cloud storage system with notification mechanism for gas pumping station

#Getting started:
There are jar files for ECS, client, sever and control room. ECS is the server administrator. It supports initialization of server, scalling up and down of 
server nodes and fault tolerance. Client is the terminal where user can issue put and get command to store or retrieve information from server. Server is 
responsible for persistent storage system and notification mechanism. Control rooms are notified when value of a key under a particular level reaches a threshold.

#Prerequisites:
1. Server pcs should support ssh connection to be launched by ecs.

#Installing: 
No installation is required. Just copy the ms5-server.jar file to several machines to be worked as server. Place ms5-client.jar to client machines. Then copy 
ms5-ecs.jar file to another machine that will perform as server administrator. Finally copy ms5-controlroom.jar file to control rooms. 

Insert server machines's ip address and port number which is going to be used during connection in the ecs.config file. In each control room, just enter port number
which will listen notification from server in the controlroom.config file.

#Running:
All the applications / jar files support wide a range of commands except server 

##Supported Commands at ECS:
1. init <numberOfNodes> <cacheSize> <displacementStrategy> -- This call launches the server with the specified cache size and displacement strategy.
2. start -- Starts the storage service by calling start() on all KVServer instances that participate in the service.
3. stop -- Stops the service; all participating KVServers are stopped for processing client requests but the processes remain running.
4. add <cacheSize> <displacementStrategy> -- Create a new KVServer with the specified cache size and displacement strategy and add it to the storage service at an arbitrary position.
5. remove -- Remove a node from the storage service at an arbitrary position.
6. shutdown -- Stops all server instances and exits the remote processes.

##Supported Commands at Client:
1. connect <host> <port> -- Establishes a connection to a server.
2. put <key> <value> -- Puts a key value pair to storage.
3. put <key> <null> -- Deletes specified key.
4. get <key> -- Returns already stored value.
5. disconnect -- Disconnects from the server.
6. logLevel <level> -- Changes the logLevel.
7. quit -- Exits the program.

##Supported Commands at Control Room:
1. connect <host> <port> -- Establishes a connection to a server.
2. subscribe <level> -- Subscribe control room to particular level.
3. unsubscribe -- Unsubscribe client from subscribed level.
4. disconnect -- Disconnects from the server.
5. logLevel <level> -- Changes the logLevel.
6. quit -- Exits the program.

#Running Test cases:
1. Run AllTest.java as Junit test. This will launch the test suite which has various test files.

#Performance Evaluation:
1. Enron email dataset is used at varing condition to evaluate and test the performance of the entire project. Performance results are avaiable in the project directory.

#Deployment:
To deploy jar file from code sources use build.xml. Run it using ant build.

#Vesioning:
Current system poses first version functionalities.

#Authors:
1. Md Shifuddin Al Masud <shifuddin.masud@gmail.com>
2. Vidrashku Ilya

#License:
This project is licensed under MIT licensed

#Acknowledgments:
This project is developed under the guidence of Martin Jergler, M.Sc. for lab course: cloud databases