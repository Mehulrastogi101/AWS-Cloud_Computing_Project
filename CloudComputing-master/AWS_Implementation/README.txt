AWS ec2 based filing system.

TO RUN...
Host the Namenode on a cloud EC2 instance (can be run locally as well)
In Datanode on line 15, define the Namenode IP address. Then host on 
	ec2 instances. (Recommended at least 3 DN instances to match replication factor)

In the client, change the Namenode IP address. Make sure an S3 object that you plan on 
	housing your data exists. 

Run the client (this can be done locally)






DN.py:
- (POST)Datanode takes data from the client and saves it to a block in
	the format {blockid : data}. After data is stored to a block, 
	Datanode immediately sends a "block-beat" to the NameNode. 

- (GET) The client retrieves data from Datanode by passing the blockid 
	to the specific Datanodes housing the desired data. Block is 
	then opened, read, and returned to the client. 

- (POST)In the event that a Datanode fails, the Namenode will move its
(NN /FT)that particular Datanode's blocks to other Datanodes in order 
	to maintain the replication factor (defined in NN as 3).

- each Datanode will send a block beat to a the Namenode every 7 seconds
	as a way for the Namenode to check that the Datanode is still alive.



NN.py:
- (POST)Namenode takes json data from the client and checks to see if it
	already exists in the system architecture. If yes, exception is 
	handled and error message is returned. Else, Namenode assigns 
	blocks of the file to Datanodes in a round-robin schedule. Namenode
	returns this list to the client and allows client to distribute data
	to each Datanode while the Namenode saves its own copy of which 
	Datanode stores which block. 

- (GET) Namenode gets a filename from the client and checks its Datanode list 
	to see which Datanodes house the data. List of these Datanodes is 
	returned to the Client. If filename doesn't exist, exception handled
	and error message is returned.

- (POST)Namenode receives a block beat from every Datanode containing the 
(DN /BB)Datanode's block ids. This is the updated in the Namenode's Datanode
	list. Scheduled to execute every 7 seconds in each Datanode.

- Namenode specifications: Block Size defined to be 64 megabytes, 
	replication factor = 3, wait time for block beats is 45 seconds.
	In the event that a Datanode fails to write a block beat to the 
	Namenode inside its 45 second timeframe, Namenode removes the 
	Datanode from its Datanode list and has the blocks it stored 
	replicated to other Datanodes. 



Client.py:
- (Write File) 1: Allows user to write a file to the filing system.
- (Read File) 2: Allows user to retrieve file contents from the filing system.
- (Datanode List) 3: Allows user to retrieve the list of Datanodes that house
	a particular file.



