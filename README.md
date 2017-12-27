**Distributed, Fault Tolerant Key-Value Store written in Flask by**
**Vikram Melkote and Mike Hamilton**

* to make a docker subnet:
	sudo docker network create --subnet 10.0.0.0/16 mynet

* build the docker container:
	sudo docker build -t mycontainer .

* to start a full node example: 
	sudo docker run -p 8084:8080 --ip=10.0.0.24 --net=mynet -e K=3 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080" -e IPPORT="10.0.0.24:8080" mycontainer

* to start a half node example:
	sudo docker run -p 8084:8080 --ip=10.0.0.24 --net=mynet -e IPPORT="10.0.0.24:8080" mycontainer

* to send a request to a node:
	curl http://10.0.0.22:8080/kv-store/get_node_details -X GET
	or
	curl http://10.0.0.21:8080/kv-store/update_view?type=add -d "ip_port=10.0.0.22:8080" -X PUT

**TO-DO:**

* Abstract helper functions into separate files for readability

* fix print functions to be one function with different tags like Tom had it

* fix the whole getReplicaDetals / global isReplica thing

* Debug current code
	* dataCluster is empty in getPartition()
	* nodes that get added in test 6 get added to view too many times.
	        maybe a problem with update_view add? or _update! or updateRatio?
	* _getAllKeys! causes broken pipe (I think in test 8)

* implement proper chord where a node only stores r buckets before and after itself
	instead of all buckets

* make read repair work so it doesn't try to send a whole dict if it's bugger than max request size.
	Look into setting up a fixed sized buffer to send huge dicts in chunks

* make sorting of arrays more efficient

* general refactoring