# Product Overview

Tair is fast-access memory (MDB)/persistent (LDB) storage service. Using a high-performance and high-availability distributed cluster architecture, Tair can meet businesses' high requirements for read/write performance and scalable capacity.

# Architecture

## System architecture

A Tair cluster has three necessary modules: the ConfigServer, DataServer and client.

Generally, a Tair cluster includes two ConfigServers and multiple DataServers. The two ConfigServers act as primary and standby servers. Heartbeat checks between the DataServer and ConfigServer are used to check live and available DataServers in the cluster to establish the distribution of data in the cluster (comparison table). DataServers store, copy and migrate data as directed by the ConfigServer. When a client is launched, it obtains data distribution information from the ConfigServer. Based on this data distribution information, the client interacts with the corresponding DataServers to perform the user's requests.

In terms of architecture, the role of ConfigServer is similar to the central node in traditional application systems. The whole cluster service depends on the ConfigServer. In fact, Tair's ConfigServers are extremely lightweight. When a working ConfigServer is experiencing downtime, another ConfigServer automatically takes over within seconds. Even if two ConfigServers are experiencing downtime simultaneously, Tair can run normally as long as there is no change in the DataServers. Users only need to connect apps to ConfigServers and do not need to know the details of the internal nodes.

### ConfigServers

* Two ConfigServers act as primary and standby.
* Live and available DataServer node information for the cluster is determined using a heartbeat check between the ConfigServer and DataServer.
* Based on the DataServer node information, the ConfigServer constructs a data distribution table which shows how data is distributed in the cluster.
* The ConfigServer provides a data distribution table query service.
* The ConfigServer schedules data migration and copying between DataServers.

### DataServers

* DataServers provide storage engines.
* DataServers receive operations initiated by clients, such as put/get/remove.
* DataServers migrate and copy data.
* DataServers provide access statistics.

### Clients

* Clients provide APIs for accessing the Tair cluster.
* Clients update and cache data distribution tables.
* Clients provide LocalCache to prevent overheated data access from affecting the Tair cluster service.
* Clients control traffic.

## Product features

### Distributed architecture

* A distributed cluster architecture is used to provide automatic disaster recovery and failover.
* Load balancing is supported and data is distributed evenly.
* System storage space and throughput performance can be scaled elastically, resolving data volume and QPS performance limitations.
* Fully-featured and user-friendly access
* The data structure is rich. Single level key-value structures and secondary indexing structures are supported.
* Various uses are supported. Counter mode is also supported.
* Data expiration and version control are supported.

## Uses

### Database caching

As business volume increases, the number of concurrent requests to the database system is increasing and the load on the database system is becoming heavier. When the database system is overloaded, response speeds are slower and, in extreme situations, the service may even be interrupted. To address this problem, Tair MDB can be deployed together with database products to provide high throughput and low latency storage.
MDB can respond quickly, and generally completes requests within milliseconds. Moreover, MDB supports a higher QPS rate and can process more concurrent requests than databases. By observing the business, the user can store hotspot data in MDB and significantly lessen the load on the database. This not only reduces database costs but also improves system availability.

### Temporary data storage

Applications such as social websites, e-commerce websites, games, and advertisements need to maintain a large volume of temporary data. Storing temporary data in MDB can reduce the memory management overheads and application load.
In a distributed environment, MDB can be used as uniform global storage which can prevent data loss caused by a single point of failure and resolve issues relating to synchronization between multiple applications.
A common example is to use MDB as a session manager. If the website uses distributed deployment and the traffic is huge, different requests from the same user may be sent to different web servers. In this case, MDB can be used as a global storage solution to save session data, user tokens, permission information and other data.

### Data Storage

* The recommendation and advertising businesses generally need to compute huge amounts of data offline. LDB supports persistent storage and provides superb performance. Online services are supported, allowing the user to regularly import offline data to LDB for online services.
* After computing, list businesses can store the final lists in LDB to be directly displayed to front-end apps. In this way, LDB meets storage and high-speed access needs.

### Blacklist/whitelist

Security apps have many blacklist/whitelist scenarios. These blacklist/whitelist scenarios are characterized by low hit rates, large access volumes, and business loss caused by data loss. LDB supports data persistence and high access volume, so it is widely used in these scenarios.

### Distributed locks

Distributed locks are usually used to prevent data inconsistency and logical chaos caused by multi thread concurrence. Distributed locks can be implemented using Tair's version feature or computing function. Thanks to LBS's persistence, the locks aren't lost and can be released normally even if the service goes down.


# Contribution

The source code is available user the GPL version 2. We are avtively looking for contributors so if you have any ideas, bug reports, or patchs you would like to contribute please do not hesitate to do so.
