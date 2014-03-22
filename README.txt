
Mesos GridEngine framework readme
--------------------------------------------
This framework is a wrapper around the GridEngine cluster resource manager for the cluster.

This framework assumes that Grid Engine is installed and running. It also assumes that it will be run
on an adminstrative node. It works by scanning the queue, getting resource allocations from Mesos,
then using the 'qconf' commands to enable/disable the nodes.


Installing GridEngine:
------------------

option #1) install from source

option #2) sudo apt-get install torque-server, torque-mom (also potentially relevant: torque-dev, torque-client)


==Structure Overview of the Framework==
---------------------------------------

==FRAMEWORK SCHEDULER==

==FRAMEWORK EXECUTOR==


Permissions:
------------