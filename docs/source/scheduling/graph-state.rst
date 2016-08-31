Graph State
===========

We represent computations as a graph of tasks, where a node is a task or
computation and an edge represents a data dependency between two nodes.  This
graph is acyclic, meaning that dependencies can only flow in one direction.

The scheduler maintains the nodes of this graph as a Python dictionary of
serialized functions and arguments.  It maintains the edges as a pair of
dictionaries, each mapping node labels to sets of node labels for the set of
nodes that depend on / are dependencies of a particular node.  This redundant
representation is important for constant-time access and is a common example
of the sort of state common within the scheduler.
