# Haddop-Edge-Vertex-Graph
Set of distributed Hadoop MapReduce programs to collect information on a large edge vertex graph

# List of programs with brief description 
**InOutDegree** - measures the number of incoming edges and outgoing edges of each vertex of a directed graph. Then writes two sorted list of the top 100 vertices one for IN and one for OUT.

**Friends**- finds all instances of bidirectional edges in a directed graph then write a list of key and values where the key is a node and the corresponding value is a list off all nodes that the key node shares a bidirectional edge.

**EdgeVertexCount**- counts all total number of Edges and Vertices in an edge vertex graph

**GeoPathLength**- traverses the graph and finds the total number of paths with pathlengths from one to ten; Can be easily changed to increase the desired pathlength. Then uses this to determine an estimate of the geodesic path length of the graph.

**ClusterCoefficient**- finds all unique paths of length 3 and Triangles(path of length 3 where the starting node of the path is also the last node in the path). These values are then used to calculate the Cluster Coefficient of the graph.
