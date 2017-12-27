from py2neo import Graph,Node,Relationship

test_graph = Graph(
    "http://localhost:7474",
    username="neo4j",
    password="china100!"
)
# print(test_graph)

test_node_1 = Node(label = "Person",name = "test_node_1")
test_node_2 = Node(label = "Person",name = "test_node_2")
test_graph.create(test_node_1)
test_graph.create(test_node_2)

# node_1_call_node_2 = Relationship(test_node_1,'CALL',test_node_2)
# node_1_call_node_2['count'] = 1
# node_2_call_node_1 = Relationship(test_node_2,'CALL',test_node_1)
# node_2_call_node_1['count'] = 2
# test_graph.create(node_1_call_node_2)
# test_graph.create(node_2_call_node_1)
