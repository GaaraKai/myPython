from py2neo import Graph,Node,Relationship,Path
import pandas as pd
import sys
import csv

test_graph = Graph(
    "http://localhost:7474",
    username="neo4j",
    password="china100!"
)
# print(test_graph)


# mobile_node_1 = Node("Mobile", name="18610558465")

csv_file_path = "D:/github_program/myPython/docs/rst/test.csv"
red = pd.read_csv(csv_file_path)
print(red)


sys.exit(0)




reader = csv.reader(open(csv_file_path,"r"))
for line in reader:
    # print(len(line))
    for single in range(0,len(line)):
        print(line[single])

        find_code_1 = test_graph.find_one(
            label="Mobile",
            property_key="name",
            property_value=line[single]
        )
        if find_code_1 is None:
            mobile_node_1 = Node("Mobile", name="18610558465")
            test_graph.create(mobile_node_1)
        else:
            print("Exist")




sys.exit(0)










find_code_1 = test_graph.find_one(
  label="Mobile",
      property_key="name",
      property_value="18610558465"
)
print(find_code_1)
if find_code_1 is None:
    mobile_node_1 = Node("Mobile", name="18610558465")
    test_graph.create(mobile_node_1)
else:
    print("Exist")

find_code_2 = test_graph.find_one(
  label="Mobile",
      property_key="name",
      property_value="13147785424"
)
print(find_code_2)
if find_code_2 is None:
    mobile_node_2 = Node("Mobile", name="13147785424")
    test_graph.create(mobile_node_2)
else:
    print("Exist")

find_code_3 = test_graph.find_one(
  label="Merchant",
      property_key="name",
      property_value="50668"
)
print(find_code_3)
if find_code_3 is None:
    mer_node_2 = Node("Merchant", name="50668")
    test_graph.create(mer_node_2)
    node_1_call_node_2 = Relationship(mobile_node_1,'HasTrxIn',mer_node_2)
    node_1_call_node_2['count'] = 4
    node_2_call_node_1 = Relationship(mobile_node_2,'HasTrxIn',mer_node_2)
    node_2_call_node_1['count'] = 6
    test_graph.create(node_1_call_node_2)
    test_graph.create(node_2_call_node_1)
else:
    print("Exist")





# find_code_1 = test_graph.find_one(
#   label="Person",
#   property_key="name",
#   property_value="test_node_1"
# )
# find_code_2 = test_graph.find_one(
#   label="Person",
#   property_key="name",
#   property_value="test_node_2"
# )
# print(find_code_1["name"])
# find_relationship = test_graph.match_one(start_node=find_code_1,end_node=find_code_2,bidirectional=False)
# print(find_relationship)

print("Done")