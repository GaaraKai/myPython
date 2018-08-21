# -*- coding: utf-8 -*-
import datetime
import gc
import logging
import os
import sys
import time
import tkinter.filedialog as tf
import tkinter.messagebox as tm

import numpy as np
import pandas as pd

from riskproject.real import config
from itertools import combinations, permutations
from py2neo import Graph, Node, Relationship, Cursor


def conn_neo4j():
    global g
    g = Graph(
        "http://localhost:7474",
        username="neo4j",
        password="china100!"
    )
    g.delete_all()


def create_node():
    global g
    tx = g.begin()

    node1 = Node("MERCHANT_A", name="A")
    node2 = Node("MERCHANT_B", name="B")
    rel = Relationship(node1, "REL", node2)
    rel['count'] = 100
    tx.merge(node1)
    tx.merge(node2)
    tx.merge(rel)
    tx.commit()


conn_neo4j()
create_node()
print("DONE")