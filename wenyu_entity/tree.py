# -*- coding:utf-8 -*-

# Name: tree
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/23 9:55


class Node(object):

    def __init__(self, pre=None, next=None, data=None):
        self.pre = pre
        self.next = next
        self.data = data

    def pre(self, pre):
        if type(self.pre) is list:
            self.pre += pre if type(pre) is list else [pre]
        else:
            self.pre = pre

    def next(self, next):
        if type(self.next) is list:
            if type(next) is list:
                self.next += next
            else:
                self.next.append(next)
        else:
            self.next = next

    @property
    def parents(self):
        return self.pre if type(self.pre) is list else [self.next]

    @property
    def grands(self):
        result = []
        for node in self.parents:
            for parent in node.parents:
                if parent not in result:
                    result.append(parent)
        return result

    @property
    def children(self):
        return self.next if type(self.next()) is list else [self.next]

    @property
    def brothers(self):
        return self.get_brothers()

    def get_brothers(self, include_me=False):
        result = []
        for node in self.parents:
            for child in node.children:
                if child not in result and child != self and not include_me:
                    result.append(child)
        return result

    def add_brother(self, node):
        self.pre.next(node)

    @property
    def uncles(self):
        uncles = []
        for parent in self.parents:
            for brother in parent.brothers:
                if brother not in uncles:
                    uncles.append(brother)
        return uncles

    @property
    def cousins(self):
        cousins = []
        for uncle in self.uncles:
            for child in uncle.children:
                if child not in cousins:
                    cousins.append(child)
        return cousins

    def add_cousin(self, node, uncle):
        uncles = self.uncles
        if uncle and uncle in uncles:
            uncle.next(node)


class Tree(object):

    node_dict = {}

    def __init__(self, root: Node, init_level=0):
        self.root = root
        self.node_dict[init_level] = [root]

    @property
    def levels(self):
        return self.get_levels()

    @property
    def levels_reversed(self):
        return self.get_levels(reverse=False)

    def get_levels(self, reverse=False):
        keys = list(self.node_dict.keys())
        keys.sort(reverse=reverse)
        return keys

    def remove_parents(self, level: int):
        for node in self.node_dict[level]:
            node.pre = None

    def remove_children(self, level: int):
        for node in self.node_dict[level]:
            node.next = None

    def insert(self, ins_level: int, nodes):
        for level in self.levels_reversed:
            if level >= ins_level:
                self.node_dict[level + 1] = self.node_dict[level]
            else:
                break
        self.node_dict[ins_level] = nodes
        self.remove_parents(ins_level - 1)
        for node in self.node_dict[ins_level]:
            for parent in node.parents:
                if parent in node[ins_level - 1]:
                    parent.next(node)
            for child in node.children:
                if child in node[ins_level + 1]:
                    child.pre(node)

    def remove(self, rmv_level: int):
        for level in self.levels:
            if level == rmv_level - 1:
                self.remove_children(level)
            elif level >= rmv_level:
                if self.node_dict.get(level + 1):
                    self.node_dict[level] = self.node_dict[level + 1]
                else:
                    self.node_dict[level] = None
                if level == rmv_level:
                    self.remove_parents(level)

    def append(self, level: int, nodes):
        level = self.levels[-1]
        for node in nodes:
            for parent in node.parents:
                if parent in self.levels[level]:
                    parent.next(node)
        self.node_dict[level + 1] = nodes
