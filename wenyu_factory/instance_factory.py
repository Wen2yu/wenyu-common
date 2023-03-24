# -*- coding:utf-8 -*-

# Author : 'zhangjiawen'
# Data : 2019-12-08 16:03

import random
import string


instances_dict = {}


def create_instance(cls, instance_dict, key, instance_type=1, *args, **kwargs):
    instance = None
    if instance_type:
        instance = instance_dict.get(key)
    if not instance:
        instance = cls(*args, **kwargs)
    return instance


def gen_key():
    return ''.join(random.sample(string.printable, 64))


class InstanceFactory:
    @staticmethod
    def get_instance(cls, key, *args, **kwargs):
        instance = cls(*args, **kwargs)
        instance_dict = instances_dict.get(cls.__name__)
        if not instance_dict:
            instance_dict = {}
            instances_dict[cls.__name__] = instance_dict
        if key:
            key = gen_key()
        instance_dict[key] = create_instance(cls, instance_dict, key, *args, **kwargs)
        return instance
