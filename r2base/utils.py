from typing import Sequence
import uuid

from collections import OrderedDict

def chunks(l: Sequence, win_len: int, stride_len: int):
    s_id = 0
    e_id = min(len(l), win_len)

    while True:
        yield l[s_id:e_id]

        if e_id == len(l):
            break

        s_id = s_id + stride_len
        e_id = min(s_id + win_len, len(l))


def get_uid() -> str:
    return str(uuid.uuid1().int >> 65)


class LRUCache:
    # initialising capacity
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    # we return the value of the key
    # that is queried in O(1) and return -1 if we
    # don't find the key in out dict / cache.
    # And also move the key to the end
    # to show that it was recently used.
    def get(self, key):
        if key not in self.cache:
            return None
        else:
            self.cache.move_to_end(key)
            return self.cache[key]

    # first, we add / update the key by conventional methods.
    # And also move the key to the end to show that it was recently used.
    # But here we will also check whether the length of our
    # ordered dictionary has exceeded our capacity,
    # If so we remove the first key (least recently used)
    def put(self, key, value) -> None:
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def pop(self, key, value):
        self.cache.pop(key, None)