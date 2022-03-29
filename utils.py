# -*- coding: utf-8 -*-
import hashlib


def _md5_hash(string):
    m = hashlib.md5()
    m.update(bytes(string, "UTF-8"))
    return str(int(m.hexdigest(), 16))[0:12]
