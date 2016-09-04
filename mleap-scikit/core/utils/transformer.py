import uuid


def uid(base):
    return "{}_{}".format(base, uuid.uuid1())