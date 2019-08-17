import json


class Transaction(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __repr__(self):
        return "<Transaction: {}>".format(json.dumps(self.kwargs))
