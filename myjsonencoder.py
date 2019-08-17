from flask.json import JSONEncoder
from transactions import Transaction
from block import Block
import json


class MyJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Transaction):
            return json.dumps(obj.kwargs)
        if isinstance(obj, Block):
            return {
                'index': obj._index,
                'timestamp': obj._timestamp,
                'pending_messages': obj._pending_messages,
                'previousHash': obj._previousHash,
                'nounce': obj._nounce,
                'hash': obj.hash
            }
        return super(MyJSONEncoder, self).default(obj)
