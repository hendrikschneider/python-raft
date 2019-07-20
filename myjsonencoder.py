from flask.json import JSONEncoder
from transactions import Transaction
from block import Block


class MyJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Transaction):
            return {
                'user': obj._user,
                'timestamp': obj._timestamp,
                'session': obj._session,
                'message': obj._message
            }
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
