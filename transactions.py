import json


class Transaction(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        # for k, v in kwargs:
        #    setattr(self, k, v)

    def __repr__(self):
        return "<Transaction: {}>".format(json.dumps(self.kwargs))
        # return "<Test user:%s timestamp:%s session:%s  message:%s>" % (self.user, self.timestamp, self.session, self.message)

"""
class Transaction(object):
    def __init__(self, user, timestamp, session, message):
        self._user = user
        self._timestamp = timestamp
        self._session = session
        self._message = message

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, value):
        self._user = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, value):
        self._session = value

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, value):
        self._message = value

    def __repr__(self):
        return "<Test user:%s timestamp:%s session:%s  message:%s>" % (self.user, self.timestamp, self.session, self.message)
"""