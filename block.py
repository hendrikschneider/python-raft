from hashlib import sha256
import random, json


class Block(object):
    def __init__(self, index, timestamp, pending_messages, previousHash, nounce, hash=None):
        self._index = index
        self._timestamp = timestamp
        self.pending_messages = pending_messages
        self.previousHash = previousHash
        self._nounce = nounce
        if hash is None:
            self.hash = self.calculateHash()
        else:
            self.hash = hash
        self.index = self._index

    """"@property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value
    """

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @property
    def pending_messages(self):
        return self._pending_messages

    @pending_messages.setter
    def pending_messages(self, value):
        self._pending_messages = value

    @property
    def previousHash(self):
        return self._previousHash

    @previousHash.setter
    def previousHash(self, value):
        self._previousHash = value

    @property
    def nounce(self):
        return self._nounce

    @nounce.setter
    def nounce(self, value):
        self._nounce = value

    def calculateHash(self):
        if (len(self.pending_messages) >= 1):
            # hashing_message = self.pending_messages[0].message
            hashing_message = self.pending_messages

            messages = str(self._index) + str(self._timestamp) + str(hashing_message) + str(self._previousHash) + str(
                self._nounce)

            output = sha256(messages.encode('utf-8')).hexdigest()

            return output
        else:
            return "No pending messages available"

    def is_proof_valid(self, difficulty):
        comparison_string = "0" * (difficulty)
        if (self.hash[0: difficulty] == comparison_string):
            return True
        return False

    def proof_of_work(self, difficulty):
        comparison_string = "0" * (difficulty)
        # while(self.hash[0 : difficulty] != comparison_string):
        while not self.is_proof_valid(difficulty):
            print("Comparing values - " + str(self.hash[0: difficulty]) + " to " + str(comparison_string))
            self.nounce += 1
            self.hash = self.calculateHash()
        print("\nfinal hash: " + str(self.hash) + "\n")

    def __repr__(self):
        # pnd_msg = []
        # for elem in self.pending_messages:
        #     pnd_msg.append(elem.message)
        return "<Test index:%s timestamp:%s pending_messages:%s previousHash:%s nounce:%s hash:%s>" % (
        self.index, self.timestamp, self.pending_messages, self.previousHash, self.nounce, self.hash)

    # def __str__(self):
    #     return "From str method of Test: a is %s, b is %s" % (self.a, self.b)

# block = Block(1, '01/01/2019', 'random message', '0')

# print (repr(block))
