from block import Block
from transactions import Transaction
from myjsonencoder import MyJSONEncoder
import datetime, time, uuid
import json
import os

class Blockchain(object):
    def __init__(self, storage_path):
        self._chain = []
        self._difficulty = 3
        self.pending_transactions = []
        self.nounce = 0
        self.nodes = set()
        self.storage_path = storage_path

    @property
    def get_chain(self):
        return [vars(block) for block in self._chain]

    def generateGenesisBlock(self):
        transaction_0 = Transaction('Root', self.get_time(), '0', 'Genesis Block')
        local_transaction = [transaction_0]
        genesis = Block(0, self.get_time(), local_transaction, 0, 0)
        genesis.hash = '000'
        return genesis

    def get_time(self):
        """
        Returns the current time in milliseconds
        :return:
        """
        timestamp = datetime.datetime.now()
        return timestamp.microsecond

    def add_transaction(self, message, user, session):
        # Create a new transaction and append it to the list of all transactions
        timestamp = self.get_time()
        transaction = Transaction(user, timestamp, session, message)
        self.pending_transactions.append(transaction)

    def mine_block(self):
        timestamp = self.get_time()
        previousHash = self.last_block().hash
        index = self._chain.index(self.last_block()) + 1
        block = self.new_block(index, timestamp, previousHash)
        return vars(block)

    def new_block(self, index, timestamp, previousHash):
        # Creates a new Block and adds it to the chain

        # timestamp = datetime.datetime.now()
        # previousHash = self.last_block().hash
        # message = " Passing some data : " + str(index)
        # self.pending_transactions.append(message)
        block = Block(index, timestamp, self.pending_transactions, previousHash, self.nounce)
        block.calculateHash() # proof_of_work(self._difficulty)
        self.pending_transactions = []
        self._chain.append(block)
        return block

    def last_block(self):
        # Returns the last Block in the chain
        return self._chain[len(self._chain) - 1]

    def is_block_valid(self, current_block, previous_block, difficulty):
        if previous_block.index + 1 != current_block.index:
            print("Failed to validate block, mismatch with previous block\n")
            return False
        elif previous_block.hash != current_block.previousHash:
            print("Failed to validate block, mismatch of current and previous hash\n")
            return False
        #elif not current_block.is_proof_valid(difficulty):
        #    print("Failed to validate block, proof is invalid\n")
        #    return False
        # elif time.strptime(current_block.timestamp, "%d/%m/%Y") < time.strptime(previous_block.timestamp, "%d/%m/%Y"):
        #     print("Failed to validate block, mismatch of block order\n")
        #     return False
        elif current_block.hash != current_block.calculateHash():
            print("Failed to validate block, hash calculation of the current block is invalid...\n")
            return False
        else:
            print("Block is valid\n")
            return True

    def is_chain_valid(self):
        for block in range(1, len(self._chain)):
            current_block = self._chain[block]
            previous_block = self._chain[block - 1]

            # if not self.is_block_valid(current_block, previous_block):
            #     return False
            self.is_block_valid(current_block, previous_block, self._difficulty)
        return True

    def add_block(self, block):
        """
        Checks vadility of the block and appends it to the blockchain.
        :param block:
        :return:
        """
        if len(self._chain) == 0:
            # chain is empty, accept first block without check
            self._chain.append(block)
        elif self.is_block_valid(block, self._chain[-1], self._difficulty):
            self._chain.append(block)

    def is_empty(self):
        """
        return true if the chain is empty
        :return:
        """
        return len(self._chain) == 0

    def initialize_chain(self):
        self._chain.append(self.generateGenesisBlock())

    def save(self):
        with open(self.storage_path, "w") as f:
            f.write(self.export_as_json())

    def load(self):
        if os.path.exists(self.storage_path):
            with open(self.storage_path, "r") as f:
                json_chain = json.loads(f.read())
                for json_block in json_chain:
                    self.import_block(json_block)

    def import_block(self, data):
        new_transaction = Transaction(data['pending_messages'][0]['user'], data['pending_messages'][0]['timestamp'],
                                      data['pending_messages'][0]['session'], data['pending_messages'][0]['message'])
        data['pending_messages'] = [new_transaction]
        self.add_block(Block(**data))

    def export_as_json(self):
        return json.dumps(self._chain, cls=MyJSONEncoder)

    def clear(self):
        self._chain = []

    # def add_peer_node(self, address):
    #     self.nodes.add(address)
    #     return True

    # def get_block_object_from_block_data(self, block_data):
    #     pass

    def __repr__(self):
        return "<chain:%s>\n" % (self._chain)

# blockchain = Blockchain()

# blockchain.add_transaction("popup_1","Grigor", 1)
# blockchain.add_transaction("popup_2","Wladislav", 2)

# blockchain.mine_block()
# last_block = blockchain.last_block()

# print(repr(last_block))

# validate = blockchain.is_chain_valid()
# print (validate)
