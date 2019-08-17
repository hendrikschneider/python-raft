from block import Block
from transactions import Transaction
from TransactionJSONEncoder import TransactionJSONEncoder
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
    def length(self):
        return len(self._chain)

    @property
    def get_chain(self):
        return [vars(block) for block in self._chain]

    def get_block(self, i):
        return self._chain[i]

    def generateGenesisBlock(self):
        transaction_0 = Transaction(type='Root', timestamp=self.get_time(), message='Genesis Block')
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

    def add_transaction(self, *args, **kwargs):
        # Create a new transaction and append it to the list of all transactions
        timestamp = self.get_time()
        transaction = Transaction(timestamp=timestamp, **kwargs)
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
        block.calculateHash()  # proof_of_work(self._difficulty)
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
        # elif not current_block.is_proof_valid(difficulty):
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
                self.import_chain_from_json(json_chain)

    def import_block(self, data):
        pending_message = json.loads(data['pending_messages'][0])
        new_transaction = Transaction(**pending_message)
        data['pending_messages'] = [new_transaction]
        self.add_block(Block(**data))

    def import_chain_from_json(self, json_chain):
        for json_block in json_chain:
            self.import_block(json_block)

    def export_as_json(self):
        """
        Returns a json representation of the blockchain
        :return:
        """
        return json.dumps(self._chain, cls=TransactionJSONEncoder)

    def clear(self):
        """
        Resets the blockchain
        :return:
        """
        self._chain = []

    def is_partial_blockchain(self, blockchain):
        if self.length > blockchain.length:
            raise Exception("Comparision failed. Passed blockchain cannot be longer than the current one.")
        for i in range(0, self.length):
            if self._chain[i].hash != blockchain.get_block(i).hash:
                return False
        return True

    def import_blockchain_from_blockchain_obj(self, blockchain_obj):
        self._chain = blockchain_obj._chain

    def __repr__(self):
        return "<chain:%s>\n" % (self._chain)
