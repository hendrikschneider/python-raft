import asyncio, argparse
import json
import logging
import random
import websockets
import queue
import sys
import uuid
import operator

from datetime import datetime

from blockchain import Blockchain
from transactions import Transaction
from block import Block
from TransactionJSONEncoder import TransactionJSONEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--listen', type=str,
                    help='ip and port: example: 127.0.0.1:8000')
args = parser.parse_args()


class RaftServer(object):
    """
    This class implements a blockchain based on the raft consensus algorithm. Therefore, it starts a websocket server and
    connects to the other raft nodes, setting up a peer2peer connection between all raft nodes. The consensus across all
    nodes is achieved by determining a leader, who is then responsible for mining and sharing new block. Instances that
    are followers will verify the newly mined block and append it to their blockchain. After a period of five minutes
    a new leadership election will automatically start.
    """
    STATE_FOLLOWER = 'follower'
    STATE_CANDIDATE = 'candidate'
    STATE_LEADER = 'leader'

    KEY_FIRST_VOTE = 'not elected'

    FORWARDED_KEY = "forwarded"

    HEARTBEAT_INTERVAL = 0.5

    ELECTION_TIMEOUT = 0.3

    LEADER_HEARTBEATS = 300 / 0.5  # 5 Minutes

    votes = {}

    _connections_to_servers = {}

    connected_clients = set()
    raft_nodes = set()
    control_connection = set()
    confirmed_commits = {}

    node_connections = {}

    def __init__(self, host, port, nodes, control_channels, blockchain_path):
        self.host = host
        self.port = port
        self.identifier = "{}:{}".format(self.host, self.port)

        self.nodes = nodes
        self.control_channels = control_channels

        self.blockchain = Blockchain(blockchain_path)
        self.blockchain.load()

        self._state = self.STATE_FOLLOWER
        self._currentTerm = 0
        self._commitIndex = 0
        self._lastApplied = 0
        self.votes[self.identifier] = self.identifier
        self._leader = self.KEY_FIRST_VOTE
        self._last_heartbeat = datetime.now()
        self._is_syncronizing = False
        self._backlog = queue.Queue()
        self.messages_to_save = {}
        self.heartbeats_left = self.LEADER_HEARTBEATS
        self.has_voted = False

    async def __aenter__(self):
        """
        Start websocket server and raft management.
        :return:
        """
        # Start Websocket Server
        await self.start_server()

        # Start raft state management
        asyncio.ensure_future(self.manage_raft_state(), loop=asyncio.get_event_loop())

        # Connect to other servers
        asyncio.ensure_future(self.periodic_connect_task(), loop=asyncio.get_event_loop())

        return self

    async def __aexit__(self, *args, **kwargs):
        await self._ws_server.__aexit__(*args, **kwargs)

    async def start_election(self):
        """
        Start election process
        :return:
        """
        self._currentTerm += 1
        self._state = self.STATE_CANDIDATE
        self.votes[self.identifier] = self.identifier
        self._leader = None
        self.has_voted = True
        await self.broadcast_to_clients({
            "type": "vote",
            "identifier": self.votes[self.identifier]
        })

    async def manage_raft_state(self):
        """
        Manages raft states.
        """
        await asyncio.sleep(2)
        while True:
            # No leader and missing heartbeat -> start new election
            if self._state == self.STATE_LEADER and self.heartbeats_left > 0:
                self.heartbeats_left -= 1
                # TODO: uncomment  + check if queue is empty
                self._last_heartbeat = datetime.now()
                await self.broadcast_to_clients({
                    "type": "heartbeat",
                    "identifier": self.identifier
                })

            # Force reelection after 20 heartbeats
            if self.heartbeats_left == 0:
                self._state = self.STATE_CANDIDATE
                self.heartbeats_left = self.LEADER_HEARTBEATS
                election_timeout = random.randint(100, 200) / 1000
                # election_timeout = random.randint(1, 3)
                await asyncio.sleep(election_timeout)

            if self.heartbeat_timed_out() and self._state != self.STATE_LEADER and not self._is_syncronizing:
                election_timeout = random.randint(150, 300) / 1000
                # election_timeout = random.randint(1, 3)
                await asyncio.sleep(election_timeout)
                await self.start_election()

            logger.debug("State: {}".format(self._state))
            logger.debug("Elected leader: {}".format(self._leader))
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)

    def heartbeat_timed_out(self):
        return (datetime.now() - self._last_heartbeat).total_seconds() > self.HEARTBEAT_INTERVAL * 1.5

    async def start_server(self):
        """
        Start websocket server.
        :return:
        """
        logger.info("Starting websocket server")
        self._ws_server = websockets.serve(lambda ws, msg: self.server_on_message(ws, msg), self.host, self.port)
        asyncio.ensure_future(self._ws_server, loop=asyncio.get_event_loop())

    async def force_clients_to_sync_with_me(self):
        """
        Force connected raft nodes to sync their blockchain to the local one.
        :return:
        """
        for ws in self.raft_nodes:
            await self.sync_client(ws)

    async def client_recv_handler(self, connection):
        """
        Handler to communicate with the other servers
        """
        try:
            while True:
                data = await connection.recv()
                message = json.loads(data)

                await self.handle_message_exchange(message, connection)
        except Exception as e:
            pass

    async def connect_to_server(self, uri, handler):
        """
        Connect to other raft server if no connection has been made so far.
        :param uri:
        :param handler:
        :return:
        """
        if not "{}:{}".format(self.host, self.port) in uri:
            # Do not connect to yourself
            try:
                ws = websockets.connect(uri)
                connection = await ws.__aenter__()
                self._connections_to_servers["{}:{}".format(*connection.remote_address)] = connection
                asyncio.ensure_future(handler(connection), loop=asyncio.get_event_loop())
                await self.on_connect(connection)
            except Exception as e:
                logger.error("Connection failed: {} {}".format(uri, e))

    async def register_as_raft_node(self, websocket):
        await websocket.send(json.dumps({
            'type': 'register',
            'data': 'node',
            'identifier': self.identifier
        }))

    async def control_recv_handler(self, connection):
        """
        Handler to communicate with the other servers
        """
        while True:
            data = await connection.recv()
            message = json.loads(data)
            logger.info('Received message at control from {}:{} > {}'.format(*connection.remote_address, message))
            await self.handle_message_exchange(message, connection)

    async def periodic_connect_task(self):
        """
        Try to connect to configured nodes every 5 seconds.
        """
        self.ws_control_channel = None
        while True:
            for node in self.nodes:
                if node not in self._connections_to_servers and node not in self.node_connections:
                    await self.connect_to_server("ws://{}".format(node), self.client_recv_handler)
            for control_channel in self.control_channels:
                # await self.connect_to_server("ws://{}".format(control_channel), self.control_recv_handler)
                if self.ws_control_channel is None:
                    self.ws_control_channel = True
                    await self.connect_to_server("ws://{}".format(control_channel), self.control_recv_handler)

            await asyncio.sleep(2)

    async def send_heartbeat(self):
        """
        Send heartbeat fo connected raft nodes.
        :return:
        """
        await self.broadcast_to_clients({
            "type": "heartbeat",
            "identifier": self.identifier
        })

    async def sync_client(self, ws):
        """
        Send the local blockchain to the connected raft node.
        :param ws:
        :return:
        """
        await ws.send(json.dumps({
            "type": "start_sync"
        }))

        await ws.send(json.dumps({
            "type": "sync",
            "data": self.blockchain.export_as_json()
        }))

    async def server_on_message(self, websocket, path):
        """
        Register new connections to server, manages a constant connection and disconnect of clients.
        Incoming messages will be passed on the handle_message_echange function.
        :param websocket:
        :param path:
        :return:
        """
        # Register websocket
        self.connected_clients.add(websocket)
        try:
            await self.on_connect(websocket)

            while True:
                try:
                    data = await websocket.recv()
                    message = json.loads(data)

                    await self.handle_message_exchange(message, websocket)
                except Exception as e:
                    logger.error(e)
                    if websocket in self.connected_clients:
                        self.connected_clients.remove(websocket)
                    if websocket in self.raft_nodes:
                        self.raft_nodes.remove(websocket)
                    if websocket in self.control_connection:
                        self.control_connection.remove(websocket)
                    if websocket in self.node_connections:
                        self.node_connections.remove(websocket)
                    break
        finally:
            if websocket in self.connected_clients:
                self.connected_clients.remove(websocket)

    async def sync_last_block(self, message_uuid):
        """
        Send the last block of the local blockchain to all connected raft nodes
        :param message_uuid:
        :return:
        """
        self.confirmed_commits[self._currentTerm] = {'confirmations': set(),
                                                     'data': self.blockchain.last_block().pending_messages[0]}
        await self.broadcast_to_clients({
            "type": "commit",
            "data": json.dumps(self.blockchain.last_block(), ensure_ascii=False, cls=TransactionJSONEncoder),
            "term": self._currentTerm,
            "uuid": message_uuid
        })

    async def broadcast_to_clients(self, message):
        """
        Broadcast a message to all connected clients.
        """
        message['term'] = self._currentTerm
        self._currentTerm += 1

        # Send to raft nodes that this instance is connected to as a client
        for ws in self.raft_nodes:
            try:
                await ws.send(json.dumps(message))
            except Exception as e:
                logger.error("Error while sending to client: {}".format(e))

    async def handle_message_exchange(self, message, websocket):
        """
        Maps arriving messages to their local handler.
        The value of the key 'type' will determine which message will be called.
        The message will then forwared to the selected handler.
        :param message:
        :param websocket:
        :return:
        """
        logger.info('Received message from {}:{} > {}'.format(*websocket.remote_address, message))

        handler = getattr(self, "on_{}".format(message['type']), None)

        if handler is not None:
            await handler(message, websocket)
        else:
            logger.error("No handler found: {}".format(message))

    async def on_connect(self, websocket):
        """
        Sending newly connected clients immediately an heartbeat to put them into follower mode.
        :param websocket:
        :return:
        """
        logger.info("New connection from: {}:{}".format(*websocket.remote_address))
        await self.register_as_raft_node(websocket)
        if self._state == self.STATE_LEADER:
            # Sending heartbeat to new connection to set me as leader
            await self.send_heartbeat()
            await self.sync_client(websocket)

    async def on_heartbeat(self, message, websocket):
        """
        Handles heartbeat messages. Current instance will go into follower mode and updates last heartbeat timestamp.
        :param message:
        :param websocket:
        :return:
        """
        self._state = self.STATE_FOLLOWER
        self._leader = message['identifier']
        self._last_heartbeat = datetime.now()

    async def on_vote(self, message, websocket):
        self.votes["{}:{}".format(*websocket.remote_address)] = message['identifier']

        if self._state == self.STATE_CANDIDATE:
            if self.heartbeat_timed_out():
                self.votes[self.identifier] = message['identifier']
                self._currentTerm = message['term']
                self._last_heartbeat = datetime.now()

                await self.broadcast_to_clients({
                    "term": self._currentTerm,
                    "type": "vote",
                    "identifier": self.votes[self.identifier]
                })

        # We subtract 1 of len(self.votes), because the current instance also voted but is not part of self.node_connections
        everyone_has_voted = (len(self.votes) - 1) == len(self.node_connections)
        if everyone_has_voted:
            leader = self.get_leader()
            if leader is not None:
                if leader == self.identifier:
                    logger.info("I am the new leader! \o/")
                    first_election = self._leader == self.KEY_FIRST_VOTE
                    # self.votes = {}
                    self._state = self.STATE_LEADER
                    self._leader = self.identifier
                    await self.send_heartbeat()
                    if first_election:
                        # System starts up -> sync chain
                        await self.force_clients_to_sync_with_me()
                else:
                    self._state = self.STATE_FOLLOWER

    def get_websocket_identifier(self, websocket):
        """
        Build a string identifier from websocket object.
        :param websocket:
        :return:
        """
        return "{}:{}".format(*websocket.remote_address)

    async def on_start_sync(self, message, websocket):
        """
        Instance goes into sync mode and queues broadcasted blocks until sync is done.
        :param message:
        :param websocket:
        :return:
        """
        logging.info("Start sync")
        self._is_syncronizing = True

    async def on_sync(self, message, webssocket):
        """
        Receives blockchain from the leader, validates that the shared blockchain is an instance of the local blockchain.
        After sync is done, all queued blocks will be appended to the local blockchain.
        :param message:
        :param webssocket:
        :return:
        """
        logger.info("Sync in progress")
        json_chain = json.loads(message['data'])
        blockchain_candidate = Blockchain(None)
        blockchain_candidate.import_chain_from_json(json_chain)
        if self.blockchain.is_empty():
            # Blockchain is empty -> Import existing one
            logger.info("Importing new blockchain")
            self.blockchain.import_blockchain_from_blockchain_obj(blockchain_candidate)
        else:
            # Node was already part of a raft group with a blockchain
            # -> Verify that imported blockchain is extension of saved one

            # TOOD: What to do, if old chain is longer -> Force sync with me
            if blockchain_candidate.length >= self.blockchain.length:
                # Just try to import if longer
                if self.blockchain.is_partial_blockchain(blockchain_candidate):
                    logger.info("Importing blockchain")
                    self.blockchain.import_blockchain_from_blockchain_obj(blockchain_candidate)
                    logger.info("Blockchain successfully synced")
                else:
                    logger.error(
                        "The loaded blockchain from does not belong to this network. Aborting!!!")
                    sys.exit()
            else:
                logger.info("My blockchain is longer. Do not accept it.")
                await self.force_clients_to_sync_with_me()

        self.blockchain.save()

        # Save messages from backlog
        await self.commit_messages_from_backlog()

        # sync is done
        self._is_syncronizing = False

    async def on_commit(self, message, websocket):
        """
        Shares data that should be stored in the blockchain with the network so that every node can verify the content was not changed.
        If the current instance is leader, a new block will be mined and synced across the network.
        A follower will append the newly mined block to it's local blockchain.
        :param message:
        :param websocket:
        :return:
        """
        if "uuid" not in message:
            # Add id and broadcast to all nodes
            message_uuid = str(uuid.uuid4())
            message['uuid'] = message_uuid

            update_message = {**message}
            update_message['type'] = 'message_broadcast'

            await self.broadcast_to_clients(update_message)

        message_uuid = message['uuid']
        self.messages_to_save[message_uuid] = {**message}

        if self._state == self.STATE_LEADER:
            # Leader is to mine the next block
            logger.info("Mining Block and broadcasting.")
            data = json.loads(message["data"])
            if self.blockchain.is_empty():
                # init blockchain
                self.blockchain.initialize_chain()
                await self.sync_last_block(message_uuid='None')

            # Calculate blockchain
            self.blockchain.add_transaction(**data)
            self.blockchain.mine_block()
            self.blockchain.save()

            del self.messages_to_save[message_uuid]
            await self.sync_last_block(message_uuid=message_uuid)
        elif self._state == self.STATE_FOLLOWER:
            # term will be None if messages comes from control connection
            term = message.get("term", None)
            if term is not None:
                self._backlog.put(message)
                self._currentTerm = term
                if not self._is_syncronizing:
                    await self.commit_messages_from_backlog()

    async def on_message_broadcast(self, message, websocket):
        """
        Saves shared unmined messages to local chache.
        :param message:
        :param websocket:
        :return:
        """
        self.messages_to_save[message['uuid']] = message
        if self._state == self.STATE_LEADER:
            await self.on_commit(message, websocket)

    async def commit_messages_from_backlog(self):
        """
        Validates a block send from the leader and appends it to the local blockchain.
        :return:
        """
        while not self._backlog.empty():
            message = self._backlog.get()
            term = message.get('term')
            broadcasted_message = json.loads(self.messages_to_save[message['uuid']]['data'])
            data = json.loads(message['data'])
            message_integrity = broadcasted_message == data
            if message_integrity:
                new_transaction = Transaction(**json.loads(data['pending_messages'][0]))
                data['pending_messages'] = [new_transaction]
                new_block = Block(**data)
                self.blockchain.add_block(new_block)
                self.blockchain.save()
                await self.broadcast_to_clients({
                    'type': 'client_commit_confirm',
                    'source_term': term
                })
            else:
                await self.start_election()

    async def on_register(self, message, websocket):
        """
        Every newly connected node must register to state its role. A connection can either be part of the raft network
        or a control connection to saves messages in the network.
        :param message:
        :param websocket:
        :return:
        """
        if message['data'] == 'node':
            # append to node list -> node is part of raft
            self.raft_nodes.add(websocket)
            self.node_connections[message['identifier']] = websocket
        elif message['data'] == 'control':
            # append to control
            self.control_connection.add(websocket)

    async def on_client_commit_confirm(self, message, websocket):
        """
        Broadcast a message to all control connections after a message has been successfully committed by all
        nodes.
        :param message:
        :param websocket:
        :return:
        """
        if self._state == self.STATE_LEADER:
            try:
                term = message['source_term']
                self.confirmed_commits[term]['confirmations'].add(self.get_websocket_identifier(websocket))
                if len(self.confirmed_commits[term]['confirmations']) == len(self.raft_nodes):
                    # send confirm to control channel

                    data = json.dumps(self.confirmed_commits[term]['data'], ensure_ascii=False,
                                      cls=TransactionJSONEncoder)
                    del self.confirmed_commits[term]
                    await self.broadcast_to_control_connections({
                        'type': 'commit_confirm',
                        'data': data
                    })
            except Exception as e:
                print("Error", e)

    async def on_commit_confirm(self, message, websocket):
        """
        Forward confirmation message to all connectec control connections
        :param message:
        :param websocket:
        :return:
        """
        await self.broadcast_to_control_connections(message)

    def get_leader(self, ):
        """
        Determines based on the votes the current leader
        :return:
        """
        leader = None

        # count votes
        votes_recieved = {}
        for k, v in self.votes.items():
            if v not in votes_recieved.keys():
                votes_recieved[v] = 0
            votes_recieved[v] += 1

        # determine leader
        sorted_votes = sorted(votes_recieved.items(), key=operator.itemgetter(1))
        max_votes = sorted_votes[-1][1]
        if max_votes >= len(self.votes.keys()) / 2.0:
            leader = sorted_votes[-1][0]
        logger.debug("New leader is: {}".format(leader))
        return leader

    async def broadcast_to_control_connections(self, message):
        """
        Broadcast a message to all connected control connections.
        """
        # Send to raft nodes that this instance is connected to as a client
        for k, ws in self._connections_to_servers.items():
            try:
                await ws.send(json.dumps(message))
            except Exception as e:
                logger.error("Error while sending to control connection: {}".format(e))


nodes = [
    "127.0.0.1:8007",
    "127.0.0.1:8009",
    "127.0.0.1:8008",
    "127.0.0.1:8010",
    "127.0.0.1:8011",
]

control_channels = [
    "127.0.0.1:8000/ws/blockchain/"
]


async def main():
    ip, port = args.listen.split(":")
    blockchain_path = "/tmp/blockchain_{}:{}.json".format(ip, port)
    async with RaftServer(ip, int(port), nodes, control_channels, blockchain_path) as _:
        pass


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_forever()
