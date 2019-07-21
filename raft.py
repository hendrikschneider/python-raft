# Hendrik Schneider

import asyncio, argparse
import json
import logging
import random
import websockets
import queue
import sys

from datetime import datetime

from blockchain import Blockchain
from transactions import Transaction
from block import Block
from myjsonencoder import MyJSONEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--listen', type=str,
                    help='ip and port: example: 127.0.0.1:8000')
args = parser.parse_args()


class RaftServer():
    STATE_FOLLOWER = 'follower'
    STATE_CANDIDATE = 'candidate'
    STATE_LEADER = 'leader'

    FORWARDED_KEY = "forwarded"

    HEARTBEAT_INTERVAL = 0.5

    ELECTION_TIMEOUT = 0.3

    nodes = [
        "127.0.0.1:8007",
        "127.0.0.1:8009",
        "127.0.0.1:8008",
    ]

    votes = {}

    _connections_to_servers = {}

    connected_clients = set()
    raft_nodes = set()
    control_connection = set()

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.identifier = "{}:{}".format(self.host, self.port)

        self.blockchain = Blockchain("/tmp/blockchain_{}.json".format(self.identifier))
        self.blockchain.load()

        self._state = self.STATE_FOLLOWER
        self._currentTerm = 0
        self._commitIndex = 0
        self._lastApplied = 0
        self.votes[self.identifier] = self.identifier
        self._leader = None
        self._last_heartbeat = datetime.now()
        self._is_syncronizing = False
        self._backlog = queue.Queue()

    async def __aenter__(self):

        # Start Websocket Server
        await self.start_server()

        # Start raft state management
        asyncio.ensure_future(self.manage_raft_state(), loop=asyncio.get_event_loop())

        # Connect to other servers
        asyncio.ensure_future(self.periodic_connect_task(), loop=asyncio.get_event_loop())

        return self

    async def __aexit__(self, *args, **kwargs):
        await self._ws_server.__aexit__(*args, **kwargs)
        # TODO: close connections

    async def send(self, message):
        pass
        # await self.websocket.send(message)

    async def receive(self):
        pass
        # return await self.websocket.recv()

    async def start_election(self):
        self._currentTerm += 1
        self._state = self.STATE_CANDIDATE
        self.votes[self.identifier] = self.identifier
        await self.broadcast_to_servers({
            "term": self._currentTerm,
            "type": "vote",
            "identifier": self.votes[self.identifier]
        })

    async def manage_raft_state(self):
        """
        Manages raft stuff
        """
        self.leader_rounds = 2000
        await asyncio.sleep(2)
        while True:
            # No leader and missing heartbeat -> start new election
            if self._state == self.STATE_LEADER and self.leader_rounds > 0:
                self.leader_rounds -= 1
                # TODO: uncomment  + check if queue is empty
                self._last_heartbeat = datetime.now()
                await self.broadcast_to_clients({
                    "term": self._currentTerm,
                    "type": "heartbeat",
                    "identifier": self.identifier
                })

            # Force reelection after 20 heartbeats
            if self.leader_rounds == 0:
                self._state = self.STATE_CANDIDATE
                self.leader_rounds = 20
                election_timeout = random.randint(100, 200) / 1000
                # election_timeout = random.randint(1, 3)
                await asyncio.sleep(election_timeout)

            heartbeat_timed_out = (
                                          datetime.now() - self._last_heartbeat).total_seconds() > self.HEARTBEAT_INTERVAL * 1.5
            if heartbeat_timed_out and self._state != self.STATE_LEADER and not self._is_syncronizing:
                election_timeout = random.randint(150, 300) / 1000
                # election_timeout = random.randint(1, 3)
                await asyncio.sleep(election_timeout)
                if len(self._connections_to_servers.keys()) > 0:
                    await self.start_election()

            logger.debug("State: {}".format(self._state))
            logger.debug("Elected leader: {}".format(self._leader))
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)

    async def start_server(self):
        logger.info("Starting websocket server")
        self._ws_server = websockets.serve(lambda ws, msg: self.server_on_message(ws, msg), self.host, self.port)
        asyncio.ensure_future(self._ws_server, loop=asyncio.get_event_loop())

    async def force_clients_to_sync_with_me(self):
        for ws in self.raft_nodes:
            await self.sync_client(ws)

    async def client_recv_handler(self, connection):
        """
        Handler to communicate with the other servers
        """
        while True:
            data = await connection.recv()
            message = json.loads(data)
            logger.info('Received message at client from {}:{} > {}'.format(*connection.remote_address, message))
            if message['type'] == 'vote':
                self.votes["{}:{}".format(*connection.remote_address)] = message['identifier']

                my_votes = sum(x == self.identifier for x in self.votes.values())
                if my_votes >= len(self.votes.keys()) / 2.0:
                    logger.info("I am the new leader! \o/")
                    first_election = self._leader is None
                    self.votes = {}
                    self._state = self.STATE_LEADER
                    self._leader = self.identifier
                    await self.broadcast_to_servers({
                        "term": self._currentTerm,
                        "type": "heartbeat",
                        "identifier": self.identifier
                    })
                    if first_election:
                        # TODO: Negiate sync
                        # Send length of chain and last block
                        # If my chain is shorter, make sync
                        # needs to be done by leader and follower
                        await self.force_clients_to_sync_with_me()

            elif message['type'] == 'heartbeat':
                self._state = self.STATE_FOLLOWER
                self._leader = message['identifier']
                self._last_heartbeat = datetime.now()
                # print('new leader for server', self.identifier)

            elif message['type'] == 'start_sync':
                logging.info("Start sync")
                self._is_syncronizing = True

            elif message['type'] == 'sync':
                logger.info("Sync in progress")
                json_chain = json.loads(message['data'])
                blockchain_candidate = Blockchain(None)
                blockchain_candidate.import_chain_from_json(json_chain)
                print(blockchain_candidate)
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
                        else:
                            logger.error(
                                "The loaded blockchain from does not belong to this network. Aborting!!!")
                            # TODO: close connections
                            # TODO: stop programm asyncio.get_event_loop().stop()
                            sys.exit()
                    else:
                        logger.info("My blockchain is longer. Do not accept it.")
                        await self.force_clients_to_sync_with_me()
                    # self.blockchain.clear()

                self.blockchain.save()
                # sync is done
                self._is_syncronizing = False

            elif message['type'] == 'commit':
                data = json.loads(message["data"])
                logger.info("Committing message: {}".format(data))
                self._backlog.put(data)
                if not self._is_syncronizing:
                    while not self._backlog.empty():
                        print(self._backlog.qsize())
                        data = self._backlog.get()
                        new_transaction = Transaction(data['pending_messages'][0]['user'],
                                                      data['pending_messages'][0]['timestamp'],
                                                      data['pending_messages'][0]['session'],
                                                      data['pending_messages'][0]['message'])
                        data['pending_messages'] = [new_transaction]
                        self.blockchain.add_block(Block(**data))
                        self.blockchain.save()
                        print("success")
                        # confirm to leader

            # logger.debug("Websocket client message recieved: {}".format(message))

    async def connect_to_server(self, uri):
        if not "{}:{}".format(self.host, self.port) in uri:
            logger.info("Trying to connect to {}".format(uri))
            # Do not connect to yourself
            try:
                ws = websockets.connect(uri)
                connection = await ws.__aenter__()
                self._connections_to_servers["{}:{}".format(*connection.remote_address)] = connection
                asyncio.ensure_future(self.client_recv_handler(connection), loop=asyncio.get_event_loop())
                await connection.send(json.dumps({
                    'type': 'register',
                    'data': 'node'
                }))
            except:
                logger.error("Connection failed: {}".format(uri))

    async def periodic_connect_task(self):
        """
            Try to connect to configured nodes every 5 seconds.
        """
        while True:

            for node in self.nodes:
                if node not in self._connections_to_servers.keys():
                    await self.connect_to_server("ws://{}".format(node))
            await asyncio.sleep(2)

    async def send_heartbeat(self):
        while True:
            await self.broadcast_to_servers({
                "term": self._currentTerm,
                "type": "heartbeat",
                "identifier": self.identifier
            })
            asyncio.sleep(self.HEARTBEAT_INTERVAL)

    async def sync_client(self, ws):
        await ws.send(json.dumps({
            "type": "start_sync"
        }))

        await ws.send(json.dumps({
            "type": "sync",
            "data": self.blockchain.export_as_json()
        }))

    async def server_on_message(self, websocket, path):
        # register(websocket) sends user_event() to websocket
        # Register.
        self.connected_clients.add(websocket)
        try:
            # Implement logic here.
            logger.info("New connection from: {}:{}".format(*websocket.remote_address))
            if self._state == self.STATE_LEADER:
                print("I am leader.. telling connecting guy")
                await websocket.send(json.dumps({
                    "term": self._currentTerm,
                    "type": "heartbeat",
                    "identifier": self.identifier
                }))

                await self.sync_client(websocket)

            while True:
                try:
                    data = await websocket.recv()
                    message = json.loads(data)

                    await self.handle_message_exchange(message)

                    forwarded = message.get(self.FORWARDED_KEY, False)
                    if forwarded and self._state != self.STATE_LEADER:
                        # received forwarded message to leader but I am not the leader -> ignore it
                        continue

                    logger.info('Received message at server from {}:{} > {}'.format(*websocket.remote_address, message))

                    if message['type'] == 'vote' and message['term'] >= self._currentTerm:
                        self._state = self.STATE_FOLLOWER
                        self._leader = message['identifier']
                        self.votes[self.identifier] = message['identifier']
                        self._currentTerm = message['term']
                        self._last_heartbeat = datetime.now()

                        await websocket.send(json.dumps({
                            "term": self._currentTerm,
                            "type": "vote",
                            "identifier": self.votes[self.identifier]
                        }))

                    elif message['type'] == 'heartbeat':
                        if self._state == self.STATE_CANDIDATE:
                            logger.info("New leader elected: {}".format(self._leader))
                            self._state = self.STATE_FOLLOWER
                        self._leader = message['identifier']
                        self._currentTerm = message['term']
                        self._last_heartbeat = datetime.now()

                    elif message['type'] == 'register':
                        # TODO: register control connection in such a way that raft algo does not consider it a candidate
                        if message['data'] == 'node':
                            # append to node list -> node is part of raft
                            self.raft_nodes.add(websocket)
                        elif message['data'] == 'control':
                            # append to control
                            self.control_connection.add(websocket)

                    elif message['type'] == "add_node":
                        # add node and send to all others
                        # 
                        #    {"address": "127.0.0.1:8009"}
                        # 
                        if self._state != self.STATE_LEADER:
                            # send it to leader for distribution
                            await self.forward_to_leader(message)
                        else:
                            new_node = message['address']
                            if not new_node in self.nodes:
                                logger.info('Adding new node: {}'.format(new_node))
                                self.nodes.append(new_node)
                            else:
                                logger.info("updating followers about new node")
                                await self.broadcast_to_clients(message)

                    elif message['type'] == 'commit':
                        if self._state != self.STATE_LEADER:
                            # send it to leader for distribution
                            await self.forward_to_leader(message)
                        else:
                            # I am leader -> mine -> broadcast
                            # mine here and broadcast block
                            data = json.loads(message["data"])
                            if self.blockchain.is_empty():
                                # init blockchain
                                self.blockchain.initialize_chain()
                                await self.sync_last_block()

                            self.blockchain.add_transaction(data['message'], data['user'], data['session'])
                            self.blockchain.mine_block()

                            # we need confirmation first
                            self.blockchain.save()

                            await self.sync_last_block()

                    # print("My state", self._state)
                    # self.broadcast(message)

                except Exception as e:
                    # logger.error(e)
                    if websocket in self.connected_clients:
                        self.connected_clients.remove(websocket)
                    # pruefen, ob in sets vorhanden und dann er löschen
                    # connected clients mit raft nodes ersetzen
                    if websocket in self.raft_nodes:
                        self.raft_nodes.remove(websocket)
                    if websocket in self.control_connection:
                        self.control_connection.remove(websocket)
                    break
                    # sys.exit()
                    # TODO: Error handling
        finally:
            if websocket in self.connected_clients:
                self.connected_clients.remove(websocket)

    async def sync_last_block(self):
        await self.broadcast_to_clients({
            "type": "commit",
            "data": json.dumps(self.blockchain.last_block(), ensure_ascii=False, cls=MyJSONEncoder)
        })

    async def forward_to_leader(self, message):
        # TODO: Just send to leader
        logger.info('Forwarding message to server')
        message[self.FORWARDED_KEY] = "true"
        await self.broadcast_to_servers(message)

    async def broadcast_to_clients(self, message):
        """
        Broadcast a message to all connected clients.
        """
        message['term'] = self._currentTerm
        if len(self.raft_nodes):
            for ws in self.raft_nodes:
                try:
                    await ws.send(json.dumps(message))
                except Exception as e:
                    logger.error("error while sending to client: {}".format(e))
            # await asyncio.wait([ws.send(json.dumps(message)) for ws in self.connected_clients])

    async def broadcast_to_servers(self, message):
        """
        Broadcast a message to all connected clients.
        """
        message['term'] = self._currentTerm
        if len(self._connections_to_servers.keys()):
            for k in self._connections_to_servers.keys():
                try:
                    await self._connections_to_servers[k].send(json.dumps(message))
                except:
                    pass

            # await asyncio.wait([self._connections_to_servers[k].send(json.dumps(message)) for k in
            #                    self._connections_to_servers.keys()])

    async def handle_message_exchange(self, message):
        pass


async def main():
    ip, port = args.listen.split(":")
    async with RaftServer(ip, int(port)) as echo:
        pass


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_forever()
