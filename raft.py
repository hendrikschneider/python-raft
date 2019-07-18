#Hendrik Schneider

import asyncio, argparse
import json
import logging
import random
import websockets

from datetime import datetime

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

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.identifier = "{}:{}".format(self.host, self.port)

        self._state = self.STATE_FOLLOWER
        self._currentTerm = 0
        self._commitIndex = 0
        self._lastApplied = 0
        self.votes[self.identifier] = self.identifier
        self._leader = None
        self._last_heartbeat = datetime.now()

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
        #TODO: close connections

    async def send(self, message):
        pass
        #await self.websocket.send(message)

    async def receive(self):
        pass
        #return await self.websocket.recv()

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
        self.leader_rounds = 20
        while True:
            # No leader and missing heartbeat -> start new election
            if self._state == self.STATE_LEADER and self.leader_rounds > 0:
                self.leader_rounds -= 1
                self._last_heartbeat = datetime.now()
                await self.broadcast_to_servers({
                    "term": self._currentTerm,
                    "type": "heartbeat",
                    "identifier": self.identifier
                })

            #Force reelection after 20 heartbeats
            if self.leader_rounds == 0:
                self._state = self.STATE_CANDIDATE
                self.leader_rounds = 20
                election_timeout = random.randint(100, 200) / 1000
                # election_timeout = random.randint(1, 3)
                await asyncio.sleep(election_timeout)

            heartbeat_timed_out = (datetime.now() - self._last_heartbeat).total_seconds() > self.HEARTBEAT_INTERVAL * 1.5
            if heartbeat_timed_out and self._state != self.STATE_LEADER:
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
        self._ws_server = websockets.serve(lambda ws,msg: self.server_on_message(ws, msg), self.host, self.port)
        asyncio.ensure_future(self._ws_server, loop=asyncio.get_event_loop())

    async def client_recv_handler(self, connection):
        """
        Handler to communicate with the other servers
        """
        while True:
            data = await connection.recv()
            message = json.loads(data)
            logger.info('Received message at server from {}:{} > {}'.format(*connection.remote_address, message))
            if message['type'] == 'vote':
                self.votes["{}:{}".format(*connection.remote_address)] = message['identifier']

                my_votes = sum( x == self.identifier for x in self.votes.values() )
                if my_votes >= len(self.votes.keys()) / 2.0:
                    logger.info("I am the new leader! \o/")
                    self.votes = {}
                    self._state = self.STATE_LEADER
                    await self.broadcast_to_servers({
                        "term": self._currentTerm,
                        "type": "heartbeat",
                        "identifier": self.identifier
                    })

            elif message['type'] == 'heartbeat':
                self._leader = message['identifier']
                self._last_heartbeat = datetime.now()
                print('new leader for server', self.identifier)

            elif message['type'] == 'commit':
                logger.info("Committing message: {}".format(message["data"]))

            #logger.debug("Websocket client message recieved: {}".format(message))

    async def connect_to_server(self, uri):
        logger.info("Trying to connect to {}".format(uri))
        if not "{}:{}".format(self.host, self.port) in uri:
            # Do not connect to yourself
            try:
                ws = websockets.connect(uri)
                connection = await ws.__aenter__()
                self._connections_to_servers["{}:{}".format(*connection.remote_address)] = connection
                asyncio.ensure_future(self.client_recv_handler(connection), loop=asyncio.get_event_loop())
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
            await asyncio.sleep(5)

    async def send_heartbeat(self):
        while True:
            await self.broadcast_to_servers({
                "term": self._currentTerm,
                "type": "heartbeat",
                "identifier": self.identifier
            })
            asyncio.sleep(self.HEARTBEAT_INTERVAL)

    async def server_on_message(self, websocket, path):
        # register(websocket) sends user_event() to websocket
        # Register.
        self.connected_clients.add(websocket)
        try:
            # Implement logic here.
            logger.info("New connection from: {}:{}".format(*websocket.remote_address))
            if self._leader == self.identifier:
                await websocket.send(json.dumps({
                    "term": self._currentTerm,
                    "type": "heartbeat",
                    "identifier": self.identifier
                }))
            while True:
                try:
                    data = await websocket.recv()
                    message = json.loads(data)
                    await self.handle_message_exchange(message)
                    logger.info('Received message at server from {}:{} > {}'.format(*websocket.remote_address, message))
                    if message['type'] == 'vote' and message['term'] >= self._currentTerm:
                        self._state = self.STATE_FOLLOWER
                        self.leader = message['identifier']
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

                    elif message['type'] == 'add_node':
                        #add node and send to all others
                        pass

                    elif message['type'] == 'commit':
                        if self._state != self.STATE_LEADER:
                            pass
                            # send to leader for distribution
                        else:
                            await self.broadcast_to_clients({
                                "type": "commit",
                                "data": message["data"]
                            })

                    # print("My state", self._state)
                    #self.broadcast(message)
                except:
                    pass
                    # TODO: Error handling
        finally:
            # Unregister.
            self.connected_clients.remove(websocket)

    async def broadcast_to_clients(self, message):
        """
        Broadcast a message to all connected clients.
        """
        message['term'] = self._currentTerm
        if len(self.connected_clients):
            await asyncio.wait([ws.send(json.dumps(message)) for ws in self.connected_clients])

    async def broadcast_to_servers(self, message):
        """
        Broadcast a message to all connected clients.
        """
        message['term'] = self._currentTerm
        if len(self._connections_to_servers.keys()):
            await asyncio.wait([self._connections_to_servers[k].send(json.dumps(message)) for k in self._connections_to_servers.keys()])

    async def handle_message_exchange(self, message):
        pass


async def main():
    ip, port = args.listen.split(":")
    async with RaftServer(ip, int(port)) as echo:
        pass

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_forever()
