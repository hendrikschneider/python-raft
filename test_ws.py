import asyncio
import websockets
import json

async def client():
    async with websockets.connect('ws://127.0.0.1:8008') as websocket:
        data = {
            "type": "commit",
            "data": json.dumps({
                "message": "foo",
                "user": "u1",
                "session": "s1"
            })
        }
        await websocket.send(json.dumps(data))


asyncio.get_event_loop().run_until_complete(client())
