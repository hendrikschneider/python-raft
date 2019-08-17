import asyncio
import websockets
import json


async def client():
    async with websockets.connect('ws://127.0.0.1:8007') as websocket:
        data = {
            "type": "commit",
            "data": json.dumps({
                "message": "foo",
                "user": "u1",
                "session": "s1"
            })
        }
        await websocket.send(json.dumps({
            "type": "register",
            "data": "control"
        }))
        await websocket.send(json.dumps(data))
        await websocket.close()
        # await asyncio.sleep(3000)

print("Sending message over control channel to raft network")
asyncio.get_event_loop().run_until_complete(client())
