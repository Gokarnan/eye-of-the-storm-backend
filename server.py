#!/usr/bin/env python3
"""Tiny room-based WebSocket relay for Eye of the Storm multiplayer.
Run with: python server.py
Requires: pip install websockets
"""
import asyncio
import json
import secrets
import os

import websockets

PORT = int(os.environ.get("PORT", "8765"))
rooms = {}       # room -> {player_id: player_state}
clients = {}     # websocket -> {id, room}
room_hosts = {}  # room -> player id
room_passwords = {}  # room -> password (kept in memory only)


def public_players(room):
    return list(rooms.get(room, {}).values())

def room_directory():
    return [{"name": name, "count": len(players), "locked": bool(room_passwords.get(name))}
            for name, players in sorted(rooms.items(), key=lambda item: item[0].lower())]

async def send_room_directory(ws):
    await send_json(ws, {"type": "rooms", "rooms": room_directory()})


def storm_for(room):
    host_id = room_hosts.get(room)
    host = rooms.get(room, {}).get(host_id, {}) if host_id else {}
    return host.get("storm", {"x": 0, "z": 0})

async def send_json(ws, payload):
    try:
        await ws.send(json.dumps(payload, separators=(",", ":")))
    except (websockets.ConnectionClosed, RuntimeError):
        pass

async def broadcast_room(room):
    if room not in rooms:
        return
    payload = {
        "type": "snapshot",
        "room": room,
        "hostId": room_hosts.get(room),
        "players": public_players(room),
        "storm": storm_for(room),
    }
    data = json.dumps(payload, separators=(",", ":"))
    for ws, info in list(clients.items()):
        if info.get("room") == room:
            try:
                await ws.send(data)
            except websockets.ConnectionClosed:
                pass

async def handler(ws):
    joined = False
    player_id = secrets.token_hex(4)
    try:
        async for raw in ws:
            if len(raw) > 16000:
                continue
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            typ = msg.get("type")
            if typ == "list_rooms":
                await send_room_directory(ws)
            elif typ in ("join", "create_room") and not joined:
                room = str(msg.get("room", ""))[:28].strip()
                name = str(msg.get("name", "Chaser"))[:18].strip() or "Chaser"
                color = str(msg.get("color", "#ffb53b"))[:16]
                password = str(msg.get("password", ""))[:64]
                if not room:
                    await send_json(ws, {"type": "room_error", "message": "Enter a room name."})
                    continue
                if typ == "create_room":
                    if room in rooms:
                        await send_json(ws, {"type": "room_error", "message": "That room already exists. Join it or choose another name."})
                        continue
                    rooms[room] = {}
                    room_passwords[room] = password
                else:
                    if room not in rooms:
                        await send_json(ws, {"type": "room_error", "message": "Room not found. Refresh the list or create it first."})
                        continue
                    if room_passwords.get(room, "") != password:
                        await send_json(ws, {"type": "room_error", "message": "Wrong password for this room."})
                        continue
                player = {"id": player_id, "name": name, "color": color,
                          "x": 0, "z": 0, "h": 0, "y": 0,
                          "storm": {"x": 0, "z": 0}}
                rooms[room][player_id] = player
                clients[ws] = {"id": player_id, "room": room}
                if room not in room_hosts:
                    room_hosts[room] = player_id
                joined = True
                await send_json(ws, {"type": "welcome", "id": player_id,
                                     "hostId": room_hosts[room], "players": public_players(room)})
                await broadcast_room(room)
                # Let any room-browser screens update after create/join.
                for other_ws, info in list(clients.items()):
                    if not info.get("room"):
                        await send_room_directory(other_ws)
            elif typ == "state" and joined:
                info = clients.get(ws)
                if not info:
                    continue
                room = info["room"]
                player = rooms.get(room, {}).get(player_id)
                if not player:
                    continue
                for key in ("x", "z", "h", "y", "score"):
                    try:
                        player[key] = float(msg.get(key, player.get(key, 0)))
                    except (TypeError, ValueError):
                        pass
                player["name"] = str(msg.get("name", player["name"]))[:18]
                player["color"] = str(msg.get("color", player["color"]))[:16]
                if player_id == room_hosts.get(room):
                    storm = msg.get("storm", {})
                    try:
                        player["storm"] = {"x": float(storm.get("x", 0)), "z": float(storm.get("z", 0))}
                    except (TypeError, ValueError, AttributeError):
                        pass
                await broadcast_room(room)
    except websockets.ConnectionClosed:
        pass
    finally:
        info = clients.pop(ws, None)
        if info:
            room = info["room"]
            members = rooms.get(room, {})
            members.pop(player_id, None)
            if not members:
                rooms.pop(room, None)
                room_hosts.pop(room, None)
                room_passwords.pop(room, None)
            elif room_hosts.get(room) == player_id:
                room_hosts[room] = next(iter(members))
            await broadcast_room(room)
            for other_ws, other_info in list(clients.items()):
                if not other_info.get("room"):
                    await send_room_directory(other_ws)

async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT, max_size=16000, ping_interval=20):
        print(f"Multiplayer relay listening on port {PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
