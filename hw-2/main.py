import json
import logging
from logging import config
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn
from starlette.responses import RedirectResponse

from server import Server
from models import RaftRequest, RaftResponse, Operation


server: Server


@asynccontextmanager
async def lifespan(app):
    global server
    if len(sys.argv) < 2:
        raise RuntimeError("Not enough arguments\nUsage: python main.py <server_id>")
    logging.info(f"Starting server with ID: {sys.argv[2]}")
    server = Server(int(sys.argv[2]))
    yield
    print("Shutdown")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    logging.info(f"App: Root access")
    return "I am alive!"

@app.get("/view")
async def root():
    logging.info(f"App: View access")
    return json.loads(json.dumps(server, indent=4))

@app.get("/storage")
async def get_value(key: str):
    logging.info(f"App: Got GET request for key: {key}")
    if not server.isLeader():
        return RedirectResponse(f"http://localhost:3333{server.leader_id}/storage")
    result: int = server.serve_client(RaftRequest(key=key), Operation.GET)
    return RaftResponse(value=result)

@app.post("/storage")
async def add_value(request: RaftRequest):
    logging.info(f"App: Got request: {request}")
    if not server.isLeader():
        return RedirectResponse(f"http://localhost:3333{server.leader_id}/storage")
    _: int = server.serve_client(request, Operation.POST)
    return RaftResponse(value="OK")

@app.put("/storage")
async def set_value(request: RaftRequest):
    logging.info(f"App: Got request: {request}")
    if not server.isLeader():
        return RedirectResponse(f"http://localhost:3333{server.leader_id}/storage")
    _: int = server.serve_client(request, Operation.PUT)
    return RaftResponse(value="OK")

@app.delete("/storage")
async def delete_value(request: RaftRequest):
    logging.info(f"App: Got request: {request}")
    if not server.isLeader():
        return RedirectResponse(f"http://localhost:3333{server.leader_id}/storage")
    _: int = server.serve_client(request, Operation.DELETE)
    return RaftResponse(value="OK")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise RuntimeError("Not enough arguments\nUsage: python main.py <port> <server_id>")
    os.environ["PATH_TO_LOG_FILE"] = f"server_{sys.argv[2]}.log"
    logging.config.fileConfig("logging.conf")
    logging.info(f"Starting FastAPI server at: {sys.argv[1]}")
    uvicorn.run(app, host="0.0.0.0", port=int(sys.argv[1]), log_config="logging.conf", log_level="info")
