import json

def _default(self, obj):
    return getattr(obj.__class__, "__json__", _default.default)(obj)


from json import JSONEncoder

_default.default = JSONEncoder().default
JSONEncoder.default = _default

import logging
from logging import config
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
import uvicorn

from server import Server
from models import CRDTRequest, CRDTResponse

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
    result: Optional[int] = server.on_get(key)
    return CRDTResponse(value=result)


@app.patch("/storage")
async def add_value(request: CRDTRequest):
    logging.info(f"App: Got request: {request}")
    _: int = server.on_patch(request)
    return CRDTResponse(value="OK")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise RuntimeError("Not enough arguments\nUsage: python main.py <port> <server_id>")
    os.environ["PATH_TO_LOG_FILE"] = f"server_{sys.argv[2]}.log"
    logging.config.fileConfig("logging.conf")
    logging.info(f"Starting FastAPI server at: {sys.argv[1]}")
    uvicorn.run(app, host="0.0.0.0", port=int(sys.argv[1]), log_config="logging.conf", log_level="info")
