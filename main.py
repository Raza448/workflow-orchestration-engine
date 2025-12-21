from fastapi import FastAPI
from api import router as api_router
from core import setup_logging

setup_logging()

app = FastAPI(title="Workflow Orchestration Engine", version="1.0.0")
app.include_router(api_router)
