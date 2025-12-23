# main.py
from fastapi import FastAPI
from core.lifecycle import lifespan  # Adjust path as needed
from api import router as api_router

app = FastAPI(
    title="Workflow Orchestration Engine", lifespan=lifespan  # This is the key link!
)

app.include_router(api_router)
