"""
Axiom — Pitcher Intelligence Platform
Entry point for the FastAPI application.

Starts uvicorn on the PORT specified in environment variables (default 8080).
Google Cloud Run sets PORT automatically.
"""
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.config import settings
from app.utils.logging import configure_logging

configure_logging(settings.LOG_LEVEL)

app = FastAPI(
    title="Axiom — Pitcher Intelligence Platform",
    description=(
        "Daily MLB pitcher scoring engine by GTM Velo. "
        "Computes HUSI (Hits Under Score Index) and KUSI (Strikeouts Under Score Index) "
        "for all probable starters each day."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=(settings.APP_ENV == "development"),
    )
