from fastapi import FastAPI
from routers import curated, health, raw, staging, stats, dashboard


app = FastAPI(
    title="Football Data Lake API",
    description="API Gateway for the Premier League football data lake.",
    version="1.0.0",
)

app.include_router(raw.router, prefix="/raw", tags=["Raw Zone"])
app.include_router(staging.router, prefix="/staging", tags=["Staging Zone"])
app.include_router(curated.router, prefix="/curated", tags=["Curated Zone"])
app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(stats.router, prefix="/stats", tags=["Stats"])
app.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])  

@app.get("/")
def root():
    return {
        "service": "Football Data Lake API",
        "docs": "http://localhost:8000/docs",
        "zones": ["/raw", "/staging", "/curated", "/health", "/stats"],
    }
