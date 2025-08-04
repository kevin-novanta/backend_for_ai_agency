from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import leads, stats
from api.Google_Sheets.Lead_Registry_Sync import sync_routes

app = FastAPI(
    title="AI Workflow Agency API",
    description="Backend API for lead scraping, outreach, and workflow insights.",
    version="1.0.0"
)

# Allow all CORS origins (you can restrict this to your frontend domain later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(leads.router, prefix="/leads", tags=["Leads"])
app.include_router(stats.router, prefix="/stats", tags=["Stats"])
app.include_router(sync_routes.router, prefix="/sync", tags=["Sync"])

# Optional root route
@app.get("/")
def read_root():
    return {"message": "AI Workflow Agency API is running."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
