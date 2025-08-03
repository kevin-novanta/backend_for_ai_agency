from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import leads, stats

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

# Optional root route
@app.get("/")
def read_root():
    return {"message": "AI Workflow Agency API is running."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
