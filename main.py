from fastapi import FastAPI
from app.api.routes import router
import uvicorn
app = FastAPI(title="Lineage POC")
app.include_router(router, prefix="/lineage", tags=["Lineage"])



if __name__ == "__main__":
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    except KeyboardInterrupt:
        print("Server stopped by user.")