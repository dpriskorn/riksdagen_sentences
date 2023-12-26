from models.api import app

if __name__ == "__main__":
    # Your code here, if needed
    # For example, running the app with uvicorn
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=80)
