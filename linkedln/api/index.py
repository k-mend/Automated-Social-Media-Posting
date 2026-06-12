import os
from fastapi import FastAPI, HTTPException, Header
from dotenv import load_dotenv
# Import your agent class from your existing script
from linkedln import ViralLinkedInAgent 

load_dotenv()
app = FastAPI()

CRON_SECRET = os.getenv("CRON_SECRET")

@app.get("/api/cron-post")
def trigger_linkedin_bot(authorization: str = Header(None)):
    """This endpoint gets triggered by Vercel's Cron engine"""
    
    # Security check to ensure random people online can't trigger your bot
    if authorization != f"Bearer {CRON_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    try:
        agent = ViralLinkedInAgent()
        # Ensure your methods run sequentially once and terminate
        agent.create_posting_queue()
        agent.run_posting_queue()
        return {"status": "success", "message": "Content curated and posted successfully!"}
    except Exception as e:
        return {"status": "error", "error": str(e)}
