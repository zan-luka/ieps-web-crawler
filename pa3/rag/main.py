from urllib import request
from urllib.request import Request

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from pathlib import Path
import ollama
import json
import logging
from sentence_transformers import SentenceTransformer
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://avnadmin:AVNS_grJJWdHWiDIwlvllK50@pg-381112ff-web-crawler.g.aivencloud.com:15049/defaultdb?sslmode=require"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(bind=engine))

EMBEDDING_MODEL : str = 'sentence-transformers/LaBSE'
LANGUAGE_MODELS : dict = {'gemma': 'gemma3:4b',
                  'gams': 'hf.co/tknez/GaMS-9B-Instruct-GGUF:Q6_K'}

SELECTED_MODEL : str = 'gams'

labse_model = None


app = FastAPI()

# Add CORS middleware to allow WebSocket connections from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QuestionRequest(BaseModel):
    question: str
    use_rag: bool

class AnswerResponse(BaseModel):
    answer: str

class SetModelRequest(BaseModel):
    model: str

def ask_ollama(instruction_prompt, input_query):
    stream = ollama.chat(
        model=LANGUAGE_MODELS[SELECTED_MODEL],
        messages=[
            {'role': 'system', 'content': instruction_prompt},
            {'role': 'user', 'content': input_query},
        ],
        stream=True,
    )

    # Print the response from the chatbot in real-time
    print('Chatbot response:')
    response = ''
    for chunk in stream:
        content = chunk['message']['content']
        print(content, end='', flush=True)
        response += content

    return response


def get_embedding_model():
    """Initialize and return the embedding model (lazy loading)"""
    global labse_model
    if labse_model is None:
        logger.info("Initializing LaBSE embedding model")
        labse_model = SentenceTransformer(EMBEDDING_MODEL)
    return labse_model

def generate_embedding(text):
    """Generate embeddings for a given text"""
    model = get_embedding_model()
    embedding = model.encode(text, convert_to_numpy=True)
    return embedding


async def get_context_from_rag(question):
    """Get relevant context using RAG with LaBSE embeddings"""
    # Generate embedding for the question
    db = SessionLocal()
    question_embedding = generate_embedding(question)

    limit = 3

    vector_string = f"[{', '.join(str(float(val)) for val in question_embedding)}]"

    sql = text("""
                    WITH similarities AS (
                        SELECT ps.page_id, ps.page_segment, ps.segment_type, ps.title, 
                               p.url, 1 - (embedding <=> :vector) AS similarity
                        FROM crawldb.page_segment ps
                        JOIN crawldb.page p ON ps.page_id = p.id
                        WHERE p.page_type_code = 'HTML'
                        AND p.http_status_code = '200'
                        AND p.url LIKE '%//slo-tech.com/novice%'
                        AND p.html_content LIKE '%<li class="categories">%'
                        AND ps.segment_type = 'paragraph'
                    )
                    SELECT * FROM similarities
                    ORDER BY similarity DESC
                    LIMIT :limit
                """)
    result = db.execute(sql, {"vector": vector_string, "limit": limit}).fetchall()

    formatted_results = []
    for row in result:
        result_entry = {
            "page_id": row[0],
            "segment": row[1],
            "segment_type": row[2],
            "title": row[3],
            "url": row[4],
            "similarity": float(row[5])
        }
        formatted_results.append(result_entry)

    logger.info(f"Query returned {formatted_results} results using LaBSE embeddings")

    # Return retrieved context
    return "\n\n".join(entry["segment"] for entry in formatted_results)

@app.post("/question", response_model=AnswerResponse)
async def answer_question(request: QuestionRequest):
    logger.info(f"Received question: {request.question}, use_rag: {request.use_rag}")
    
    if request.use_rag:
        context = await get_context_from_rag(request.question)
    else:
        context = ""

    answer = ask_ollama(context, request.question)
    return AnswerResponse(answer=answer)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket connection accepted")
    
    try:
        # Receive the question from the client
        data = await websocket.receive_text()
        logger.info(f"Received data: {data}")
        
        request_data = json.loads(data)
        question = request_data.get("question", "")
        use_rag = request_data.get("use_rag", False)
        
        logger.info(f"Processing question: '{question}', use_rag: {use_rag}")
        
        # Determine context based on RAG setting
        if use_rag:
            context = await get_context_from_rag(question)
        else:
            context = ""
        
        # Send initial confirmation
        await websocket.send_text("")
        
        # Get the stream from Ollama
        try:
            stream = ollama.chat(
                model=LANGUAGE_MODELS[SELECTED_MODEL],
                messages=[
                    {'role': 'system', 'content': context},
                    {'role': 'user', 'content': question},
                ],
                stream=True,
            )
            
            # Stream the response chunks to the client
            for chunk in stream:
                content = chunk['message']['content']
                if content:
                    logger.debug(f"Sending chunk: {content}")
                    await websocket.send_text(content)
                    
        except Exception as e:
            logger.error(f"Error during Ollama chat: {str(e)}")
            await websocket.send_text(f"Error processing your request: {str(e)}")
                
    except WebSocketDisconnect:
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
        try:
            await websocket.send_text(f"Error: {str(e)}")
            await websocket.close()
        except:
            pass


@app.get("/", response_class=HTMLResponse)
async def get_form():
    html_path = Path("templates/index.html")
    return HTMLResponse(content=html_path.read_text(), status_code=200)

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

