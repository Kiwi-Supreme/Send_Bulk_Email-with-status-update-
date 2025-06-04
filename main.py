
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open("email_list.json", "r") as f:
    email_list = json.load(f)["emails"]

@app.get("/", response_class=HTMLResponse)
async def form(request: Request):
    return templates.TemplateResponse("form.html", {"request": request})

@app.post("/send", response_class=HTMLResponse)
async def send_bulk_email(request: Request, subject: str = Form(...), message: str = Form(...)):
    for email in email_list:
        payload = {"email": email, "subject": subject, "message": message}
        producer.send("bulk_email_topic", value=payload)
    producer.flush()
    return templates.TemplateResponse("success.html", {"request": request})
