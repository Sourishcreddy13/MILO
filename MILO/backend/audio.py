# audio.py (DL-based STT + TTS)

import asyncio
import base64
import tempfile
from fastapi import APIRouter, UploadFile, File, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState
import torch

# Deep Learning models
import whisper
from TTS.api import TTS

router = APIRouter()

# ============================
# 1️⃣ Speech-to-Text (Whisper)
# ============================

# Load Whisper model once at startup
print("Loading Whisper model (small)...")
whisper_model = whisper.load_model("small")  # use "base" for speed

@router.post("/transcribe")
async def transcribe_audio(file: UploadFile = File(...)):
    """
    Transcribes uploaded audio using Whisper deep learning model.
    Returns plain text transcription.
    """
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        result = whisper_model.transcribe(tmp_path, fp16=False)
        transcript = result["text"].strip()
        print(f"[Whisper] Transcript: {transcript}")
        return {"transcription": transcript}
    except Exception as e:
        print(f"Error in STT: {e}")
        return {"error": str(e)}


# ============================
# 2️⃣ Text-to-Speech (Tacotron2)
# ============================

# Load Tacotron2 model once
print("Loading Tacotron2 TTS model...")
tts_model = TTS(model_name="tts_models/en/ljspeech/tacotron2-DDC", progress_bar=False, gpu=torch.cuda.is_available())

async def text_to_speech_async(text: str):
    """
    Converts text to speech using Tacotron2 + WaveGlow.
    Returns Base64-encoded WAV audio.
    """
    if not text:
        return None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
            tts_model.tts_to_file(text=text, file_path=tmp.name)

            # Read and base64-encode audio
            with open(tmp.name, "rb") as f:
                audio_bytes = f.read()
            audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")

        print("[TTS] Audio generated successfully.")
        return audio_b64

    except Exception as e:
        print(f"Error during TTS: {e}")
        return None


# ============================
# 3️⃣ (Optional) WebSocket Streaming STT
# ============================

@router.websocket("/listen")
async def websocket_stt_endpoint(websocket: WebSocket):
    """
    (Simplified) WebSocket endpoint for real-time streaming transcription.
    It buffers incoming audio chunks, saves periodically, and runs Whisper.
    """
    await websocket.accept()
    print("WebSocket connected for STT.")

    audio_chunks = bytearray()

    try:
        while True:
            data = await websocket.receive_bytes()
            audio_chunks.extend(data)

            # Simple chunk-based trigger (process after ~1MB)
            if len(audio_chunks) > 1_000_000:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
                    tmp.write(audio_chunks)
                    tmp_path = tmp.name
                result = whisper_model.transcribe(tmp_path, fp16=False)
                transcript = result["text"].strip()
                await websocket.send_text(transcript)
                audio_chunks.clear()

    except WebSocketDisconnect:
        print("WebSocket client disconnected.")
    except Exception as e:
        print(f"Error in WebSocket STT: {e}")
    finally:
        if websocket.client_state != WebSocketState.DISCONNECTED:
            await websocket.close()
        print("WebSocket closed.")
