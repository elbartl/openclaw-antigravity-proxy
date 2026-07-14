import asyncio
import json
import time
import uuid
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
from google.antigravity import Agent, LocalAgentConfig, CapabilitiesConfig

app = FastAPI(title="Antigravity OpenAI Proxy for OpenClaw")

# Konfiguracja Agenta Antigravity
# Włączamy uprawnienia (capabilities), aby agent miał pełne możliwości
config = LocalAgentConfig(
    system_instructions="Jesteś zaawansowanym asystentem AI podłączonym do OpenClaw. Odpowiadaj zwięźle i precyzyjnie.",
    capabilities=CapabilitiesConfig()
)

@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    data = await request.json()
    messages = data.get("messages", [])
    stream = data.get("stream", False)
    
    if not messages:
        return JSONResponse({"error": "Brak wiadomości w zapytaniu"}, status_code=400)

    # OpenClaw przesyła całą historię w formacie tablicy messages.
    # Ponieważ dla każdego zapytania uruchamiamy nową instancję agenta, 
    # budujemy pełen kontekst konwersacji w jednym prompcie.
    formatted_prompt = "Kontekst poprzednich wiadomości:\n"
    for msg in messages[:-1]: # wszystkie poza ostatnią
        role = msg.get("role", "user")
        content = msg.get("content", "")
        formatted_prompt += f"[{role.upper()}]: {content}\n"
    
    last_user_msg = messages[-1].get("content", "")
    formatted_prompt += f"\nAktualne zapytanie użytkownika:\n{last_user_msg}"

    if stream:
        async def event_generator():
            request_id = f"chatcmpl-{uuid.uuid4()}"
            created_time = int(time.time())
            model_name = data.get("model", "antigravity")
            
            # Tworzymy jednorazowego agenta do obsługi tego strumienia
            async with Agent(config) as agent:
                response = await agent.chat(formatted_prompt)
                
                async for token in response:
                    chunk = {
                        "id": request_id,
                        "object": "chat.completion.chunk",
                        "created": created_time,
                        "model": model_name,
                        "choices": [{
                            "delta": {"content": token},
                            "index": 0,
                            "finish_reason": None
                        }]
                    }
                    yield f"data: {json.dumps(chunk)}\n\n"
                
                # Zakończenie strumienia (zgodnie ze standardem OpenAI)
                yield "data: [DONE]\n\n"

        return StreamingResponse(event_generator(), media_type="text/event-stream")
    else:
        # Odpowiedź bez strumieniowania (synchroniczna z punktu widzenia klienta HTTP)
        async with Agent(config) as agent:
            response = await agent.chat(formatted_prompt)
            full_content = ""
            async for token in response:
                full_content += token
                
            return JSONResponse({
                "id": f"chatcmpl-{uuid.uuid4()}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": data.get("model", "antigravity"),
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": full_content
                    },
                    "finish_reason": "stop",
                    "index": 0
                }]
            })

if __name__ == "__main__":
    print("Uruchamianie serwera Antigravity Proxy dla OpenClaw na porcie 8000...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
