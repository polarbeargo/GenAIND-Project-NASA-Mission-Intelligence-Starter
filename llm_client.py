from typing import Dict, List
from openai import OpenAI

def generate_response(openai_key: str, user_message: str, context: str, 
                     conversation_history: List[Dict], model: str = "gpt-3.5-turbo") -> str:
    """Generate response using OpenAI with context"""
    if not openai_key:
        raise ValueError("OpenAI API key is required")

    system_prompt = (
        "You are a NASA mission intelligence assistant. Answer questions using the provided "
        "retrieval context when available. Be accurate, concise, and explicit about uncertainty. "
        "If the context is insufficient, say what is missing instead of inventing details."
    )

    messages = [{"role": "system", "content": system_prompt}]

    context_text = (context or "").strip()
    if context_text:
        messages.append(
            {
                "role": "system",
                "content": (
                    "Retrieved context for this turn:\n"
                    f"{context_text}"
                ),
            }
        )

    for history_item in conversation_history or []:
        role = history_item.get("role")
        content = history_item.get("content")
        if role in {"user", "assistant", "system"} and isinstance(content, str) and content.strip():
            messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": user_message})

    client = OpenAI(api_key=openai_key)
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.2,
        max_tokens=700,
    )

    if not response.choices:
        return "I could not generate a response."

    content = response.choices[0].message.content
    return content.strip() if content else "I could not generate a response."