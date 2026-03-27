import os


DEFAULT_OPENAI_BASE_URL = "https://openai.vocareum.com/v1"
DEFAULT_OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_OPENAI_CHAT_MODEL = "gpt-3.5-turbo"
DEFAULT_OPENAI_CHAT_MODEL_OPTIONS = (
    "gpt-3.5-turbo",
    "gpt-4",
    "gpt-4-turbo-preview",
)


def get_openai_api_key(include_chroma_fallback: bool = True) -> str | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        return api_key
    if include_chroma_fallback:
        return os.getenv("CHROMA_OPENAI_API_KEY")
    return None


def get_openai_base_url() -> str:
    return os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)


def get_openai_embedding_model() -> str:
    return os.getenv("OPENAI_EMBEDDING_MODEL", DEFAULT_OPENAI_EMBEDDING_MODEL)


def get_openai_chat_model() -> str:
    return os.getenv("OPENAI_CHAT_MODEL", DEFAULT_OPENAI_CHAT_MODEL)


def get_openai_chat_model_options() -> list[str]:
    configured_model = get_openai_chat_model()
    options = list(DEFAULT_OPENAI_CHAT_MODEL_OPTIONS)
    if configured_model not in options:
        options.insert(0, configured_model)
    return options


def set_chroma_openai_api_key(api_key: str) -> None:
    os.environ["CHROMA_OPENAI_API_KEY"] = api_key