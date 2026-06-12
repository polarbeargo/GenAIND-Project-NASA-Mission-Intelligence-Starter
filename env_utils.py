from pathlib import Path
import os

from dotenv import load_dotenv


def load_project_env(anchor_file: str | Path) -> bool:
    """Load the nearest project .env by walking upward from a file path."""
    anchor_path = Path(anchor_file).resolve()
    search_dir = anchor_path.parent if anchor_path.is_file() else anchor_path
    override = "KUBERNETES_SERVICE_HOST" not in os.environ

    for directory in (search_dir, *search_dir.parents):
        env_path = directory / ".env"
        if env_path.exists():
            return load_dotenv(dotenv_path=env_path, override=override)

    return False