import os
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

if not TMDB_API_KEY:
    raise ValueError("TMDB_API_KEY is not set")

TMDB_BASE_URL = "https://api.themoviedb.org/3"

PROXY_URL = "http://192.168.56.1:4066"

PROXIES = {
    "http": PROXY_URL,
    "https": PROXY_URL
}

RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"