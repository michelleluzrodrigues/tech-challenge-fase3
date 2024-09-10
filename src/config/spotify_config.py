import os
import base64
import requests
from dotenv import load_dotenv

load_dotenv()  # Carregar as variáveis de ambiente do arquivo .env

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

def get_access_token():
    auth_url = "https://accounts.spotify.com/api/token"
    auth_header = base64.b64encode(f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()).decode()
    
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    data = {
        "grant_type": "client_credentials"
    }
    
    response = requests.post(auth_url, headers=headers, data=data)
    
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception("Erro ao obter token de acesso")

