import requests
from config.spotify_config import get_access_token

# Função para registrar erros de maneira mais consistente
def log_error(response, action):
    print(f"Erro ao {action}: {response.status_code} - {response.text}")
    raise Exception(f"Erro ao {action}: {response.status_code}")

# Função para buscar características de áudio da música
def get_track_audio_features(track_id):
    token = get_access_token()
    url = f"https://api.spotify.com/v1/audio-features/{track_id}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        log_error(response, 'buscar características de áudio da música')

# Função para buscar informações completas de uma música (metadados)
def get_track_info(track_id):
    token = get_access_token()
    url = f"https://api.spotify.com/v1/tracks/{track_id}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        log_error(response, 'buscar dados da música')

# Função para buscar artistas
def get_artist_info(artist_id):
    token = get_access_token()
    url = f"https://api.spotify.com/v1/artists/{artist_id}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        log_error(response, 'buscar dados do artista')

# Função para buscar músicas ou artistas com base em uma palavra-chave
def search_spotify(query, search_type='track', limit=10):
    """
    Função para buscar músicas ou artistas no Spotify.
    :param query: Palavra-chave para busca
    :param search_type: Tipo de busca ('track', 'artist', etc.)
    :param limit: Número de resultados retornados (máx 50)
    :return: Resultados da busca no formato JSON
    """
    token = get_access_token()
    url = f"https://api.spotify.com/v1/search?q={query}&type={search_type}&limit={limit}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        log_error(response, f'buscar {search_type} com a query "{query}"')

def get_available_genres():
    """
    Busca todos os gêneros disponíveis na API do Spotify.
    :return: Lista de gêneros disponíveis
    """
    token = get_access_token()
    url = "https://api.spotify.com/v1/recommendations/available-genre-seeds"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('genres', [])
    else:
        raise Exception(f"Erro ao buscar gêneros: {response.status_code}, {response.text}")