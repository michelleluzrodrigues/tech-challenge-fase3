import requests
import time
from config.spotify_config import get_access_token

# Função para registrar erros de maneira mais consistente
def log_error(response, action):
    """
    Registra e levanta uma exceção com detalhes sobre o erro na requisição.

    :param response: Objeto de resposta da API
    :param action: Ação que estava sendo realizada quando o erro ocorreu
    """
    print(f"Erro ao {action}: {response.status_code} - {response.text}")
    raise Exception(f"Erro ao {action}: {response.status_code}")

# Função para monitorar o limite de requisições (rate limit) da API
def monitor_rate_limit(response):
    """
    Monitora o número de requisições restantes e o tempo de reset do rate limit.

    :param response: Objeto de resposta da API
    """
    remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
    reset_time = int(response.headers.get("X-RateLimit-Reset", 0))  # Tempo até o reset, em segundos
    if remaining < 5:
        print(f"Rate limit próximo. Aguardando {reset_time} segundos...")
        time.sleep(reset_time)

# Função para buscar características de áudio de uma música
def get_track_audio_features(track_id, retries=3, backoff_factor=2):
    """
    Busca as características de áudio de uma música com tratamento de rate limit.

    :param track_id: ID da música
    :param retries: Número de tentativas em caso de erro 429 (rate limit)
    :param backoff_factor: Fator de espera entre as tentativas (exponencial)
    :return: JSON com as características da música
    """
    token = get_access_token()
    url = f"https://api.spotify.com/v1/audio-features/{track_id}"
    headers = {"Authorization": f"Bearer {token}"}

    retry_time = backoff_factor
    for attempt in range(retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", retry_time))
            print(f"Rate limit atingido, aguardando {retry_after} segundos antes de tentar novamente...")
            time.sleep(retry_after)
            retry_time = min(retry_time * 2, 60)  # Limita o backoff a 60 segundos
        else:
            log_error(response, 'buscar características de áudio da música')

        monitor_rate_limit(response)

    log_error(response, 'buscar características de áudio da música após múltiplas tentativas')

# Função para buscar informações completas de uma música (metadados)
def get_track_info(track_id):
    """
    Busca metadados completos de uma música.

    :param track_id: ID da música
    :return: JSON com os metadados da música
    """
    token = get_access_token()
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        log_error(response, 'buscar dados da música')

# Função para buscar informações de um artista
def get_artist_info(artist_id):
    """
    Busca informações detalhadas de um artista.

    :param artist_id: ID do artista
    :return: JSON com os dados do artista
    """
    token = get_access_token()
    url = f"https://api.spotify.com/v1/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        log_error(response, 'buscar dados do artista')

# Função para buscar músicas ou artistas com base em uma palavra-chave
def search_spotify(query, search_type='track', limit=10, offset=0):
    """
    Busca músicas ou artistas no Spotify com base em uma palavra-chave.

    :param query: Palavra-chave para busca
    :param search_type: Tipo de busca ('track', 'artist', etc.)
    :param limit: Número de resultados retornados (máx 50)
    :param offset: Pular um número específico de resultados
    :return: JSON com os resultados da busca
    """
    token = get_access_token()
    url = f"https://api.spotify.com/v1/search?q={query}&type={search_type}&limit={limit}&offset={offset}"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        log_error(response, f'buscar {search_type} com a query "{query}"')

# Função para buscar todos os gêneros disponíveis no Spotify
def get_available_genres():
    """
    Busca a lista de todos os gêneros musicais disponíveis na API do Spotify.

    :return: Lista de gêneros disponíveis
    """
    token = get_access_token()
    url = "https://api.spotify.com/v1/recommendations/available-genre-seeds"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('genres', [])
    else:
        raise Exception(f"Erro ao buscar gêneros: {response.status_code}, {response.text}")
