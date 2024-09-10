import time
from services.spotify_service import search_spotify, get_track_audio_features, get_artist_info, get_available_genres
from utils.helpers import SpotifyDatabaseManager  # Importa a classe do helpers

# Inicializar o gerenciador do banco de dados
db_manager = SpotifyDatabaseManager()

# Função para Dividir a Lista de Gênero
def chunk_list(lst, chunk_size):
    """Divide a lista em pedaços menores."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

# Função para buscar várias músicas e salvar no banco de dados
def collect_music_data(genres, limit=10, chunk_size=5):
    # Dividir a lista de gêneros em blocos menores
    for genre_chunk in chunk_list(genres, chunk_size):
        # Unir os gêneros separados por vírgula para a busca
        query = 'genre:' + ','.join(genre_chunk)
        print(f"Buscando músicas com os gêneros: {', '.join(genre_chunk)}")

        # Buscar resultados na API do Spotify
        search_results = search_spotify(query, search_type='track', limit=limit)

        if search_results:
            for track in search_results['tracks']['items']:
                track_id = track['id']
                track_name = track['name']
                artist = track['artists'][0]

                # Buscar as características da música
                track_audio_features = get_track_audio_features(track_id)

                # Combinar os dados da música com suas características
                track_data = {**track, **track_audio_features}

                # Buscar dados do artista
                artist_data = get_artist_info(artist['id'])

                # Salvar os dados no banco de dados usando o SpotifyDatabaseManager
                db_manager.save_track_data(track_data, artist_data)

                print(f"Dados da música '{track_name}' e do artista '{artist['name']}' salvos com sucesso.")

# Função principal para rodar o script continuamente
def run_in_real_time_all_genres(interval=60, limit=10, chunk_size=5):
    """
    Executa o processo de coleta de dados em tempo real com base em todos os gêneros disponíveis.
    :param interval: Intervalo de tempo entre as buscas (em segundos)
    :param limit: Número de músicas a coletar por busca
    :param chunk_size: Quantidade de gêneros a serem buscados por vez
    """
    # Buscar todos os gêneros disponíveis na API do Spotify
    genres = get_available_genres()

    if not genres:
        print("Nenhum gênero encontrado.")
        return

    while True:
        print(f"Iniciando coleta de músicas de todos os gêneros...")
        collect_music_data(genres, limit=limit, chunk_size=chunk_size)
        print(f"Aguardando {interval} segundos para a próxima coleta...")
        time.sleep(interval)  # Esperar antes de realizar nova coleta

# Exemplo de execução do script
if __name__ == "__main__":
    run_in_real_time_all_genres(interval=600, limit=10, chunk_size=5)  # Buscar músicas de todos os gêneros em blocos de 5


