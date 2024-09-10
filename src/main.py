import time
from services.spotify_service import search_spotify, get_track_audio_features, get_artist_info, get_available_genres
from utils.helpers import SpotifyDatabaseManager  # Importa a classe do helpers

# Inicializar o gerenciador do banco de dados
db_manager = SpotifyDatabaseManager()

# Função para dividir a lista de gêneros em blocos menores
def chunk_list(lst, chunk_size):
    """Divide a lista em pedaços menores."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

# Função para buscar várias músicas e salvar no arquivo Parquet
def collect_music_data(genres, limit=10, chunk_size=5, parquet_file='spotify_data.parquet'):
    db_manager = SpotifyDatabaseManager(parquet_file)

    for genre_chunk in chunk_list(genres, chunk_size):
        query = 'genre:' + ','.join(genre_chunk)
        print(f"Buscando músicas com os gêneros: {', '.join(genre_chunk)}")

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

                # Salvar os dados no arquivo Parquet
                db_manager.save_track_data_parquet(track_data, artist_data)

                print(f"Dados da música '{track_name}' e do artista '{artist['name']}' salvos com sucesso.")

# Função principal para rodar o script continuamente
def run_in_real_time_all_genres(interval=60, limit=10, chunk_size=5, parquet_file='spotify_data.parquet'):
    genres = get_available_genres()

    if not genres:
        print("Nenhum gênero encontrado.")
        return

    while True:
        print(f"Iniciando coleta de músicas de todos os gêneros...")
        collect_music_data(genres, limit=limit, chunk_size=chunk_size, parquet_file=parquet_file)
        print(f"Aguardando {interval} segundos para a próxima coleta...")
        time.sleep(interval)  # Esperar antes de realizar nova coleta

# Exemplo de execução do script
if __name__ == "__main__":
    run_in_real_time_all_genres(interval=600, limit=10, chunk_size=5, parquet_file='spotify_data.parquet')  # Buscar músicas de todos os gêneros em blocos de 5
