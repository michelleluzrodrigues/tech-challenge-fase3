import requests
import time
import random
from services.spotify_service import search_spotify, get_track_audio_features, get_artist_info, get_available_genres
from utils.helpers import SpotifyDatabaseManager  # Importa a classe do helpers

# Inicializar o gerenciador do banco de dados
db_manager = SpotifyDatabaseManager()

# Função para buscar músicas de um gênero específico e salvar no arquivo Parquet
def collect_music_data(genre, limit=10, offset=0, parquet_file='spotify_data.parquet'):
    db_manager = SpotifyDatabaseManager(parquet_file)
    
    query = f'genre:{genre}'
    print(f"Buscando músicas com o gênero: {genre} (Offset: {offset})")

    # Passar o offset para buscar músicas diferentes
    search_results = search_spotify(query, search_type='track', limit=limit, offset=offset)

    if not search_results or 'tracks' not in search_results or not search_results['tracks']['items']:
        print(f"Nenhuma música encontrada para o gênero: {genre}")
        return False

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

    return True

# Função principal para rodar o script continuamente com gêneros embaralhados
def run_in_real_time_all_genres(interval=600, limit=10, parquet_file='spotify_data.parquet'):
    genres = get_available_genres()

    if not genres:
        print("Nenhum gênero encontrado.")
        return

    offset = 0  # Inicializa o offset

    while True:
        # Embaralhar a lista de gêneros antes de cada coleta de dados
        random.shuffle(genres)
        print(f"Iniciando coleta de músicas de gêneros embaralhados...")

        for genre in genres:
            success = collect_music_data(genre, limit=limit, offset=offset, parquet_file=parquet_file)
            if success:
                offset += limit  # Aumenta o offset se músicas forem encontradas
            else:
                print(f"Falha ao buscar músicas para o gênero: {genre}, tentando o próximo...")

        print(f"Aguardando {interval} segundos para a próxima coleta...")        
        time.sleep(interval)  # Esperar antes de realizar nova coleta

# Exemplo de execução do script
if __name__ == "__main__":
    run_in_real_time_all_genres(interval=600, limit=10, parquet_file='spotify_data.parquet')  # Buscar músicas de um gênero por vez
