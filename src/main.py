import random
import time
from services.spotify_service import search_spotify, get_track_audio_features, get_artist_info, get_available_genres
from utils.helpers import SpotifyDatabaseManager

# Inicializar o gerenciador do banco de dados
db_manager = SpotifyDatabaseManager()

# Lista de gêneros inválidos para ignorar
invalid_genres = []

# Função para monitorar o limite de requisições (rate limit) da API
def monitor_rate_limit(response):
    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))  # Pega o número de requisições restantes
    reset_time = int(response.headers.get("X-RateLimit-Reset", 60))  # Tempo até o reset, em segundos
    if remaining < 5:  # Se estiver próximo do limite
        wait_time = max(reset_time, 60)  # Espera até o reset (mínimo de 60 segundos)
        print(f"Rate limit próximo. Aguardando {wait_time} segundos...")
        time.sleep(wait_time)

# Função para buscar músicas de um gênero específico e salvar no arquivo Parquet
def collect_music_data(genre, limit=10, offset=0, parquet_file='spotify_data.parquet'):
    if genre in invalid_genres:
        print(f"Ignorando gênero inválido: {genre}")
        return False

    db_manager = SpotifyDatabaseManager(parquet_file)
    
    query = f'genre:{genre}'
    print(f"Buscando músicas com o gênero: {genre} (Offset: {offset})")

    try:
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

    except Exception as e:
        print(f"Erro ao buscar músicas para o gênero '{genre}': {str(e)}")
        invalid_genres.append(genre)  # Marcar o gênero como inválido para não tentar novamente
        return False

# Função principal para rodar o script continuamente com gêneros embaralhados
def run_in_real_time_all_genres(interval=1800, limit=5, parquet_file='spotify_data.parquet'):
    genres = get_available_genres()

    if not genres:
        print("Nenhum gênero encontrado.")
        return

    offset = 0  # Inicializa o offset

    while True:
        # Embaralhar a lista de gêneros antes de cada coleta de dados
        random.shuffle(genres)
        print(f"Iniciando coleta de músicas de gêneros embaralhados...")

        for genre in genres[:3]:  # Buscar apenas 3 gêneros por ciclo para evitar excessos
            success = collect_music_data(genre, limit=limit, offset=offset, parquet_file=parquet_file)
            if success:
                offset += limit  # Aumenta o offset se músicas forem encontradas
            else:
                print(f"Falha ao buscar músicas para o gênero: {genre}, tentando o próximo...")

        print(f"Aguardando {interval} segundos para a próxima coleta...")
        time.sleep(interval)  # Esperar antes de realizar nova coleta

# Exemplo de execução do script
if __name__ == "__main__":
    run_in_real_time_all_genres(interval=1800, limit=5, parquet_file='spotify_data.parquet')  # Intervalo de 30 minutos
