# src/utils/scheduler.py
import schedule
import time
from services.spotify_service import get_track_audio_features
from utils.helpers import save_track_data

def collect_spotify_data():
    track_id = "11dFghVXANMlKmJXsNCbNl"
    try:
        # Coleta os dados da música
        track_data = get_track_audio_features(track_id)
        
        # Salva os dados no banco de dados ou S3
        save_track_data(track_id, track_data)
        print(f"Dados coletados e salvos para a música {track_id}")
    except Exception as e:
        print(f"Erro ao coletar dados: {e}")

# Agendar a função para rodar a cada 1 hora
schedule.every(1).hours.do(collect_spotify_data)

# Manter o agendamento rodando
while True:
    schedule.run_pending()
    time.sleep(1)
