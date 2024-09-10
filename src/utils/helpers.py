import pandas as pd
import os

class SpotifyDatabaseManager:
    def __init__(self, parquet_file="spotify_data.parquet"):
        self.parquet_file = parquet_file

    def save_track_data_parquet(self, track_data, artist_data):
        """
        Salva as informações da música e do artista em um arquivo Parquet local.
        :param track_data: Dicionário com dados da música
        :param artist_data: Dicionário com dados do artista
        """
        combined_data = {
            'track_id': track_data['id'],
            'track_name': track_data['name'],
            'artist_id': artist_data['id'],
            'artist_name': artist_data['name'],
            'acousticness': track_data.get('acousticness', None),
            'danceability': track_data.get('danceability', None),
            'energy': track_data.get('energy', None),
            'tempo': track_data.get('tempo', None),
            'valence': track_data.get('valence', None),
            'popularity_track': track_data.get('popularity', None),
            'popularity_artist': artist_data.get('popularity', None),
            'followers': artist_data['followers']['total'],
            'genres': ', '.join(artist_data.get('genres', []))
        }

        df = pd.DataFrame([combined_data])

        # Verificar se o arquivo Parquet já existe
        if not os.path.exists(self.parquet_file):
            df.to_parquet(self.parquet_file, engine='pyarrow', index=False)
        else:
            df_existing = pd.read_parquet(self.parquet_file, engine='pyarrow')
            df_combined = pd.concat([df_existing, df], ignore_index=True)
            df_combined.to_parquet(self.parquet_file, engine='pyarrow', index=False)

