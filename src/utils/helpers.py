import sqlite3

class SpotifyDatabaseManager:
    def __init__(self, db_name="spotify_data.db"):
        self.db_name = db_name
        self.create_tables()

    def connect(self):
        """Conecta ao banco de dados SQLite."""
        return sqlite3.connect(self.db_name)

    def create_tables(self):
        """Cria as tabelas 'tracks' e 'artists' se elas não existirem."""
        conn = self.connect()
        cursor = conn.cursor()

        # Criar tabela de músicas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            id TEXT PRIMARY KEY,
            name TEXT,
            artist_name TEXT,
            artist_id TEXT,
            acousticness REAL,
            danceability REAL,
            energy REAL,
            tempo REAL,
            valence REAL,
            popularity INTEGER
        )
        ''')

        # Criar tabela de artistas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS artists (
            id TEXT PRIMARY KEY,
            name TEXT,
            popularity INTEGER,
            followers INTEGER,
            genres TEXT
        )
        ''')

        conn.commit()
        conn.close()

    def save_track_data(self, track_data, artist_data):
        """Salva ou atualiza as informações de uma música e do artista no banco de dados."""
        conn = self.connect()
        cursor = conn.cursor()

        # Salvar ou atualizar informações da música
        cursor.execute('''
        INSERT OR REPLACE INTO tracks 
        (id, name, artist_name, artist_id, acousticness, danceability, energy, tempo, valence, popularity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            track_data['id'],
            track_data['name'],
            artist_data['name'],
            artist_data['id'],
            track_data.get('acousticness', None),
            track_data.get('danceability', None),
            track_data.get('energy', None),
            track_data.get('tempo', None),
            track_data.get('valence', None),
            track_data['popularity']
        ))

        # Salvar ou atualizar informações do artista
        cursor.execute('''
        INSERT OR REPLACE INTO artists 
        (id, name, popularity, followers, genres)
        VALUES (?, ?, ?, ?, ?)
        ''', (
            artist_data['id'],
            artist_data['name'],
            artist_data['popularity'],
            artist_data['followers']['total'],
            ', '.join(artist_data.get('genres', []))
        ))

        conn.commit()
        conn.close()

    def get_all_tracks(self):
        """Retorna todas as músicas do banco de dados."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM tracks")
        tracks = cursor.fetchall()

        conn.close()
        return tracks

    def get_all_artists(self):
        """Retorna todos os artistas do banco de dados."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM artists")
        artists = cursor.fetchall()

        conn.close()
        return artists

    def get_track_by_id(self, track_id):
        """Busca uma música pelo ID."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM tracks WHERE id = ?", (track_id,))
        track = cursor.fetchone()

        conn.close()
        return track

    def get_artist_by_id(self, artist_id):
        """Busca um artista pelo ID."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM artists WHERE id = ?", (artist_id,))
        artist = cursor.fetchone()

        conn.close()
        return artist
