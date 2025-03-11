import sqlite3
import pathlib

DB_NAME = 'customerdatabase.db'
DB_PATH = pathlib.Path(__file__).parent / DB_NAME

def main():
    # Recreate the database every time
    if DB_PATH.is_file():
        DB_PATH.unlink()
    con = sqlite3.connect('customerdatabase.db') 
    con.execute('''
        CREATE TABLE channel (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
    ''')
    con.execute('''
        INSERT INTO channel
        VALUES  (1,'referral'),
                (2,'paid advertising'),
                (3,'email marketing'),
                (4,'social media')
    ''')
    con.execute('''
        CREATE TABLE cutomers (
            id INTEGER PRIMARY KEY,
            cost FLOAT NOT NULL,
            conversion_rate FLOAT NOT NULL,
            revenue INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            FOREIGN KEY (channel_id) REFERENCES channel(id)
        );
    ''')


if __name__ == "__main__":
    main()