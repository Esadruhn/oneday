import sqlite3
import pathlib
import pandas as pd

DB_NAME = "customerdatabase.db"
DB_PATH = pathlib.Path(__file__).parent / DB_NAME
CSV_PATH = pathlib.Path(__file__).parent / "data" / "customer_acquisition_data.csv"


def init_db(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE channel (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
    """
    )
    cursor.execute(
        """
        INSERT INTO channel
        VALUES  (1,'referral'),
                (2,'paid advertising'),
                (3,'email marketing'),
                (4,'social media')
    """
    )
    connection.commit()
    cursor.execute(
        """
        CREATE TABLE customer (
            id INTEGER PRIMARY KEY,
            cost FLOAT NOT NULL,
            conversion_rate FLOAT NOT NULL,
            revenue INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            FOREIGN KEY (channel_id) REFERENCES channel(id)
        );
    """
    )

    df = pd.read_csv(CSV_PATH, index_col=0)
    channels_res = cursor.execute("""SELECT id, name from channel""")
    channels = channels_res.fetchall()
    channel_dict = {name: idx for idx, name in channels}
    df = df.replace({"channel": channel_dict})
    data = list(df.itertuples(index=True, name=None))

    cursor.executemany(
        """INSERT INTO customer (id, channel_id, cost, conversion_rate, revenue) VALUES (?, ?, ?, ? , ?)""",
        data,
    )
    connection.commit()

    cursor.close()


def analyse(cursor):
    res = cursor.execute(
        """
        SELECT channel.name, AVG(cost) as mean_cost, AVG(revenue) as mean_revenue, AVG(conversion_rate) as mean_conv_rate
        FROM customer
        INNER JOIN channel ON customer.channel_id = channel.id
        GROUP BY channel_id
        ORDER BY mean_cost ASC, mean_revenue DESC, mean_conv_rate DESC
    """
    )
    data = res.fetchall()
    return data

def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

def main():
    # Recreate the database every time
    if DB_PATH.is_file():
        DB_PATH.unlink()
    connection = sqlite3.connect(DB_NAME)
    connection.row_factory = dict_factory
    init_db(connection)

    cursor = connection.cursor()
    stat_data = analyse(cursor)
    cursor.close()

    print(stat_data)

    connection.close()


if __name__ == "__main__":
    main()
