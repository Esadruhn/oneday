import sqlite3
import pathlib
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
from plotly.subplots import make_subplots


DB_NAME = "postdatabase.db"
DB_PATH = pathlib.Path(__file__).parent / DB_NAME
CSV_PATH = pathlib.Path(__file__).parent / "data" / "Instagram-Data-1.csv"


def init_db(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE post (
            id INTEGER PRIMARY KEY,
            description TEXT NOT NULL,
            duration INTEGER NOT NULL,
            publish_time DATETIME NOT NULL,
            permalink VARCHAR(50) NOT NULL CHECK(LENGTH(permalink) <= 50),
            post_type VARCHAR(30) NOT NULL CHECK(LENGTH(post_type) <= 30),
            data_comment VARCHAR(100) CHECK(data_comment IS NULL or LENGTH(data_comment) <= 100),
            date INTEGER NOT NULL,
            impressions INTEGER NOT NULL,
            reach INTEGER NOT NULL,
            likes INTEGER NOT NULL,
            shares INTEGER NOT NULL,
            follows INTEGER,
            comments INTEGER NOT NULL,
            saves INTEGER NOT NULL,
            plays INTEGER
        );
    """
    )
    df = pd.read_csv(CSV_PATH, index_col=0)
    data = list(df.itertuples(index=True, name=None))

    assert len(data) > 1, "Data file is empty"
    execute_str = "INSERT INTO post VALUES (" + "?," * (len(data[0]) - 1) + "?)"
    cursor.executemany(
        execute_str,
        data,
    )
    connection.commit()
    cursor.close()


def analyse(cursor):
    post_types_res = cursor.execute(
        """
        SELECT 
            post_type, 
            COUNT(*) as number_of_posts,
            AVG(impressions),
            AVG(likes),
        FROM post
        GROUP BY post_type
    """
    )
    post_types = post_types_res.fetchall()
    print(f"Post types")
    print(post_types)


def main():
    # Recreate the database every time
    if DB_PATH.is_file():
        DB_PATH.unlink()
    connection = sqlite3.connect(DB_NAME)
    init_db(connection)

    cursor = connection.cursor()
    stat_data = analyse(cursor)
    cursor.close()

    # fig = make_subplots(
    #     rows=2,
    #     cols=2,
    #     subplot_titles=[
    #         "Customer Acquisition Cost by Channel",
    #         "Customer Revenue by Channel",
    #         "Customer Conversion Rate by Channel",
    #         "Customer Lifetime Value by Channel",
    #     ],
    # )
    # fig.add_trace(
    #     go.Bar(x=[t[0] for t in stat_data], y=[t[1] for t in stat_data]), row=1, col=1
    # )
    # fig.add_trace(
    #     go.Bar(x=[t[0] for t in stat_data], y=[t[2] for t in stat_data]), row=1, col=2
    # )
    # fig.add_trace(
    #     go.Bar(x=[t[0] for t in stat_data], y=[t[3] for t in stat_data]), row=2, col=1
    # )
    # fig.add_trace(
    #     go.Bar(x=[t[0] for t in stat_data], y=[t[4] for t in stat_data]), row=2, col=2
    # )

    # fig.show()

    connection.close()


if __name__ == "__main__":
    main()
