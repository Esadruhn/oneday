import sqlite3
import pathlib
import pandas as pd
import plotly.graph_objects as go
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


def analyse_posts(cursor):
    nb_posts = cursor.execute("""SELECT COUNT(*) as count FROM post""").fetchone()[
        "count"
    ]

    follows_null = cursor.execute(
        """SELECT COUNT(*) as nb_null_follows FROM post WHERE follows IS NULL"""
    ).fetchone()["nb_null_follows"]
    print(f"Number of null follows: {follows_null} out of {nb_posts} posts")

    plays_null = cursor.execute(
        """SELECT COUNT(*) as nb_null_plays FROM post WHERE plays IS NULL"""
    ).fetchone()["nb_null_plays"]
    print(f"Number of null plays: {plays_null} out of {nb_posts} posts")

    data_comment_null = cursor.execute(
        """SELECT COUNT(*) as nb_null_comment FROM post WHERE data_comment IS NULL"""
    ).fetchone()["nb_null_comment"]
    print(f"Number of null data comments: {data_comment_null} out of {nb_posts} posts")

    post_stats = cursor.execute(
        """ SELECT
            post_type,
            impressions,
            likes,
            1000*likes / NULLIF(impressions, 0) as rel_likes,
            1000*shares / NULLIF(impressions, 0) as rel_shares,
            1000*follows / NULLIF(impressions, 0) as rel_follows,
            1000*reach / NULLIF(impressions, 0) as rel_reach,
            1000*saves / NULLIF(impressions, 0) as rel_saves
            FROM post
            ORDER BY impressions DESC, rel_likes DESC
        """
    ).fetchall()

    return post_stats


def analyse_posts_by_type(cursor):

    post_type_stats = cursor.execute(
        """
        SELECT 
            post_type, 
            COUNT(*) as nb_posts,
            AVG(impressions) as mean_impressions,
            MIN(impressions) as min_impressions,
            MAX(impressions) as max_impressions,
            SUM(likes / impressions) / COUNT(*) as rel_likes,
            AVG(likes) as mean_likes,
            MIN(likes) as min_likes,
            MAX(likes) as max_likes,
            MIN(shares) as min_shares,
            MAX(shares) as max_shares,
            AVG(shares) as mean_shares,
            MIN(follows) as min_follows,
            MAX(follows) as max_follows,
            AVG(follows) as mean_follows,
            MIN(saves) as min_saves,
            MAX(saves) as max_saves,
            AVG(saves) as mean_saves
        FROM post
        GROUP BY post_type
        ORDER BY nb_posts ASC
    """
    ).fetchall()

    return post_type_stats


def get_outliers(cursor):
    """
    Outliers are values that fall outside of 1.5 times the range between the first and third quartile.
    """
    quartile_breaks = cursor.execute(
        """ 
        SELECT
            impr_quartile,
            MAX(impressions) AS quartile_break
        FROM(
            SELECT
                impressions,
                NTILE(4) OVER (ORDER BY impressions) AS impr_quartile
            FROM post) AS quartiles
        WHERE impr_quartile IN (1, 3)
        GROUP BY impr_quartile
        """
    ).fetchall()
    print(quartile_breaks)
    # [{'impr_quartile': 1, 'quartile_break': 13452}, {'impr_quartile': 3, 'quartile_break': 28918}]

    lower_bound = quartile_breaks[0]["quartile_break"]
    upper_bound = quartile_breaks[1]["quartile_break"]
    iqr = (upper_bound - lower_bound) * 1.5
    lower = lower_bound - iqr
    upper = upper_bound + iqr

    outliers = cursor.execute(
        """SELECT *
                                FROM post
                                WHERE impressions < ? OR impressions > ?""",
        (lower, upper),
    ).fetchall()
    print(len(outliers))
    return outliers


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def main():
    # Recreate the database every time
    if DB_PATH.is_file():
        DB_PATH.unlink()
    connection: sqlite3.Connection = sqlite3.connect(DB_PATH)
    connection.row_factory = dict_factory
    init_db(connection)

    cursor = connection.cursor()
    post_stats = analyse_posts(cursor)
    post_type_stats = analyse_posts_by_type(cursor)
    outliers = get_outliers(cursor)
    cursor.close()

    fig = make_subplots(
        rows=2,
        cols=6,
        subplot_titles=[
            "Number of posts",
            "Mean number of impressions",
            "Mean number of likes",
            "Mean number of shares",
            "Mean number of follows",
            "Mean number of saves",
            "",
            "Number of impressions",
            "Relative number of likes per impression (*1000)",
            "Relative number of shares per impression (*1000)",
            "Relative number of follows per impression (*1000)",
            "Relative number of saves per impression (*1000)",
        ],
    )
    # Count of posts
    fig.add_trace(
        go.Bar(
            x=[t["post_type"] for t in post_type_stats],
            y=[t["nb_posts"] for t in post_type_stats],
        ),
        row=1,
        col=1,
    )
    # Average impressions
    fig.add_trace(
        go.Bar(
            x=[t["post_type"] for t in post_type_stats],
            y=[t["mean_impressions"] for t in post_type_stats],
        ),
        row=1,
        col=2,
    )
    fig.add_trace(
        go.Bar(
            x=[t["post_type"] for t in post_type_stats],
            y=[t["mean_likes"] for t in post_type_stats],
        ),
        row=1,
        col=3,
    )
    fig.add_trace(
        go.Bar(
            x=[t["post_type"] for t in post_type_stats],
            y=[t["mean_shares"] for t in post_type_stats],
        ),
        row=1,
        col=4,
    )
    fig.add_trace(
        go.Bar(
            x=[t["post_type"] for t in post_type_stats],
            y=[t["mean_follows"] for t in post_type_stats],
        ),
        row=1,
        col=5,
    )
    fig.add_trace(
        go.Bar(
            x=[t["post_type"] for t in post_type_stats],
            y=[t["mean_saves"] for t in post_type_stats],
        ),
        row=1,
        col=6,
    )

    # Average impressions
    color_dict = {"IG image": "blue", "IG reel": "red", "IG carousel": "green"}
    colors: list[str] = [color_dict[t["post_type"]] for t in post_stats]
    fig.add_trace(
        go.Bar(
            x=list(range(len(post_stats))),
            y=[t["impressions"] for t in post_stats],
            marker_color=colors,
        ),
        row=2,
        col=2,
    )
    fig.add_trace(
        go.Bar(
            x=list(range(len(post_stats))),
            y=[t["rel_likes"] for t in post_stats],
            marker_color=colors,
        ),
        row=2,
        col=3,
    )
    fig.add_trace(
        go.Bar(
            x=list(range(len(post_stats))),
            y=[t["rel_shares"] for t in post_stats],
            marker_color=colors,
        ),
        row=2,
        col=4,
    )
    fig.add_trace(
        go.Bar(
            x=list(range(len(post_stats))),
            y=[t["rel_follows"] for t in post_stats],
            marker_color=colors,
        ),
        row=2,
        col=5,
    )
    fig.add_trace(
        go.Bar(
            x=list(range(len(post_stats))),
            y=[t["rel_saves"] for t in post_stats],
            marker_color=colors,
        ),
        row=2,
        col=6,
    )

    fig.show()

    connection.close()


if __name__ == "__main__":
    main()
