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
            publish_time TEXT NOT NULL,
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
    df["Publish time"] = pd.to_datetime(df["Publish time"]).astype(str)
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
    print(f"There are {nb_posts} posts.")

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


def analyse_posts_by_type(cursor):

    post_type_stats = cursor.execute(
        """
        SELECT 
            post_type, 
            COUNT(*) as nb_posts
        FROM post
        GROUP BY post_type
        ORDER BY nb_posts ASC
    """
    ).fetchall()

    return post_type_stats


def get_post_distribution(cursor):
    """
    Outliers are values that fall outside of 1.5 times the range between the first and third quartile.
    """

    params = {
        "impressions": "impressions",
        "likes*1000 / impressions": "1000*likes / NULLIF(impressions, 0)",
        "shares*1000 / impressions": "1000*shares / NULLIF(impressions, 0)",
        "follows*1000 / impressions": "1000*follows / NULLIF(impressions, 0)",
        "reach*1000 / impressions": "1000*reach / NULLIF(impressions, 0)",
    }
    all_quartiles = dict()
    for key, param in params.items():
        quartile_breaks = cursor.execute(
            f""" 
            SELECT
                param_quartile,
                MAX(param) AS quartile_break
            FROM(
                SELECT
                    {param} as param,
                    NTILE(4) OVER (ORDER BY {param}) AS param_quartile
                FROM post) AS quartiles
            GROUP BY param_quartile
            """
        ).fetchall()

        quartile_breaks_by_type = cursor.execute(
            f""" 
            SELECT
                param_quartile,
                post_type,
                MAX(param) AS quartile_break
            FROM(
                SELECT
                    {param} as param,
                    post_type,
                    NTILE(4) OVER (PARTITION BY post_type ORDER BY post_type, {param}) AS param_quartile
                FROM post) AS quartiles
            WHERE param_quartile IN (1,2,3)
            GROUP BY post_type, param_quartile
            """
        ).fetchall()

        # List of [names, q1s, medians, q3s, lowerfences, upperfences]
        quartiles = [
            ["all"],  # names
            [
                t["quartile_break"] for t in quartile_breaks if t["param_quartile"] == 1
            ],  # q1
            [
                t["quartile_break"] for t in quartile_breaks if t["param_quartile"] == 2
            ],  # median
            [
                t["quartile_break"] for t in quartile_breaks if t["param_quartile"] == 3
            ],  # q3
            [],  # lowerfence
            [],  # upperfence
        ]
        for item in quartile_breaks_by_type:
            if item["param_quartile"] == 1:
                quartiles[0].append(item["post_type"])
            assert (
                quartiles[0][-1] == item["post_type"]
            )  # extra precaution to ensure we are on the right post type
            quartiles[item["param_quartile"]].append(item["quartile_break"])

        # calculate lower and upper fences
        quartiles[4] = [
            q1 - (q3 - q1) * 1.5 for q1, q3 in zip(quartiles[1], quartiles[3])
        ]
        quartiles[5] = [
            q3 + (q3 - q1) * 1.5 for q1, q3 in zip(quartiles[1], quartiles[3])
        ]

        # only calculated for all posts, not by post type
        outliers = cursor.execute(
            f"""SELECT {param} as param
                FROM post
                WHERE {param} < ? OR {param} > ? ORDER BY param ASC""",
            (quartiles[4][0], quartiles[5][0]),
        ).fetchall()
        print(f"{len(outliers)} outliers for the parameter {param}.")

        all_quartiles[key] = quartiles

    return all_quartiles


def get_date_impact(cursor):
    return cursor.execute(
        """SELECT 
                DATE(publish_time) AS publish,
                SUM(impressions) AS total_impressions,
                SUM(likes) AS total_likes
            FROM post
            GROUP BY publish
            ORDER BY publish ASC;
        """
    ).fetchall()


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
    analyse_posts(cursor)
    post_type_stats = analyse_posts_by_type(cursor)
    all_quartiles = get_post_distribution(cursor)
    date_data = get_date_impact(cursor)
    cursor.close()

    fig = make_subplots(
        rows=3,
        cols=3,
        subplot_titles=[
            "Number of posts",
        ]
        + [f"Distribution of {name}" for name in all_quartiles.keys()]
        + [
            "Number of impressions per publishing date",
            "Number of likes per publishing date",
        ],
    )
    # Count of posts
    fig.add_trace(
        go.Bar(
            x=[t["post_type"] for t in post_type_stats],
            y=[t["nb_posts"] for t in post_type_stats],
            name="Number of posts",
        ),
        row=1,
        col=1,
    )
    for idx, (name, quartiles) in enumerate(all_quartiles.items()):
        fig.add_trace(
            go.Box(
                x=quartiles[0],
                q1=quartiles[1],
                median=quartiles[2],
                q3=quartiles[3],
                lowerfence=quartiles[4],
                upperfence=quartiles[5],
                name=f"{name} Quartiles",
            ),
            row=((idx + 1) // 3) + 1,
            col=(idx + 1) % 3 + 1,
        )
    fig.add_trace(
        go.Bar(
            x=[item["publish"] for item in date_data],
            y=[item["total_impressions"] for item in date_data],
            name="Impressions per day",
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=[item["publish"] for item in date_data],
            y=[item["total_likes"] for item in date_data],
            name="Likes per day",
        ),
        row=3,
        col=2,
    )
    fig.show()

    connection.close()


if __name__ == "__main__":
    main()
