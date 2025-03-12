# EDA with SQL

https://statso.io/eda-using-sql-case-study/

You are a Data Analyst at a digital marketing firm tasked with optimizing Instagram content performance. Using a dataset of historical Instagram post metrics, perform an Exploratory Data Analysis (EDA) to uncover insights about engagement, post performance, and audience behavior.

Download the dataset into the `data` folder (`Instagram-Data-1.csv` file, abiut 250kB).

Use Python 3.12

```bash
pip install -r requirements.txt
python main.py
```

Null values: only in the `follows` and `plays columns`:

- follows: 5 /  446 posts
- plays: 314 / 446 posts
- data comments: 446 / 446 posts

We will not consider the `plays` or `data comments` category as there are too many unknown values.
The number of null follows is low, might ask why there are some, I would assume that a null value means 0 follows.

Most posts are of the carousel type, but the image posts get more impressions on average and the reel posts more likes, shares and follows.
A quarter of the posts get a lot of visibility (impressions): 100 / 446 have between 30 and 500K impressions, there are 57 outliers in the data.

