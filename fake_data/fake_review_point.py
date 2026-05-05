from faker import Faker
import random
from datetime import datetime,timedelta
import csv
import pandas as pd

fake = Faker()

fact_order = pd.read_csv("fact_order.csv")
reviews = []
for i, row in fact_order.iterrows():
    if random.random() > 0.8:
        continue

    rating = random.choices(
        [5, 4, 3, 2, 1],
        weights=[0.7, 0.15, 0.1, 0.04, 0.01]
    )[0]

    order_date = pd.to_datetime(row["order_date"])

    review_date = order_date + timedelta(days=random.randint(1, 30))

    reviews.append([
        i,
        row["product_id"],
        fake.name(),
        review_date,
        fake.email(),
        rating,
        fake.sentence(),
        review_date
    ])

df = pd.DataFrame(reviews, columns=[
    "ReviewID",
    "ProductID",
    "ReviewerName",
    "ReviewDate",
    "Email",
    "Rating",
    "Comment",
    "ModifiedDate"
])

df.to_csv("product_review.csv", index=False)
print("Done")
print("First 10 rows:")
print(df.head(10))

