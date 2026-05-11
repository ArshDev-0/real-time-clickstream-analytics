import pandas as pd
import random
import time

rows = 1500  # You can increase to 200000 if you want

pages = ["home", "products", "about", "contact", "cart", "checkout"]
countries = ["India", "USA", "UK", "Germany", "Canada"]

data = []

for i in range(rows):
    data.append({
        "user_id": random.randint(1, 5000),
        "page": random.choice(pages),
        "country": random.choice(countries),
        "timestamp": int(time.time())
    })

df = pd.DataFrame(data)
df.to_csv("clickstream_large.csv", index=False)

print("Dataset generated successfully!")
