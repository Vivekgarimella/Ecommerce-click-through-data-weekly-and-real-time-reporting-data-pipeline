import pandas as pd
import time
import random
# reading mock data
df = pd.read_csv("/Users/garim/Downloads/raw_sample.csv")

# considering
df = df[:len(df)//10]

# dropping unnecessary columns
df = df.drop('nonclk',axis = 1)

# renaming column names to hae better understanding
df = df.rename(columns= {"user":"user_id","time_stamp":"date","adgroup_id":"product_category","pid":"product_name","clk":"click_event"})

# generating timestamps
df['date'] = df['date'] + (100*df['user_id'])
df['date'] = df['date'].apply(lambda x: pd.to_datetime(time.strftime('%Y-%m-%d', (time.localtime(x)))))

# considering categories and products present in gonoodle's ecommerce site only.
categories = ["Classroom", "Apparel", "Bundles", "Sale"]
category_dict = {"Classroom": ["Champs Bookmarks", "The Best Tees Sticker Sheet", "Blazer Fresh Sticker Sheet",
                               "Mindfulness Starter Set",
                               "Mindfulness Tonie", "GoNoodle Lanyard", "Champs Sticker Sheet", "Champs Pencil Set"],
                 "Apparel": ["Champs Pins", "Tee (White)",
                             "Tee (Black)", "GoNoodle Baseball Cap", "Mooshigan Tee", "GoNoodle Verb Tee",
                             "Blazer Fresh Youth T-Shirt",
                             "Noodle Television Youth T-Shirt", "Go Noodle Youth T-Shirt", "GoNoodle Youth Apron"],
                 "Bundles": ["Learn On The Go Pack",
                             "For The Classroom", "Teacher Appreciation Pack", "Birthday Party Kit"],
                 "Sale": ["GoNoodle Temporary Tattoo - Bundle of 25", "Gulps Wodda Bottle", "GoNoodle Verb Tee",
                          "GoNoodle Tote Bag", "GoNoodle Nalgene® Water Bottle", "GoNoodle Coffee Mug",
                          "GoNoodle Journal", "The Best Tees Sticker Sheet",
                          "GoNoodle Champ Cookie Cutters", "GoNoodle Youth Long Sleeve Tee",
                          "Squatchy Berger Kid's Beanie",
                          "GoNoodle Champ Kid's Socks", "Birthday Party Kit"]

                 }

products = ["Champs Bookmarks", "The Best Tees Sticker Sheet", "Blazer Fresh Sticker Sheet", "Mindfulness Starter Set",
            "Mindfulness Tonie", "GoNoodle Lanyard", "Champs Sticker Sheet", "Champs Pencil Set", "Champs Pins",
            "Tee (White)",
            "Tee (Black)", "GoNoodle Baseball Cap", "Mooshigan Tee", "GoNoodle Verb Tee", "Blazer Fresh Youth T-Shirt",
            "Noodle Television Youth T-Shirt", "Go Noodle Youth T-Shirt", "GoNoodle Youth Apron",
            "Learn On The Go Pack",
            "For The Classroom", "Teacher Appreciation Pack", "Birthday Party Kit", "Blazer Fresh Youth T-Shirt",
            "GoNoodle Temporary Tattoo - Bundle of 25",
            "Gulps Wodda Bottle", "GoNoodle Verb Tee", "GoNoodle Tote Bag", "GoNoodle Nalgene® Water Bottle",
            "GoNoodle Coffee Mug",
            "GoNoodle Journal", "The Best Tees Sticker Sheet", "GoNoodle Champ Cookie Cutters",
            "GoNoodle Youth Long Sleeve Tee",
            "Squatchy Berger Kid's Beanie", "GoNoodle Champ Kid's Socks", "Birthday Party Kit"]

# randomly distribution of product names
df['product_name'] = df['product_name'].apply(lambda x: random.choice(products))

df['product_category'] = df['product_name']

# setting of respective product categories
def category_finder(x,dictionary):
    for i,j in dictionary.items():
        if x in j:
            return i

df['product_category'] = df['product_category'].apply(lambda x:category_finder(x,category_dict))

# writing mock data to AWS S3 bucket
df.reset_index().to_json("s3://ecommmerce/data/ecommerce_data.json")