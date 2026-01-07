import pandas as pd
df = pd.read_json('yelp_academic_dataset_business.json', lines=True)

#print (df.head())
#print(df.columns)
business_ids = df['business_id']
name = df['name']
address = df['full_address']
hours = df['hours']
city = df['city']
state = df['state']
review_coutn = df['review_count']
stars = df['stars']

selected_columns = df[['business_id', 'name', 'full_address', 'hours', 'city','state','review_count','stars']]

print(selected_columns.head())

''' Index(['business_id', 'full_address', 'hours', 'open', 'categories', 'city',
       'review_count', 'name', 'neighborhoods', 'longitude', 'state', 'stars',
       'latitude', 'attributes', 'type'],
      dtype='object') '''


#print(df['business_id')])
#Next you are asked to identify the number of businesses in the state of Wisconsin with greater than 20 reviews.

state_wisconsin = df.loc['state'] == "WI"

state_wisconsin_number_of_businesses = df[state_wisconsin] && (df[review_count'] >
