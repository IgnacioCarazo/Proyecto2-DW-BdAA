# -*- coding: utf-8 -*-

import time
import random
import pandas as pd
import datetime

# Extract
df_game_reviews = pd.read_csv('./data/steam_review_10_rows.csv')
df_game_details = pd.read_csv('./data/games.csv')


# Transform

# Eliminar filas con al menos un valor nulo en df_game_reviews
df_game_reviews = df_game_reviews.dropna()

# Eliminar filas con al menos un valor nulo en df_game_details
df_game_details = df_game_details.dropna()

# Eliminar filas duplicadas en df_game_reviews
df_game_reviews = df_game_reviews.drop_duplicates()

# Eliminar filas duplicadas en df_game_details
df_game_details = df_game_details.drop_duplicates()

df_game_reviews.columns = df_game_reviews.columns.str.lower().str.replace('.', '_')
df_game_details.columns = df_game_details.columns.str.lower().str.replace('.', '_')
df_game_reviews.columns = df_game_reviews.columns.str.lower().str.replace(' ', '_')
df_game_details.columns = df_game_details.columns.str.lower().str.replace(' ', '_')


# Drop unnecessary columns
df_game_reviews = df_game_reviews.drop(
    columns=['weighted_vote_score'])

df_game_details = df_game_details[[
    'appid', 'name', 'categories', 'developers', 'genres']]

# Compare timestamp_created and timestamp_updated, and assign True/False
df_game_reviews['timestamp_updated'] = df_game_reviews.apply(lambda row: pd.to_datetime(
    row['timestamp_updated'], unit='s') != pd.to_datetime(row['timestamp_created'], unit='s'), axis=1)

# Rename the column to 'updated'
df_game_reviews = df_game_reviews.rename(
    columns={'timestamp_updated': 'updated'})
# Rename the column to 'updated'
df_game_reviews = df_game_reviews.rename(
    columns={'author_steamid': 'steam_id'})


# Convert timestamp_created to datetime
df_game_reviews['timestamp_created'] = pd.to_datetime(
    df_game_reviews['timestamp_created'], unit='s')


# Convert author_last_played to datetime
df_game_reviews['author_last_played'] = pd.to_datetime(
    df_game_reviews['author_last_played'], unit='s')

# Create column playtime_percentage_change
df_game_reviews["playtime_percentage_change"] = df_game_reviews.apply(lambda row: (
    row["author_playtime_last_two_weeks"] / row["author_playtime_forever"] * 100) if row["author_playtime_forever"] > 0 else 0, axis=1)

# Create column percentage_played_after_review
df_game_reviews["percentage_played_after_review"] = df_game_reviews.apply(
    lambda row: ((row["author_playtime_forever"] -
                 row["author_playtime_at_review"]) / row["author_playtime_forever"] * 100)
    if row["author_playtime_forever"] > 0 else 0,
    axis=1
)


# Create Game Review Fact Table

# Initialize dimension tables with empty dataframes
df_language = pd.DataFrame(columns=['id', 'name'])
df_date = pd.DataFrame(columns=['id', 'date', 'year', 'month', 'day'])
df_game = pd.DataFrame(columns=['id', 'steam_id', 'name'])
df_author = pd.DataFrame(columns=['id', 'steam_id'])
df_developer = pd.DataFrame(columns=['id', 'name'])
df_genre = pd.DataFrame(columns=['id', 'name'])
df_category = pd.DataFrame(columns=['id', 'name'])
df_method_of_acquisition = pd.DataFrame(
    columns=['id', 'steam_purchase',
             'received_for_free', 'early_access']
)

# Initialize fact tables
game_review_columns = ['review_id', 'author_id', 'game_id', 'language_id', 'date_id',
                       'steam_id', 'votes_helpful', 'votes_funny', 'updated', 'recommended']
df_reviews = pd.DataFrame(columns=game_review_columns)

game_playtime_columns = ['author_playtime_id', 'author_id', 'game_id', 'developer_id', 'category_id', 'genre_id',
                         'method_of_acquisition_id', 'playtime_forever', 'playtime_last_two_weeks', 'playtime_at_review', 'last_played', 'num_games_owned', 'num_reviews', 'playtime_percentage_change']
df_playtime = pd.DataFrame(columns=game_playtime_columns)

# Create helper functions to assign auto-incrementing IDs


def get_or_create_id(df, column_name, value):
    # Check if the value already exists in the DataFrame
    if value in df[column_name].values:
        # If exists, return the id
        return df[df[column_name] == value].iloc[0]['id'], df
    else:
        # If not exists, create a new row with a new ID
        new_id = df['id'].max() + 1 if not df.empty else 1
        new_row = pd.DataFrame({column_name: [value], 'id': [new_id]})
        # Use pd.concat to add the new row
        df = pd.concat([df, new_row], ignore_index=True)
        return new_id, df


def get_or_create_acquisition_method_id(df, steam_purchase, received_for_free, early_access):
    # Check if the exact combination already exists
    existing_row = df[(df['steam_purchase'] == steam_purchase) &
                      (df['received_for_free'] == received_for_free) &
                      (df['early_access'] == early_access)]

    if not existing_row.empty:
        return existing_row.iloc[0]['id'], df
    else:
        new_id = df['id'].max() + 1 if not df.empty else 1
        new_row = pd.DataFrame({
            'id': [new_id],
            'steam_purchase': [steam_purchase],
            'received_for_free': [received_for_free],
            'early_access': [early_access]
        })
        df = pd.concat([df, new_row], ignore_index=True)
        return new_id, df


# Function to get or create a date ID
def get_or_create_date_id(df, date):
    if date in df['date'].values:
        return df[df['date'] == date].iloc[0]['id'], df
    else:
        new_id = df['id'].max() + 1 if not df.empty else 1
        new_row = pd.DataFrame({'id': [new_id], 'date': [date],
                                'year': [date.year], 'month': [date.month], 'day': [date.day]})
        df = pd.concat([df, new_row], ignore_index=True)
        return new_id, df


# Populate game review fact table and its associated dimensions
# Iterate over each row in df_game_reviews and populate fact tables and dimensions

start_time = time.time()  # Start the overall timer
iteration_count = 0  # Initialize the iteration counter

for index, row in df_game_reviews.iterrows():
    # Start the timer for every 1k iterations
    if iteration_count % 1000 == 0 and iteration_count > 0:
        elapsed_time = time.time() - last_batch_time  # Time for the last 1k iterations
        print(f"Processed {iteration_count} rows. Time for last 1k iterations: {
              elapsed_time:.2f} seconds.")

    # Language ID
    language_id, df_language = get_or_create_id(
        df_language, 'name', row['language'])

    # Date ID
    date = pd.to_datetime(row['timestamp_created'])
    date_id, df_date = get_or_create_date_id(df_date, date)

    # Fetch game name from df_game_details
    game_name = df_game_details.loc[df_game_details['appid']
                                    == row['app_id'], 'name']

    # Convert to string (if found) or assign 'Unknown' if missing
    game_name = game_name.iloc[0] if not game_name.empty else 'Unknown'

    # Get or create game ID while including the game name
    if row['app_id'] in df_game['steam_id'].values:
        game_id = df_game[df_game['steam_id'] == row['app_id']].iloc[0]['id']
    else:
        new_id = df_game['id'].max() + 1 if not df_game.empty else 1
        new_row = pd.DataFrame({'id': [new_id], 'steam_id': [
                               row['app_id']], 'name': [game_name]})
        df_game = pd.concat([df_game, new_row], ignore_index=True)
        game_id = new_id

    # Author ID
    author_id, df_author = get_or_create_id(
        df_author, 'steam_id', row['steam_id'])

    # Review Table - Create a new row for df_reviews
    new_review_row = {
        'review_id': index + 1,  # Assuming review_id is the index + 1
        'author_id': author_id,
        'game_id': game_id,
        'language_id': language_id,
        'date_id': date_id,
        'steam_id': row['steam_id'],
        'votes_helpful': row['votes_helpful'],
        'votes_funny': row['votes_funny'],
        'updated': row['updated'],
        'recommended': row['recommended']
    }

    # Concatenate the new row to the reviews DataFrame
    new_review_row = pd.DataFrame([new_review_row])
    df_reviews = pd.concat([df_reviews, new_review_row], ignore_index=True)

    # Get game details
    game_details = df_game_details[df_game_details['appid'] == row['app_id']]

    if game_details.empty:
        continue  # Skip if no game details found

    # Extract developers, categories, and genres
    developers = game_details.iloc[0]['developers'].split(
        ';') if pd.notna(game_details.iloc[0]['developers']) else []
    categories = game_details.iloc[0]['categories'].split(
        ';') if pd.notna(game_details.iloc[0]['categories']) else []
    genres = game_details.iloc[0]['genres'].split(
        ';') if pd.notna(game_details.iloc[0]['genres']) else []

    # Get IDs for developers
    developer_ids = []
    for dev in developers:
        dev_id, df_developer = get_or_create_id(df_developer, 'name', dev)
        developer_ids.append(dev_id)

    # Get IDs for categories
    category_ids = []
    for cat in categories:
        cat_id, df_category = get_or_create_id(df_category, 'name', cat)
        category_ids.append(cat_id)

    # Get IDs for genres
    genre_ids = []
    for genre in genres:
        genre_id, df_genre = get_or_create_id(df_genre, 'name', genre)
        genre_ids.append(genre_id)

    # Get method of acquisition ID
    method_id, df_method_of_acquisition = get_or_create_acquisition_method_id(
        df_method_of_acquisition,
        row['steam_purchase'],
        row['received_for_free'],
        row['written_during_early_access']
    )

    # Playtime Table - Create a new row for df_playtime
    new_playtime_row = {
        'author_playtime_id': index + 1,
        'author_id': author_id,
        'game_id': game_id,
        'developer_id': developer_ids[0] if developer_ids else None,
        'category_id': category_ids[0] if category_ids else None,
        'genre_id': genre_ids[0] if genre_ids else None,
        'method_of_acquisition_id': method_id,
        'playtime_forever': row['author_playtime_forever'],
        'playtime_last_two_weeks': row['author_playtime_last_two_weeks'],
        'playtime_at_review': row['author_playtime_at_review'],
        'last_played': row['author_last_played'],
        'num_games_owned': row['author_num_games_owned'],
        'num_reviews': row['author_num_reviews'],
        'playtime_percentage_change': row['playtime_percentage_change'],
        'percentage_played_after_review': row['percentage_played_after_review']
    }

    df_playtime = pd.concat(
        [df_playtime, pd.DataFrame([new_playtime_row])], ignore_index=True)

    iteration_count += 1  # Increment the counter after each iteration

    if iteration_count % 1000 == 0:
        last_batch_time = time.time()  # Mark the time after every 1k iterations

# At the end of the loop, print the time for the remaining iterations if needed
elapsed_time = time.time() - start_time
print(f"Total time for processing {
      iteration_count} rows: {elapsed_time:.2f} seconds.")


# Populate game review fact table and its associated dimensions

# Populate playtime fact table and associated dimensions

start_time = time.time()  # Start the overall timer
iteration_count = 0  # Initialize the iteration counter


# Function to print 10 random rows from a DataFrame

def print_random_rows(df, table_name):
    print(f"\n{table_name}:")
    # Select 10 random rows with reproducible results
    print(df.sample(min(10, len(df)), random_state=42))


# Print 10 random rows from each table
print_random_rows(df_reviews, "Game Review Fact Table")
print_random_rows(df_language, "Dimension Language Table")
print_random_rows(df_date, "Dimension Date Table")
print_random_rows(df_game, "Dimension Game Table")
print_random_rows(df_author, "Dimension Author Table")
print_random_rows(df_developer, "Dimension Developer Table")
print_random_rows(df_genre, "Dimension Genre Table")
print_random_rows(df_category, "Dimension Category Table")
print_random_rows(df_method_of_acquisition,
                  "Dimension Method of Acquisition Table")
print_random_rows(df_playtime, "Playtime Fact Table")
