import os
import random
import requests
import gzip
import shutil
import pandas as pd
from bs4 import BeautifulSoup

# Set the temporary directory for data storage
DATA_DIR = "temp"
# Use the ratings dataset instead of basics:
GZ_FILENAME = "title.ratings.tsv.gz"
TSV_FILENAME = "title.ratings.tsv"
GZ_FILEPATH = os.path.join(DATA_DIR, GZ_FILENAME)
TSV_FILEPATH = os.path.join(DATA_DIR, TSV_FILENAME)
# Ratings dataset URL:
IMDB_RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"

# Internal storage for movie ids that satisfy the min_votes threshold.
_movie_ids = []

def _download_and_extract():
    """Download and extract the IMDb ratings dataset if not already done."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Download the gzip file if it doesn't exist
    if not os.path.exists(GZ_FILEPATH):
        print("Downloading ratings dataset...")
        response = requests.get(IMDB_RATINGS_URL, stream=True)
        response.raise_for_status()  # Ensure we notice bad responses
        with open(GZ_FILEPATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download completed.")
    else:
        print("GZ file already exists. Skipping download.")
    
    # Extract the file if the TSV doesn't exist
    if not os.path.exists(TSV_FILEPATH):
        print("Extracting ratings dataset...")
        with gzip.open(GZ_FILEPATH, "rb") as f_in, open(TSV_FILEPATH, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        print("Extraction completed.")
    else:
        print("TSV file already exists. Skipping extraction.")

def _load_movie_ids(min_votes=0):
    """
    Load movie ids (tconst) from the ratings TSV file into memory.
    Only include movies with numVotes >= min_votes.
    """
    global _movie_ids
    if _movie_ids:
        return

    _download_and_extract()

    print("Loading movie IDs from the ratings dataset...")
    with open(TSV_FILEPATH, "rt", encoding="utf-8") as file:
        # Read header
        header = file.readline().strip().split("\t")
        try:
            tconst_index = header.index("tconst")
            numVotes_index = header.index("numVotes")
        except ValueError:
            raise Exception("Required columns not found in header.")

        # Read movie ids that meet the minimum votes threshold
        for line in file:
            parts = line.strip().split("\t")
            if parts and len(parts) > max(tconst_index, numVotes_index):
                try:
                    votes = int(parts[numVotes_index])
                except ValueError:
                    continue  # Skip if numVotes is not an integer
                if votes >= min_votes:
                    _movie_ids.append(parts[tconst_index])
    print(f"Loaded {len(_movie_ids)} movie IDs with at least {min_votes} votes.")

def get_random_movie_id(min_votes=0):
    """
    Returns a random movie ID (tconst) from the IMDb ratings dataset that meets the min_votes threshold.
    Each ID is only returned once. Raises an Exception when no IDs remain.
    """
    global _movie_ids
    # Load movie ids if not already loaded.
    if not _movie_ids:
        _load_movie_ids(min_votes=min_votes)

    if not _movie_ids:
        raise Exception("No more movie IDs available with the specified min_votes threshold.")

    # Pick and remove a random movie id from the list
    index = random.randrange(len(_movie_ids))
    return _movie_ids.pop(index)

def scrape_worldwide_box_office(movie_id):
    """
    Scrapes the worldwide box office figure from Box Office Mojo for the given movie_id.
    It searches for a span element containing "Worldwide", then gets its parent div and
    looks for any child element with class "money" to extract the figure.
    If the element is not found, an Exception is thrown.
    """
    url = f"https://www.boxofficemojo.com/title/{movie_id}/credits/"
    
    # Get the page content
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve page for movie ID {movie_id} (status code: {response.status_code})")
    
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all span elements that contain the text "Worldwide"
    worldwide_spans = soup.find_all('span', string=lambda text: text and "Worldwide" in text)
    
    # Loop through each span to find the parent div and then search for a child with class "money"
    for span in worldwide_spans:
        parent_div = span.find_parent('div')
        if parent_div:
            money_elem = parent_div.find(class_="money")
            if money_elem:
                # Return the box office figure (e.g., "$47,680,966")
                return money_elem.get_text(strip=True)
    
    # If no matching element is found, throw an error
    raise Exception("Worldwide box office figure not found.")

# Example usage:
# movie_id = "tt28607951"  # Replace with the actual movie ID
# print(scrape_worldwide_box_office(movie_id))

# Example usage of get_random_movie_id:
# To only get movies with at least 1000 votes, call:
# random_movie_id = get_random_movie_id(min_votes=1000)
# print(random_movie_id)
