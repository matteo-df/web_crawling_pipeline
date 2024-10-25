from bs4 import BeautifulSoup
import re
from warcio.archiveiterator import ArchiveIterator
import psycopg
from dotenv import load_dotenv
import os
import csv
import logging

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

work_dir='/opt/airflow/dags'


def list_segments(file_type):
  directory = os.path.join(work_dir, 'data', file_type)
  logger.info(directory)
  try:
    # Get a list of all items in the directory
    items = os.listdir(directory)

    segments = [item.split('=')[-1] for item in items if os.path.isdir(os.path.join(directory, item))]
    logger.info(segments)
    return segments
  except FileNotFoundError:
    logger.error(f"Error: The directory '{directory}' does not exist.")
    return None

def list_segment_files(segment, file_type):
  directory = os.path.join(work_dir, 'data', file_type, f'segment={segment}')

  try:
    # Get a list of all items in the directory
    items = os.listdir(directory)

    files = [os.path.join(directory, item) for item in items if os.path.isfile(os.path.join(directory, item))]

    return files

  except FileNotFoundError:
    return f"Error: The directory '{directory}' does not exist."


def split_files(files_list, n_chunks):
  length = len(files_list)

  n_chunks = min(n_chunks, length)

  if n_chunks >0:
    # Calculate the base size of each chunk
    chunk_size = length // n_chunks
    remainder = length % n_chunks

    result = []
    start = 0

    for i in range(n_chunks):
      # If remainder > 0, we add an extra element to this chunk
      end = start + chunk_size + (1 if i < remainder else 0)
      result.append(files_list[start:end])
      start = end

    return result


def save_warc_file_names_as_csv(file_type='warc'):
  logger.info(work_dir)
  segments = list_segments(file_type)
  file_names = []
  for s in segments:
    file_names += (list_segment_files(s, file_type))
  logger.info(str(file_names))
  # Save the array as a CSV file
  csv_path = os.path.join(work_dir, 'data', 'warc_file_names.csv')
  assert(len(file_names)>0, 'Csv is empty')
  with open(csv_path, mode='w') as file:
    writer = csv.writer(file)
    writer.writerows([[file_name] for file_name in sorted(file_names)])
  logger.info(f'Csv saved in {csv_path}')


def get_warc_file_chunks(n_chunks):
  files = []
  try:
    csv_path = os.path.join(work_dir, 'data', 'warc_file_names.csv')
    with open(csv_path, mode='r') as file:
      reader = csv.reader(file)

      # Iterate over each row in the CSV
      for row in reader:
        files+= row

  except FileNotFoundError:
    return f"Error: The file 'warc_file_names.csv' does not exist."
  return split_files(files, n_chunks)


# Function to extract external links from a WARC file and store them in a set
def extract_urls_from_warc(warc_file):
    with open(warc_file, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                content = record.content_stream().read()
                soup = BeautifulSoup(content, 'html.parser')
                links = soup.find_all('a', href=True)
                external_links = {link['href'] for link in links if re.match(r'^https?://', link['href'])}
                if external_links:
                  return external_links


# Function to connect to PostgreSQL using credentials from .env
def connect_to_postgres():
  try:
    conn = psycopg.connect(
      host=os.getenv('POSTGRES_HOST'),
      port=os.getenv('POSTGRES_PORT'),
      dbname=os.getenv('WARC_POSTGRES_DB'),
      user=os.getenv('WARC_POSTGRES_USER'),
      password=os.getenv('WARC_POSTGRES_PASSWORD')
    )
    logging.info('Connection to Postgres: OK')
    return conn
  except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    return None


# Function to save a set of URLs to the PostgreSQL database
def save_urls_to_db(conn, url_set, warc_file):
  try:
    with conn.cursor() as cur:
      # Insert each URL from the set into the database
      for url in url_set:
        cur.execute(
          "INSERT INTO web_crawling_urls (url) VALUES (%s) ON CONFLICT DO NOTHING",
          (url,)
        )
    conn.commit()
    logging.info(f"Inserted {len(url_set)} URLs from {warc_file}")
  except Exception as e:
    logging.error(f"Error inserting URLs: {e}")


def process_warc_file(warc_chunk):
    conn = connect_to_postgres()  # Establish connection to PostgreSQL
    if conn:
      try:
        for warc_file in warc_chunk:
          print(f"Processing file: {warc_file}")
          url_set = extract_urls_from_warc(warc_file)  # Extract URLs into a set
          if url_set:
            save_urls_to_db(conn, url_set, warc_file)  # Save unique URLs to the database
          with open(os.path.join(work_dir, 'data', 'processed_warc_file.txt'), "a") as file:
            file.write(warc_file)
            file.write('\n')
          # os.remove(warc_file)

      finally:
        conn.close()

def main():
  save_warc_file_names_as_csv()
  print(get_warc_file_chunks(4))

if __name__ == "__main__":
  main()