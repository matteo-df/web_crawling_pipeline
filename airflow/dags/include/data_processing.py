from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
import logging
import os
from dotenv import load_dotenv
from pyspark.sql.types import StringType

load_dotenv()

logger = logging.getLogger(__name__)

import requests
import dns.resolver
import pycountry

def get_ip_for_domain(domain):
  try:
    result = dns.resolver.resolve(domain, 'A')
    return result[0].to_text()
  except Exception as e:
    print(f'Error resolving domain: {e}')
    return None


def get_country_for_ip(ip):
  try:
    response = requests.get(f'https://ipinfo.io/{ip}/json')
    data = response.json()
    country_code = data.get('country')
    country = pycountry.countries.get(alpha_2=country_code)
    return country.name if country else 'Unknown'
  except Exception as e:
    return f'Error retrieving information: {e}'


def get_country_for_domain(domain):
  ip = get_ip_for_domain(domain)
  if ip:
    return get_country_for_ip(ip)
  return None

work_dir = '/opt/airflow/dags'

# Initialize a Spark session to connect to the cluster
spark = SparkSession.builder \
    .appName("SparkOnDockerCluster") \
    .getOrCreate()

# Load data from PostgreSQL or perform operations
url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('WARC_POSTGRES_DB')}"
properties = {
    "user": os.getenv('WARC_POSTGRES_USER'),
    "password": os.getenv('WARC_POSTGRES_PASSWORD'),
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=url, table="web_crawling_urls", properties=properties)

# Flag if url is home_page
df = df\
    .withColumn('primary_link', F.split((F.split(F.col('url'), '://')[1]), '/')[0])\
    .withColumn('is_homepage', F.size(F.split((F.split(F.col('url'), '://')[1]), '/'))==1)

# Save enriched data as parquet
df.write.format('parquet').mode('overwrite').save(f'{work_dir}/data/silver_web_urls')

# Aggregate by primary links and compute their frequency
grouped_df = df.groupBy('primary_link').agg(F.count('*').alias('frequency'))

# column that specifies the country of the website
# based on top-level-domain mapping stored in a csv file
cctlds_df = spark.read.option('header', True).csv(f'{work_dir}/data/url_utils/cctlds_mapping.csv')
#
country_from_cctld_df = grouped_df\
    .withColumn('tld', F.concat(F.lit('.'), F.element_at(F.split(F.col('primary_link'),'[\.]'), -1)))\
    .join(cctlds_df, on='tld', how='left')\
    .drop('tld')

get_country_udf = F.udf(get_country_for_domain, StringType())
# if the tld doesn't map a country, an api call is used
country_from_api_df = country_from_cctld_df\
  .where('country is null')\
  .withColumn("country", get_country_udf(F.col('primary_link')))

grouped_df = country_from_cctld_df.where('country is not null').unionByName(country_from_api_df)


# column that categorizes the type of content hosted by the website
# based on URL Classification Dataset [DMOZ] at https://www.kaggle.com/datasets/shawon10/url-classification-dataset-dmoz/
category_df = spark \
    .read\
    .option('header', True)\
    .csv(f'{work_dir}/data/url_utils//URL_Classification.csv')\
    .withColumn('primary_link', F.split((F.split(F.col('url'), '://')[1]), '/')[0])\
    .where(F.col('primary_link').isNotNull())\
    .withColumn('rn', F.row_number().over(Window.partitionBy("primary_link").orderBy(F.length(F.col('url')))))\
    .where("rn=1")\
    .drop('rn')

url_categorized_df = grouped_df\
    .join(category_df, on='primary_link', how='left')

# Flag that indicates if the website is an ad-based domain
# based on Adaway Host list of Ad Websites at https://adaway.org/hosts.txt
adaway_df = spark\
    .read\
    .csv(f'{work_dir}/data/url_utils/adaway.txt')\
    .filter(~(F.substring(F.col('_c0'),1,1)==F.lit('#')))\
    .select(F.split(F.col('_c0'), ' ')[1].alias('ad-based-domain'))\
    .distinct()

url_category_ad_df = url_categorized_df\
    .where(F.col('category').isNull())\
    .join(adaway_df, on=F.col('primary_link')==F.col('ad-based-domain'), how='left')\
    .withColumn('is_ad', F.col('ad-based-domain').isNotNull())\
    .selectExpr('primary_link', 'country','frequency', 'category', 'is_ad')\
    .union(
        url_categorized_df\
            .where(F.col('category').isNotNull())\
            .withColumn('is_ad', F.lit(False))\
            .selectExpr('primary_link', 'country','frequency', 'category', 'is_ad')
    )

# Save the aggregated df in a gold parquet table 
url_category_ad_df.write.format('parquet').mode('overwrite').partitionBy('country').save(f'{work_dir}/data/gold_urls_categorized')


# Stop the session when done
spark.stop()