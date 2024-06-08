# Databricks notebook source
import bs4
import datetime
import logging
import os
from pyspark.sql.functions import lit
from pyspark.sql.types import Row, _parse_datatype_string
import requests
import time

from utils.constants import *
from utils.utils import *

# COMMAND ----------

# set up logging
logging.basicConfig(level=logging.INFO,
                    format = '%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s')
log = logging.getLogger(__name__)

# COMMAND ----------

def main(source):
    """
    Main process for Czech Real Estate extraction
    """
    logging.info(f"Started Main Process")
    # get paths from source
    paths = get_paths(source)
    # iterate paths
    contentlist = []
    for path in paths:
        # extract raw text content from path
        raw_data = raw_extraction(path)
        # extract html elements from text
        trusted_data = trusted_extraction(raw_data, path)      
        # instantiate content object
        contents = content(path, raw_data, trusted_data)
        # append content to content list
        contentlist.append(contents)
    # ingest raw content
    ingestion_raw(contentlist, targettable_raw)
    # ingest trusted content
    ingestion_trusted(contentlist, targettable_trusted)
    # create consumption view
    create_consumption_view(targettable_trusted, targetview_consumption)
    logging.info(f"Completed Main Process")    

def get_paths(source):
    """
    retrieves the content paths from dbruntime.dbutils.FileInfo object
    Args:
        source (str): dbruntime.dbutils.FileInfo
    Returns:
        paths (list<str>): list containing string paths 
    """
    # extract path elements using list comprehension
    paths = [file.path for file in dbutils.fs.ls(source)]
    logging.info(f"- extracted {str(len(paths))} paths from {source}")
    return paths

def raw_extraction(path):
    """
    Extracts raw html content from source url
    Args:
        url(str): url of the source content
        targettable (str): write destination of the target table
        targetschmea (str): optional parameter to define default schema
    """
    content = {}
    # add metadata to params    
    content = add_metadata_to_dict(content, path)
    # check if path is url
    if path.startswith('http'):
        # call url extraction method
        content['html_content'] = extract_from_url(path)
        logging.info(f"- collected raw for {path=}")
        return content
    # check if path is location in filestore
    elif path.startswith('dbfs'):
        # call filestore extraction method
        content['html_content'] = extract_from_filestore(path)
        logging.info(f"- collected raw for {path=}")
        return content
    # handle unexpected path
    else:
        raise ValueError(f"unexpected path {path=}")

def trusted_extraction(content, path):
    """
    extracts data from html content
    Args:
        content (str): html content string
    Returns:
        params (dict<string>): dict containing datapoints extracted from html
    """
    # instantiate params dict
    params = {}
    # add metadata to params
    params = add_metadata_to_dict(params, path)    

    # convert html to a bs4 object
    soup = bs4.BeautifulSoup(content.get('html_content'),'html.parser')

    # find norm price
    norm_price = soup.find('span', class_='norm-price ng-binding').get_text(strip=True)
    # add key-value pair to params dict
    params['norm_price'] = norm_price

    # find location
    location_text = soup.find('span', class_='location-text ng-binding').get_text(strip=True)
    # add key-value pair to params dict
    params['location_text'] = location_text

    # isolate params1
    params1 = soup.find('ul', 'params1')
    # iterate through params1 elements
    for item in params1.find_all('li', class_= 'param'):
        # extract the key and value of param1 elements
        param1_key = item.find('label').get_text().lower().replace(' ','_').replace(':','')
        # clean the special characters out of the keys
        param1_key = ''.join(czech_mapping.get(char, char) for char in param1_key)
        param1_val = item.find('span').get_text()
        # add key-value pair to params dict
        params[param1_key] = param1_val

    # isolate params2
    params2 = soup.find('ul', 'params2')
    # iterate through params2 elements
    for item in params2.find_all('li', class_= 'param'):
        # extract the key and value of param2 elements    
        param2_key = item.find('label').get_text().lower().replace(' ','_').replace(':','')
        # clean the special characters out of the keys
        param2_key = ''.join(czech_mapping.get(char, char) for char in param2_key)        
        param2_val = item.find('span').get_text()
        # add key-value pair to params dict
        params[param2_key] = param2_val    
    logging.info(f"- collected trusted for {path=}")  
    return params

def ingestion_raw(content, targettable, targetschema = 'default', writemode = 'overwrite'):
    """
    writes raw html content to target table
    Args:
        source (str): source path of the content
        content (str): raw html content from the webpage
        targettable (str): write destination of the raw content
        targetschema (str): optional schema destination of the raw content
        writemode (str): dataframe write mode, defaults to overwrite
    """
    content = [con.raw for con in content]
    # create pyspark DateFrame from content
    sparkdf = spark.createDataFrame(content)
    logging.info(f"wrote raw content to sparkdf")
    # write content to Table
    sparkdf.write.format("delta").mode(writemode).option('mergeSchema', 'true').saveAsTable(f"{targetschema}.{targettable}")
    logging.info(f"wrote sparkdf to {targetschema}.{targettable}") 

def ingestion_trusted(content, targettable, targetschema = 'default', writemode ='overwrite'):
    """
    writes trusted html content to target table
    Args:
        content (str): trusted html content from the webpage
        targettable (str): write destination of the raw content
        targetschema (str): optional schema destination of the raw content
        writemode (str): dataframe write mode, defaults to overwrite
    """
    content = [con.trusted for con in content]
    # create pyspark DateFrame from content
    sparkdf = spark.createDataFrame(content)
    logging.info(f"wrote trusted content to sparkdf")
    # write content to Table
    sparkdf.write.format("delta").mode(writemode).option('mergeSchema', 'true').saveAsTable(f"{targetschema}.{targettable}")
    logging.info(f"wrote sparkdf to {targetschema}.{targettable}")

def create_consumption_view(sourcetable, targettable, sourceschema = 'default', targetschema = 'default'):
    """
    Creates consumption view for targettable_trusted
    Args:
        sourcetable (str): source table to create view from
        targettable (str): target view to write the data to
        sourceschema (str): optional parameter to define schema
        targetschema (str): optional parameter to define schema
    """
    # collect the table schema
    columns = spark.sql("select * from prod_cz_re_trusted limit 1").schema.names
    # sort the columns by hierarchy and then alphabetically
    columns = sort_by_hierarchy_first(columns, colhierarchy)
    # create a view from the trusted table
    spark.sql(f"""create or replace view {targetschema}.{targettable}
                  as (
                      select {', '.join(columns)}
                      from {sourceschema}.{sourcetable}
                      )
               """)
    logging.info(f"created view {targetschema}.{targettable}")

# COMMAND ----------

if __name__ == '__main__':
    main(source)

