## Czech Real Estate Asessment

Extracts out Czech real estate data from html content

[main.py](main.py) - performs etl of source data. Source data can be list of urls or directory in dbfs file path. The script iterates over the path and first writes the raw content to sql table, then extracts out the parameters defined in asessment

--------------------
#### Workflow


![workflow diagram](/img/cz_workflow_diagram.drawio.png)

main.py was designed to run on a databricks workflow, due to limitations of the community edition we are unable to design workflows and job, however main.py was designed to process once per day, in this case you can easily change dataframe write parameters from overwrite to append for daily scheduling.

--------------------
#### Content

writes the following content
* default.prod_cz_re_raw: holds the raw html content in case further extraction is required as well as ingestion metadata
  - html_content (string): html content stored as text
  - filetimestamp (date): ingestion date
  - source_id (string): identifier extracted from url
  - source_path (string): url or path of the data source  
  - source_rundate (timestamp): run timestamp

* default.prod_cz_re_trusted: holds the extracted html content from czech real estate websites. Czech characters are normalised to mapping fiel contained in utils.constants. If a new parameters appears the new schema merges with the existing one.
  - energetitska_narochnost_budovi (string): 
  - filetimestamp (date): ingestion date
  - id_zukazki (string): 
  - location_text (string): property location
  - norm_price (string): property price
  - plotshu_podluhova (string): 
  - podluzhee (string): 
  - source_id (string): identifier extracted from url
  - source_path (string): url or path of the data source
  - source_rundate (timestamp): run timestamp
  - stuv_obyektu (string): 
  - stuvbu (string): 
  - topenee (string): 
  - tselkova_tsenu (string): 
  - uktuulizutse (string): 
  - uzhitna_plotshu (string): 
  - vlustnitstvee (string): 
  - dopruvu (string): 
  - elektrzinu (string): 
  - odpud (string): 
  - purkovanee (string): 
  - telekomunikutse (string): 
  - umeestyenee_obyektu (string): 
  - veetuh (string): 
  - vibuvenee (string): 
  - vodu (string): 
  - bezburiairovee (string): 
  - bulkoon (string): 
  - id (string): 
  - komunikutse (string): 
  - tip_bitu (string): 
  - plin (string): 
  - poznamku_k_tsenye (string): 
  - sklep (string): 
  - gurazh (string): 
  - przevod_do_ov (string): 
  - unuitu (string): 
  - rok_koluudutse (string): 
  - dutum_nustyehovanee (string): 
  - rok_rekonstruktse (string): 
  - nakludi_nu_bidlenee (string):

* default.vw_cz_re_consumption: a view of the trusted layer with a ranked column hierarchy


Created by: Barren-d  
Last update: 2024-06-08
