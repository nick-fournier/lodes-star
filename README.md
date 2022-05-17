# LODES-Star

LODES-Star is a handy python wrapper for fetching LODES tables from the US Census' https FTP server. The primary function is `load_lodes()` which has the following inputs

- state - State abbreviation (e.g., "CA" for California) [string] (No default, required field)
- zone_types - Data zone type (Default='OD'):
  - "OD" - Origin-Destination data as home and work location by Census Block
  - "RAC" - Residence Area Characteristics (i.e., origins) by Census Block
  - "WAC" - Work Area Characteristics (i.e., destinations) by Census Block
- job_cat - Job type code [string] (Default='JT00'):
  - "JT00" for All Jobs
  - "JT01" for Primary Jobs
  - "JT02" for All Private Jobs
  - "JT03" for Private Primary Jobs
  - "JT04" for All Federal Jobs
  - "JT05" for Federal Primary Jobs 
- year - Data year [integer] (Default=most recent available)
- cache - Cache LODES file into local disk for quick future loading [bool] (Default=True)
- cache_only - Do not load into memory, download into cache only [bool] (Default=False)
