# powerbi-report-exporter

This script generates and downloads Power BI reports through the Power BI REST Api. Concurrent tasks can be run with concurrent.futures. Script was used for testing purposes. A new directory, downloaded_reports, is created for the PDFs which are downloaded to test that the script works as it shoulds.

## ids.txt and business_ids.csv

The script uses local files ids.txt for client_id, client_secret, tenant_id, group(workspace)_id, report_id and bearer token. The txt-file is formatted as follows:
client_id,val
client_secret,val
tenant_id,val
group_id_dev,val
report_id_pdf_dev,val
bearer,val,expiry as datetime

and business_ids.csv as follows:
123
123k
321
321k

where newline is used as separator and "k" used to distinguish between not-concern and concern.

## Installing libraries and running the script

requirements.txt has needed libraries and their versions, though requests is the only one not part of Python standard library. This can be installed by running "pip install -r requirements.txt" on terminal. Script can be run with the command python report-exporter.py.