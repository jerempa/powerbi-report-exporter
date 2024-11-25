import requests
import time
import concurrent.futures
import logging
from datetime import datetime, timedelta
import os
import csv
from PyPDF2 import PdfWriter
import io

logging.basicConfig(level=logging.INFO,
                    format='%(message)s')



class ReportExporter:
    """
    A class to export reports from Power BI.

    Attributes
    ----------
    bearer : str
        The bearer token for authentication.
    group_id : str
        The group ID for the Power BI workspace.
    report_id : str
        The report ID for the Power BI report.
    """
    
    def __init__(self, bearer, group_id, report_id):
        """
        Initializes the ReportExporter with the bearer token, group ID, and report ID.

        Parameters
        ----------
        bearer : str
            The bearer token for authentication.
        group_id : str
            The group ID for the Power BI workspace.
        report_id : str
            The report ID for the Power BI report.
        """
        self.bearer = bearer
        self.group_id = group_id
        self.report_id = report_id
        self.num_of_export_jobs_done = 0
        self.file1 = None
        self.file2 = None
        self.file3 = None

    def get_export_id(self, business_id: str, language: str, page_filt: list, batch_num: int) -> str | None:
        """
        Requests an export ID for a report.

        Parameters
        ----------
        business_id : str
            The business ID to filter the report.
        language : str
            The language for the report At the moment only affects the thousands separator (space or comma) but later also the texts.

        batch_num : int:
            The pages to export (0 = first 5, 1 = following 5 and so on)
        Returns
        -------
        str | None
            The export ID if the request was successful, otherwise None.
        """
        url = f'https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{self.report_id}/exportTo'

        company_filter = f"CompanyBasicInfo/business_id_k eq '{business_id}'"


        body = {
            "format": "PDF",
            "exportReportSettings": {"locale": language},
            "powerBIReportConfiguration": {
                "reportLevelFilters": [{"filter": company_filter}],
                "pages": page_filt
            }
        }

        headers = {"Authorization": f'Bearer {self.bearer}'}

        response = requests.post(url, json=body, headers=headers)
        if response.raise_for_status():
            logging.info(response.raise_for_status())

        res_json = response.json()
        if response.status_code == 202 and batch_num >= 0:
            self.generate_report(res_json["id"], business_id, language, batch_num)
            return res_json["id"]
        else:
            return None

    def generate_report(self, export_id: str, business_id: str, language: str, batch_num: int) -> None:
        """
        Polls the report generation status and downloads the report when it's ready.

        Parameters
        ----------
        export_id : str
            The export ID for the report. NOTE: the export id is valid for 24 hours, so the reports could be generated once a day and the website could have the download request.
        business_id : str
            The business ID used for filtering the report.
        language : str
            The language setting for the report.
        batch_num : int:
            The pages to export (0 = first 5, 1 = following 5 and so on)

        Returns
        -------
        None
        """
        url = f'https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{self.report_id}/exports/{export_id}'

        headers = {"Authorization": f'Bearer {self.bearer}'}

        start = time.time()
        logging.info("Generation started")
        while True:
            response = requests.get(url, headers=headers)
            if response.raise_for_status():
                logging.info(response.raise_for_status())

            res_json = response.json()
            if res_json["status"] == "Succeeded":
                end = time.time()
                logging.info(f'Report generation took: {end - start:.2f} seconds')
                self.download_report(export_id, business_id, language, batch_num)
                break
            time.sleep(5)  # Polling the generation to avoid unnecessary amount of requests

    def download_report(self, export_id: str, business_id: str, language: str, batch_num: int) -> None:
        """
        Downloads the generated report.

        Parameters
        ----------
        export_id : str
            The export ID for the report.
        business_id : str
            The business ID used for filtering the report.
        language : str
            The language setting for the report.
        batch_num : int:
            The pages to export (0 = first 5, 1 = following 5 and so on)

        Returns
        -------
        None
        """
        self.num_of_export_jobs_done += 1
        url = f'https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/reports/{self.report_id}/exports/{export_id}/file'

        headers = {"Authorization": f'Bearer {self.bearer}'}
        response = requests.get(url, headers=headers)
        if response.raise_for_status():
            logging.info(response.raise_for_status())
        if batch_num == 0:
            self.file1 = response.content
        elif batch_num == 1:
            self.file2 = response.content
        elif batch_num == 2:
            self.file3 = response.content

        if self.num_of_export_jobs_done == 3:
            merger = PdfWriter()
            for pdf in [io.BytesIO(self.file1), io.BytesIO(self.file2), io.BytesIO(self.file3)]:
                merger.append(pdf)
            if os.path.basename(os.getcwd()) != "downloaded_reports":
                try:
                    os.chdir(f'{os.getcwd()}/downloaded_reports')
                except FileNotFoundError:
                    os.makedirs("downloaded_reports")
                    os.chdir(f'{os.getcwd()}/downloaded_reports') #Creating a separate directory for the downloaded PDFs

            with open(f'{business_id}_{language}_pagetestts.pdf', 'wb') as output:
                merger.write(output)
            merger.close()

def main():
    id_dict = retrieve_ids(False)
    
    group_id_dev = id_dict.get("group_id_dev")
    report_id_pdf_dev = id_dict.get("report_id_pdf_dev")
    bearer = id_dict.get("bearer")

    business_ids = retrieve_business_ids()
    business_id = business_ids[0].get("business_id")
    lang = business_ids[0].get("language")

    exporter = ReportExporter(bearer, group_id_dev, report_id_pdf_dev)
    pages_to_exports = [[{"pageName": "ReportSectionffd94e439ad791240782"}, {"pageName": "ReportSection377cec962b14e9482a43"},
                {"pageName": "ReportSection8ef5f434cbd0c2a8267a"}, {"pageName": "ReportSection4827d0d49fb7df54c2ec"},
                {"pageName": "ReportSection0e52bbd7623b2347c0a4"}],
                  
                [{"pageName": "ReportSection9ef8b7919024d83e7895"}, {"pageName": "ReportSection2a29d2db992103942906"},
                {"pageName": "ReportSection21486389cbbc18b62387"}, {"pageName": "ReportSection99fc2082b25e31558847"},
                {"pageName": "ReportSection5a86149b9db5ec087cf6"}]
                ,
                [{"pageName": "ReportSection1464ad75bf04ab83fd67"}, {"pageName": "0e6dda969d261c4f39cd"},
                {"pageName": "ReportSection9009cb97d4c7c656e884"}, {"pageName": "ReportSection0f8615045ea9054a7a01"},
                {"pageName": "ReportSection342ba822d4199cab3d30"}]
                ]
    

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for index, page_filt in enumerate(pages_to_exports):
            futures.append(executor.submit(exporter.get_export_id, business_id, lang, page_filt, index))
        concurrent.futures.wait(futures) #goes through the pages in three separate batches

    logging.info("All PDFs exported")

def create_bearer(tenant_id: str, client_id: str, client_secret: str) -> None:
    """
    Creates a bearer token for authentication (if the one in the txt-file has expired).

    Parameters
    ----------
    tenant_id : str
        The tenant ID for the Azure AD.
    client_id : str
        The client ID for the application.
    client_secret : str
        The client secret for the application.

    Returns
    -------
    None
    """
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default"
    }
    response = requests.post(url, data=body)
    res = response.json()

    id_dict = bearer_to_file(res["access_token"])
    return id_dict

def bearer_to_file(bearer: str) -> None:
    """
    Saves the bearer token to a file with an expiration time.

    Parameters
    ----------
    bearer : str
        The bearer token for authentication.

    Returns
    -------
    None
    """
    now = datetime.now()
    expiry = now + timedelta(hours=1)
    id_dict = retrieve_ids(True)
    with open("ids.txt", "w") as f:
        for key, value in id_dict.items():
            if key == "bearer":
                f.write(f"bearer,{bearer},{expiry}\n")
            else:
                f.write(f"{key},{value}\n")

    id_dict = retrieve_ids(True) #updating the dict with the newly written bearer
    return id_dict
                

def retrieve_ids(skip_check: bool) -> dict:
    """
    Retrieves IDs and bearer token from a file.

    Parameters
    ----------
    skip_check : bool
        If True, skips the bearer token expiration check to avoid infinte loop.

    Returns
    -------
    dict
        A dictionary containing the retrieved IDs and bearer token.
    """
    id_dict = {}
    bearer_expiry = None
    with open("ids.txt", "r") as f:
        client_id = id_dict.get("client_id")
        client_secret = id_dict.get("client_secret")
        tenant_id = id_dict.get("tenant_id")
        for line in f:
            line = line.strip("\n")
            line = line.split(",")
            line_str = line[0].strip()
            line_id = line[1].strip()
            id_dict[line_str] = line_id
            if line_str == "bearer":
                bearer_expiry = datetime.strptime(line[2], "%Y-%m-%d %H:%M:%S.%f")


    if not skip_check: 
        now = datetime.now()
        if now > bearer_expiry: #If time now is larger than the expiry in txt then the bearer has expired
            tenant_id = id_dict.get("tenant_id")
            client_id = id_dict.get("client_id")
            client_secret = id_dict.get("client_secret")
            id_dict = create_bearer(tenant_id, client_id, client_secret)
    
    return id_dict

def retrieve_business_ids() -> list[dict]:
    """
    Retrieves business IDs from a CSV file, used for testing.

    Returns
    -------
    list
        A list of dictionaries containing business IDs and language settings.
    """
    business_id_list = []
    with open('business_ids.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\n')
        for row in reader:
            id = ', '.join(row).strip()
            business_id_list.append({"business_id": id, "language": "fi-FI"}) #Only Finnish reports are now used
    return business_id_list

if __name__ == "__main__":
    main()
