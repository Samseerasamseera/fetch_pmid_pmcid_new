import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
from typing import List
import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError


class PMCXMLEDownloaderSync:
    def __init__(
        self,
        email: str,
        api_key: str,
        pmcids: List[str],
        aws_access_key_id: str,
        aws_secret_access_key: str,
        s3_bucket_name: str = "geneius-pathway-data",
        s3_prefix: str = "pmc_xml_13/",
        batch_size: int = 100,
        max_retries: int = 3,
        retry_delay: int = 60, 
    ):
        self.email = email
        self.api_key = api_key
        self.pmcids = pmcids
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.s3_bucket_name = s3_bucket_name
        self.s3_prefix = s3_prefix
        self.results = []
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def _upload_to_s3(self, pmcid: str, xml_content: str):
        key = f"{self.s3_prefix}{pmcid}.xml"
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket_name,
                Key=key,
                Body=xml_content.encode("utf-8"),
                ContentType="application/xml",
            )
            print(f" Uploaded {pmcid}.xml to s3://{self.s3_bucket_name}/{key}")
            self.results.append({"pmcid": pmcid, "saved": "Yes", "error": ""})
        except (BotoCoreError, NoCredentialsError) as e:
            print(f" Failed to upload {pmcid}.xml to S3: {e}")
            self.results.append({"pmcid": pmcid, "saved": "No", "error": str(e)})

    def _download_xml_batch(self, batch: List[str], batch_num: int):
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        params = {
            "db": "pmc",
            "id": ",".join(batch),
            "retmode": "xml",
            "tool": "BulkDownloader",
            "email": self.email,
            "api_key": self.api_key,
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=60)
                if response.status_code == 200:
                    xml_text = response.text
                    root = ET.fromstring(xml_text)
                    articles = root.findall(".//article")
                    for i, article in enumerate(articles):
                        pmcid = batch[i]
                        article_xml = ET.tostring(article, encoding="unicode")
                        self._upload_to_s3(pmcid, article_xml)
                    print(f" Batch {batch_num}: {len(articles)} articles uploaded.")
                    return
                else:
                    print(f" Batch {batch_num} HTTP error: {response.status_code}")
            except Exception as e:
                print(f" Attempt {attempt} for batch {batch_num} failed: {e}")

            if attempt < self.max_retries:
                print(f" Retrying batch {batch_num} in {self.retry_delay}s...")
                time.sleep(self.retry_delay)
            else:
                print(f" Batch {batch_num} permanently failed after {self.max_retries} attempts.")
                for pmcid in batch:
                    self.results.append({"pmcid": pmcid, "saved": "No", "error": "Batch failed after retries"})

    def run(self):
        start_time = time.time()
        batches = [
            self.pmcids[i: i + self.batch_size]
            for i in range(0, len(self.pmcids), self.batch_size)
        ]
        for i, batch in enumerate(batches):
            self._download_xml_batch(batch, i + 1)

        duration = time.time() - start_time
        print(f"\n Task completed in {duration:.2f} seconds for {len(self.pmcids)} PMCIDs.")
        return self.results


# ======================
# main
# ======================
if __name__ == "__main__":
    df = pd.read_csv(r"C:\Users\User\Downloads\PMC_DATA.csv")
    pmcids = df["pmc_id"].dropna().astype(str).str.strip().tolist()

    downloader = PMCXMLEDownloaderSync(
    email=" ",
    api_key=" ",
    pmcids=pmcids,
    aws_access_key_id=" ",
    aws_secret_access_key=" ",
    s3_bucket_name="geneius-pathway-data",
    s3_prefix="pmc_xml_8/",
    batch_size=100,
    max_retries=3,
    retry_delay=60,  
)

    results = downloader.run()

    summary_df = pd.DataFrame(results)
    summary_df.to_csv("pmc_upload_summary.csv", index=False)
    print("\n Summary saved to pmc_upload_summary.csv")
