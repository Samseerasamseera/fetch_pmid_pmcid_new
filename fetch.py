import requests
import time
import random
import pandas as pd
import threading

class PubMedProcessor:
    def __init__(self, molecule, credentials):
        self.molecule = molecule
        self.credentials = credentials
        self.tool = "Gene-ius-pathways"
        self.pmid_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        self.idconv_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
        self.email, self.api_key = self.get_random_credentials()

    def get_random_credentials(self):
        return random.choice(self.credentials)

    def rotate_credentials(self):
        self.email, self.api_key = self.get_random_credentials()

    def search_query_generator(self, molecule):
        return f'"{molecule}"'

    def fetch_all_pmids(self):
        all_pmids = []
        start = 0
        MAX_PMID_LIMIT = 9999  # NCBI hard limit for esearch retstart

        while True:
            params = {
                "db": "pubmed",
                "term": self.search_query_generator(self.molecule),
                "retmode": "json",
                "retmax": 10000,
                "retstart": start,
                "tool": self.tool,
                "email": self.email,
                "api_key": self.api_key
            }

            while True:
                try:
                    response = requests.get(self.pmid_url, params=params)
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            break
                        except Exception as json_error:
                            print(f" JSON error (PMID fetch): {json_error}. Retrying in 60s...")
                            print(f"Response content: {response.text[:300]}")
                            time.sleep(60)
                    else:
                        print(f" Status {response.status_code} (PMID fetch). Retrying in 60s...")
                        time.sleep(60)
                except Exception as e:
                    print(f" Exception (PMID fetch): {e}. Retrying in 60s...")
                    time.sleep(60)

            batch_pmids = data.get("esearchresult", {}).get("idlist", [])
            if not batch_pmids:
                break
            all_pmids.extend(batch_pmids)
            start += len(batch_pmids)

            if start >= MAX_PMID_LIMIT:
                print(f"⚠️ Reached NCBI maximum limit of {MAX_PMID_LIMIT} PMIDs. Stopping further fetch.")
                break

            time.sleep(0.4)

        return all_pmids


    def convert_pmids_to_pmcids_df(self, pmids):
        results = []
        batch_size = 200

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i + batch_size]
            headers = {
                "User-Agent": f"{self.tool}/1.0 (mailto:{self.email})"
            }

            params = {
                "tool": self.tool,
                "email": self.email,
                "api_key": self.api_key,
                "format": "json",
                "ids": ",".join(batch)
            }

            while True:
                try:
                    response = requests.get(self.idconv_url, headers=headers, params=params)
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            record_map = {str(r.get("pmid")): r.get("pmcid") for r in data.get("records", [])}
                            for pmid in batch:
                                results.append({"PMID": pmid, "PMCID": record_map.get(pmid)})
                            break
                        except Exception as json_error:
                            print(f" JSON error (PMCID convert): {json_error}. Retrying in 60s...")
                            print(f"Response content: {response.text[:300]}")
                            time.sleep(60)
                    else:
                        print(f" Status {response.status_code} (PMCID convert). Retrying in 60s...")
                        time.sleep(60)
                except Exception as e:
                    print(f" Exception (PMCID convert): {e}. Retrying in 60s...")
                    time.sleep(60)

            time.sleep(0.4)

        return pd.DataFrame(results)


def run_for_molecule(mol, credentials, index):
    processor = PubMedProcessor(mol, credentials)

    # Rotate credentials after every 3-5 molecules
    if index % random.randint(3, 5) == 0:
        processor.rotate_credentials()
        print(f"🔄 Rotated API key for molecule: {mol}")

    print(f"🔍 Starting: {mol} using {processor.email}")
    pmids = processor.fetch_all_pmids()
    print(f"[{mol}] ✅ PMIDs fetched: {len(pmids)}")

    if pmids:
        df = processor.convert_pmids_to_pmcids_df(pmids)
        df.to_csv(f"{mol}_pmid_to_pmcid.csv", index=False)
        print(f"[{mol}] ✅ Saved to {mol}_pmid_to_pmcid.csv")
    else:
        print(f"[{mol}] ⚠️ No PMIDs found.")

    time.sleep(5)


if __name__ == "__main__":
    credentials = [
        ("ayishanishana.ciods@yenepoya.edu.in", "913e27e3607a8e7ea9655beb8c7e2efddb09"),
        ("alimathsambreena.ciods@yenepoya.edu.in", "5a3295e8aaafcd066e0f0899341104a21808"),
        ("24665@yenepoya.edu.in", "389d936615c9ddebb876e256a7dd6b1b8f09"),
        ("samshisam87@gmail.com", "2d95e759a9ae1dd725248f9ab5bd6b3b2d09"),
        ("prathikciods@gmail.com", "f668ec9336ed118d5012431cc1eee99fdb08"),
        ("prathikbs.ciods@yenepoya.edu.in", "70a470634f876fad7fa8d9143913258b3e08"),
    ]

    molecules = ["IL19", "IL24", "axl", "camkk1", "taf6", "chek1", "egfr", "pak1", "cdk", "rnpep", "dstn", "fgfr", "aak1"]

    threads = []
    for idx, mol in enumerate(molecules):
        t = threading.Thread(target=run_for_molecule, args=(mol, credentials, idx))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
