import ssl
import time
import pandas as pd
from Bio import Entrez
from langdetect import detect, DetectorFactory

# Bypassing SSL errors on Windows
ssl._create_default_https_context = ssl._create_unverified_context
DetectorFactory.seed = 0

Entrez.email = "x@estudiant.upf.edu"  # my email
Entrez.tool = "NLPfinalproject"

def get_spanish_abstracts(limit=10000):
   
    query = 'Spanish[Language] AND ("2010"[Date - Publication] : "2021"[Date - Publication]) AND hasabstract[Filter]'
   
    print(f"Searching PubMed for: {query}")
    try:
        search_handle = Entrez.esearch(db="pubmed", term=query, retmax=limit)
        search_results = Entrez.read(search_handle)
        search_handle.close()
        id_list = search_results["IdList"]
    except Exception as e:
        print(f"Search failed: {e}. Try running it again in a minute.")
        return pd.DataFrame()

    print(f"Found {len(id_list)} IDs. Waiting 2 seconds for server stability...")
    time.sleep(2) 
    
    print("Fetching and filtering content (this may take a minute)...")
    
    # Process in batches of 500 to prevent server timeouts
    final_data = []
    batch_size = 500
    
    for i in range(0, len(id_list), batch_size):
        batch_ids = id_list[i : i + batch_size]
        try:
            fetch_handle = Entrez.efetch(db="pubmed", id=",".join(batch_ids), rettype="xml", retmode="xml")
            records = Entrez.read(fetch_handle)
            fetch_handle.close()

            for article in records['PubmedArticle']:
                try:
                    abstract_list = article['MedlineCitation']['Article']['Abstract']['AbstractText']
                    full_text = " ".join([str(part) for part in abstract_list])
                    full_text = " ".join(full_text.split())
                    
                    # Ensure it is Spanish
                    if detect(full_text) != 'es':
                        continue 
                    
                    word_list = full_text.split()
                    if len(word_list) < 40: # less than 40 words out
                        continue
                    
                    midpoint = len(word_list) // 2
                    first_half = " ".join(word_list[:midpoint])
                    second_half = " ".join(word_list[midpoint:])
                    
                    final_data.append({
                        "pmid": str(article['MedlineCitation']['PMID']),
                        "first_half": first_half,
                        "second_half_gold": second_half,
                        "word_count": len(word_list)
                    })
                except:
                    continue
            
            print(f"Progress: Processed {i + len(batch_ids)}/{len(id_list)} IDs...")
            time.sleep(1) 

        except Exception as e:
            print(f"Batch failed, skipping to next: {e}")
            continue

    return pd.DataFrame(final_data)

# saving data
df = get_spanish_abstracts(limit=10000)
if not df.empty:
    df.to_csv("spanish_abstracts.csv", index=False, encoding="utf-8-sig")
    print(f"Success! Saved {len(df)} Spanish abstracts to 'spanish_abstracts.csv'")