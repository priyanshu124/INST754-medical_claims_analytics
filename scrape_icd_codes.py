import pandas as pd
import requests
from bs4 import BeautifulSoup
import pprint

BASE_URL = "https://www.icd10data.com"
START_URL = BASE_URL + "/ICD10CM/Codes"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_page(url):
    """Fetch page and return BeautifulSoup object."""
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to fetch {url} (status {response.status_code})")
        return None
    return BeautifulSoup(response.text, "html.parser")

def extract_list_entries(soup, is_top_level=False):
    """
    Generalized function to extract code, description, and URL.
    Targets the <ul> list either as the first one found (top level) 
     OR the one following 'Codes' (nested levels).
    """
    entries = []
    ul = None

    if is_top_level:
        # For the top-level page, find the first <ul> inside body-content
        body_content = soup.find("div", class_="body-content")
        ul = body_content.find("ul") if body_content else None
    else:
        # For nested pages, find the <ul> following the 'Codes' marker
        codes_header = soup.find(lambda tag: tag.name == 'div' and tag.text.strip() == 'Codes')
        if codes_header:
            ul = codes_header.find_next_sibling("ul")
        else: print("No 'Codes' header found.")

    if not ul:
        print("Could not find the target <ul> list.")
        return entries

    for li in ul.find_all("li", recursive=False):
        a_tag = li.find("a", class_="identifier")
        
        if not a_tag:
            continue
            
        code_range = a_tag.get_text(strip=True)
        url = BASE_URL + a_tag['href']

        div = li.find("div")
        description = li.get_text(" ", strip=True).replace(a_tag.get_text(strip=True), "").replace(div.get_text(" ", strip=True), "").strip()

        if not description and not code_range:
            continue

        entries.append({
            "CodeRange": code_range,
            "Description": description,
            "URL": url
        })
        
    return entries

def scrape_icd_codes(start, end):

    final_code_list = []
    
    # 1. Scraping Level 1 (Top Chapters)
    print("--- Scraping Level 1: Top Chapters ---")
    soup_level_1 = fetch_page(START_URL)
    
    if not soup_level_1:
        return

    top_level_chapters = extract_list_entries(soup_level_1, is_top_level=True)
    print(f"Found {len(top_level_chapters)} top-level chapters.")

    # Loop through Level 1
    for i, ch1 in enumerate(top_level_chapters):
        url_l2 = ch1['URL']
        desc_l1 = ch1['Description']

        if i < start or i > end:
            continue  # Skip chapters outside the specified range
        # 2. Scraping Level 2 (Blocks/Categories)
        print(f"\n--- Scraping Level 2: {ch1['CodeRange']} ---")
        soup_level_2 = fetch_page(url_l2)
        
        if not soup_level_2: continue
        
        sec_level_codes = extract_list_entries(soup_level_2, is_top_level=False)

        # Loop through Level 2
        for ch2 in sec_level_codes:
            url_l3 = ch2['URL']
            desc_l2 = ch2['Description']

            # 3. Scraping Level 3 (Specific Codes/Sub-categories)
            #print(f"  --> Scraping Level 3: {ch2['CodeRange']}")
            soup_level_3 = fetch_page(url_l3)

            if not soup_level_3: continue
            
            # The entries here are typically the final codes/sub-categories
            third_level_codes = extract_list_entries(soup_level_3, is_top_level=False)

            # Loop through Level 3 and combine data
            for final_entry in third_level_codes:
                final_code_list.append({
                    "Coderange": final_entry['CodeRange'], # Final code range
                    "level1_desc": desc_l1,
                    "level2_desc": desc_l2,
                    "level3_desc": final_entry['Description'], # Description from the final page
                    "URL": final_entry['URL']
                })
    
    # Final Output
    print(f"\n=======================================================")
    print(f"✅ FINAL RESULT: {len(final_code_list)} Codes Extracted (first 5 shown):")
    print("=======================================================")

    return final_code_list

    

def main():
    # Scrape ICD codes from chapters 1 to 22
    #code_list_1_18 = scrape_icd_codes(start=0, end=18)
    code_list_18_22 = scrape_icd_codes(start=18, end=22)
    
 
    df = pd.DataFrame(code_list_18_22)

    df.to_csv(
        "./data/icd_codes_scraped_18_22.csv",
        index=False,
        encoding="utf-8"
    )

    


if __name__ == "__main__":
    main()
