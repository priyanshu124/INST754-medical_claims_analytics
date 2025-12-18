import pandas as pd


def ind_codes_df(path = "./data/icd10cm-codes-2026.txt"):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Split ONLY on first whitespace
            icd_code, description = line.split(maxsplit=1)

            rows.append({
                "icd_code": icd_code,
                "description": description
            })

    df_codes = pd.DataFrame(rows)


    # Strip whitespace and get first 3 characters
    df_codes['icd_code'] = df_codes['icd_code'].str.strip()
    df_codes["description"] = df_codes["description"].str.strip()
    df_codes['icd_prefix'] = df_codes['icd_code'].str[:3]

    return df_codes

def code_ranges_df(path1 = "./data/icd_codes_scraped.csv", path2 = "./data/icd_codes_scraped_18_22.csv"):
    code_range_1 = pd.read_csv(path1, dtype=str)
    code_range_2 = pd.read_csv(path2, dtype=str)
    df_ranges = pd.concat([code_range_1, code_range_2], ignore_index=True)

    df_ranges['icd_prefix'] = df_ranges['Coderange'].str[:3]
    return df_ranges

def merge_codes_and_ranges(df_codes, df_ranges):
    # Merge on the icd_prefix
    merged_df = pd.merge(
        df_codes, 
        df_ranges, 
        on='icd_prefix', 
        how='left', 
    )

    # Select and reorder columns
    final_df = merged_df[[
        'icd_code', 
        'description', 
        'Coderange', 
        'level1_desc', 
        'level2_desc', 
        'level3_desc', 
        'URL'
    ]]

    final_df.columns = [
        'diagnosis_code', 
        'description', 
        'code_range',
        'level_1_description', 
        'level_2_description', 
        'level_3_description', 
        'url'
    ]

    return final_df


def main():
    # Load individual ICD codes
    df_codes = ind_codes_df("./data/icd10cm-codes-2026.txt")

    # Load code ranges with descriptions
    df_ranges = code_ranges_df("./data/icd_codes_scraped.csv", "./data/icd_codes_scraped_18_22.csv")

    # Merge the two dataframes
    final_df = merge_codes_and_ranges(df_codes, df_ranges)

    # Save the final dataframe to CSV
    final_df.to_csv(
        "./data/diagnosis_codes.csv",
        index=False,
        encoding="utf-8"
    )

    print(f"✅ Final ICD-10 Data saved with {len(final_df)} records.")

    print(final_df.head(5))


if __name__ == "__main__":
   main()
