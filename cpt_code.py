from datasets import load_dataset
import pandas as pd

# Load the dataset
dataset = load_dataset("atta00/cpt-hcpcs-codes")

# Combine all splits if you want one file
df_train = dataset["train"].to_pandas()
df_test  = dataset["test"].to_pandas()

df = pd.concat([df_train, df_test], ignore_index=True)

# Save to CSV
df.to_csv("./data/cpt_hcpcs_codes.csv", index=False)
