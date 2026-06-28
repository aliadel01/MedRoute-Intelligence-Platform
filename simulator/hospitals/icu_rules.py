import pandas as pd
import numpy as np

df = pd.read_csv("simulator/hospitals/hospitals.csv")

def calculate_icu_beds(total_beds):
    # Handle missing or invalid data safely
    if pd.isna(total_beds) or total_beds <= 0:
        return 0
    
    # Apply Tiered Rules
    if total_beds < 100:
        icu = total_beds * 0.05
    elif 100 <= total_beds < 500:
        icu = total_beds * 0.10
    else:
        icu = total_beds * 0.15
        
    # Round to nearest whole integer and ensure a minimum of 1 bed for active hospitals
    return max(1, int(np.round(icu)))

df['ICU_Beds'] = df['BEDS'].apply(calculate_icu_beds)

# Filter out hospitals with zero beds
df = df[df['BEDS'] > 0]  

df.to_csv("simulator/hospitals/hospitals_with_icu_beds.csv", index=False)