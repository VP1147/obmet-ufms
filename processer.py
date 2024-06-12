import pandas as pd
file = "CR1000_OBMET_Veterinaria.csv"
cols = ['TIMESTAMP', 'PTemp_C_Avg', 'RG_Total_Tot']

try:
    # Row 0 is the file header
    # Row 2 contains the variable units (i.e., Deg C)
    # Row 3 contains the method used for measuring the variable (i.e. Avg, Tot, Smp, Max)
    data = pd.read_csv(file, sep=";", skiprows=[0,2,3], usecols=cols)
    print(data)

except FileNotFoundError:
    print(f"File '{file}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")