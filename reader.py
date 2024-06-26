import pandas as pd
import locale
locale.setlocale(locale.LC_NUMERIC, '')
from locale import atof

file = "CR1000_OBMET_Veterinaria.csv"

# TIMESTAMP and AVERAGE-type values
avgs = ['TIMESTAMP','RG_in_Avg','NET_Wm2_Avg','Temp_2m_Avg','UR_2m','WS_10m_Avg',
            'G_solo_Avg','WS_2m_Avg','Tsolo_1cm_Avg','UV_in_Avg','Tsolo_5cm_Avg',
            'Tsolo_10cm_Avg','Pressao_kPa_Avg',]

# TIMESTAMP and MAX-type values
maxs = ['TIMESTAMP','Temp_2m_Max','UR_2m_Max','WS_10m_Max']

# TIMESTAMP and MIN-type values
mins = ['TIMESTAMP','Temp_2m_Min','UR_2m_Min',]

# TIMESTAMP and TOTAL-type values
tots = ['TIMESTAMP','RG_Total_Tot','Ppt_mm_Tot']
try:
    ## AVERAGE-type values

    avg_data = pd.read_csv(file, sep=";", skiprows=[0,2,3], usecols=avgs)

    # Create a dataframe for the AVERAGE-type values
    df_avg = pd.DataFrame(avg_data)

    # Convert the TIMESTAMP to Pandas DateTime
    df_avg['TIMESTAMP'] = pd.to_datetime(df_avg['TIMESTAMP'], format='%d/%m/%Y %H:%M')

    # Convert the dataframe to default locale
    cols = df_avg.columns.difference(['TIMESTAMP'])
    df_avg[cols] = df_avg[cols].applymap(atof)

    # Convert measured values to float
    df_avg[cols] = df_avg[cols].astype(float)
    #df["PTemp_C_Avg"] = df["PTemp_C_Avg"].astype(float)
    print(df_avg)

    # Calculate the average values of each day
    print("<<<--- Daily Avg --->>>")
    df_avg = df_avg.resample('d', on='TIMESTAMP').mean().dropna(how='all')
    print(df_avg)

    ## MAX-type values
    max_data = pd.read_csv(file, sep=";", skiprows=[0,2,3], usecols=maxs)
    df_max = pd.DataFrame(max_data)
    df_max['TIMESTAMP'] = pd.to_datetime(df_max['TIMESTAMP'], format='%d/%m/%Y %H:%M')
    cols = df_max.columns.difference(['TIMESTAMP'])
    df_max[cols] = df_max[cols].applymap(atof)
    df_max[cols] = df_max[cols].astype(float)
    print("<<<--- Daily Max --->>>")
    df_max = df_max.resample('d', on='TIMESTAMP').max().dropna(how='all')
    print(df_max)

    ## MIN-type values
    min_data = pd.read_csv(file, sep=";", skiprows=[0,2,3], usecols=mins)
    df_min = pd.DataFrame(min_data)
    df_min['TIMESTAMP'] = pd.to_datetime(df_min['TIMESTAMP'], format='%d/%m/%Y %H:%M')
    cols = df_min.columns.difference(['TIMESTAMP'])
    df_min[cols] = df_min[cols].applymap(atof)
    df_min[cols] = df_min[cols].astype(float)
    print("<<<--- Daily Min --->>>")
    df_min = df_min.resample('d', on='TIMESTAMP').min().dropna(how='all')
    print(df_min)

    ## TOTAL-type values
    tot_data = pd.read_csv(file, sep=";", skiprows=[0,2,3], usecols=tots)
    df_tot = pd.DataFrame(tot_data)
    df_tot['TIMESTAMP'] = pd.to_datetime(df_tot['TIMESTAMP'], format='%d/%m/%Y %H:%M')
    cols = df_tot.columns.difference(['TIMESTAMP'])
    df_tot[cols] = df_tot[cols].applymap(atof)
    df_tot[cols] = df_tot[cols].astype(float)
    print("<<<--- Daily Total --->>>")
    df_tot = df_tot.resample('d', on='TIMESTAMP').sum().dropna(how='all')
    print(df_tot)

    ## Append all dataframes into FINAL DATA
    df_f = df_avg.join([df_max,df_min,df_tot])
    print(df_f)


    ## Send to a new CSV
    df_f.to_csv('resultado.csv', index=True)
    print(">>> Concluído!")
except FileNotFoundError:
    print(f"File '{file}' not found.")
except Exception as e:
    print(f"An error occurred: {e}")