import pandas as pd
import glob

path = "/Users/shiwei/Desktop/课件/第三学期/transportation/练习/DATI MONOPATTINI SHARING/OPERATORE A/Torino_Corse24-25_MENSILI_senza_percorso/*.csv"
files = glob.glob(path)

#Operator A cleaning
print("the number of document of OperatorA:", len(files))

df_list = []
for f in files:
    temp = pd.read_csv(f)
    df_list.append(temp)

df = pd.concat(df_list, ignore_index=True)
print("index before cleaning", len(df))

rename_map = {
    "ID_VEICOLO": "vehicle_id",
    "DATAORA_INIZIO": "start_time",
    "DATAORA_FINE": "end_time",

    "LATITUDINE_INIZIO_CORSA": "start_lat",
    "LONGITUTIDE_INIZIO_CORSA": "start_lon",
    "LATITUDINE_FINE_CORSA": "end_lat",
    "LONGITUTIDE_FINE_CORSA": "end_lon",

    "DISTANZA_KM": "distance_km",
    "DURATA_MIN": "duration_min",
    "RISERVATO": "reserved",

    "BATTERIA_INIZIO_CORSA": "battery_start",
    "BATTERIA_FINE_CORSA": "battery_end",

    "ANNO_MESE": "year_month"
}

df = df.rename(columns=rename_map)

essential = [
    "vehicle_id", "start_time", "end_time",
    "start_lat", "start_lon", "end_lat", "end_lon"
]

df_clean = df.dropna(subset=essential)

df_clean = df_clean[df_clean["distance_km"] > 0]
df_clean = df_clean[df_clean["duration_min"] > 0]

df_clean = df_clean[(df_clean["battery_start"] >= 0) & (df_clean["battery_start"] <= 100)]
df_clean = df_clean[(df_clean["battery_end"] >= 0) & (df_clean["battery_end"] <= 100)]

print("index after cleaning:", len(df_clean))

df_clean.to_csv("/Users/shiwei/Desktop/operator_A_clean.csv", index=False)

#Operator B cleaning
print("The number of ducuments of Operator B:", len(files))

df_list = []
for f in files:
    temp = pd.read_excel(f)
    df_list.append(temp)

df = pd.concat(df_list, ignore_index=True)
print("index before cleaning:", len(df))

rename_map = {
    "Identificativo noleggio": "trip_id",
    "Targa veicolo": "vehicle_id",
    "Data inizio corsa": "start_time",
    "Data fine corsa": "end_time",
    
    "Lat inizio corsa_coordinate": "start_lat",
    "Lon inizio corsa_coordinate": "start_lon",
    "Lat fine corsa_coordinate": "end_lat",
    "Lon fine corsa_coordinate": "end_lon",
    
    "Tempo Tot": "duration_min",
    "KM Tot": "distance_km",
    
    "Prenotazione": "reserved",
    "Batteria inizio": "battery_start",
    "Batteria fine": "battery_end"
}

df = df.rename(columns=rename_map)

essential = ["trip_id", "vehicle_id", "start_time", "end_time",
             "start_lat", "start_lon", "end_lat", "end_lon"]

df_clean = df.dropna(subset=essential)

df_clean = df_clean[df_clean["distance_km"] > 0]
df_clean = df_clean[df_clean["duration_min"] > 0]

df_clean = df_clean[(df_clean["battery_start"] >= 0) & (df_clean["battery_start"] <= 100)]
df_clean = df_clean[(df_clean["battery_end"] >= 0) & (df_clean["battery_end"] <= 100)]

print("Index after cleaning:", len(df_clean))


df_clean.to_csv("/Users/shiwei/Desktop/operator_B_clean.csv", index=False)
print("cleaning done! save data document as operator_B_clean.csv")

#Operator C cleaning
print("Number of documents of Operator Bird:", len(files))

df_list = []
for f in files:
    temp = pd.read_csv(f)
    df_list.append(temp)

df = pd.concat(df_list, ignore_index=True)
print("Index before cleaning:", len(df))

rename_map = {
    "ID_VEICOLO": "vehicle_id",
    "DATAORA_INIZIO": "start_time",
    "DATAORA_FINE": "end_time",

    "LATITUDINE_INIZIO_CORSA": "start_lat",
    "LONGITUTIDE_INIZIO_CORSA": "start_lon",
    "LATITUDINE_FINE_CORSA": "end_lat",
    "LONGITUTIDE_FINE_CORSA": "end_lon",

    "PERCORSO": "percorso",
    "DISTANZA_KM": "distance_km",
    "DURATA_MIN": "duration_min",
    "RISERVATO": "reserved"
}

df = df.rename(columns=rename_map)

essential = [
    "vehicle_id", "start_time", "end_time",
    "start_lat", "start_lon", "end_lat", "end_lon"
]

df_clean = df.dropna(subset=essential)

df_clean = df_clean[df_clean["distance_km"] > 0]
df_clean = df_clean[df_clean["duration_min"] > 0]

print("Index after cleaning:", len(df_clean))

output_path = "/Users/shiwei/Desktop/Bird_clean.csv"
df_clean.to_csv(output_path, index=False)

print("Cleaning done! File saved as Bird_clean.csv")

#Combine all operators
import pandas as pd

df_A = pd.read_csv("operator_A_clean.csv", dtype=str)
df_B = pd.read_csv("operator_B_clean.csv", dtype=str)
df_bird = pd.read_csv("bird_clean.csv", dtype=str)

df_A["operator"] = "A"
df_B["operator"] = "B"
df_bird["operator"] = "Bird"

common_cols = df_A.columns.intersection(df_B.columns).intersection(df_bird.columns)

df_A = df_A[common_cols]
df_B = df_B[common_cols]
df_bird = df_bird[common_cols]

df_all = pd.concat([df_A, df_B, df_bird], ignore_index=True)

print("A:", df_A.shape)
print("B:", df_B.shape)
print("Bird:", df_bird.shape)
print("ALL:", df_all.shape)
print("Operators in ALL:", df_all["operator"].unique())

df_all.to_csv("/Users/shiwei/Desktop/all_operators_clean.csv", index=False)




