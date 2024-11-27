import os
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

def import_excel_to_dataframe(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return pd.DataFrame()
    except Exception as e:
        print(e)
        return pd.DataFrame()

def import_all_excels(folder_path: str) -> pd.DataFrame:
    all_data = []

    for file in os.listdir(folder_path):
        if file.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file)
            df = import_excel_to_dataframe(file_path, sheet_name='raport')
            if not df.empty:
                all_data.append(df)

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def clean_data(df: pd.DataFrame):
    artifacts = ['--', '---', 'NaN', 'None', '']

    df.replace(artifacts, None, inplace=True)

    return df

def set_column_types(df: pd.DataFrame):

    column_types = {
        'TrainID': 'INTEGER',
        'TrainNumber': 'TEXT',
        'JourneyID': 'INTEGER',
        'StationID': 'INTEGER',
        'StationName': 'TEXT',
        'ArrivalTime': 'DATETIME',
        'DepartureTime': 'DATETIME',
        'CurrentLoad': 'INTEGER',
        'In': 'INTEGER',
        'Out': 'INTEGER'
    }

    for column, dtype in column_types.items():
        if column in df.columns:
            if dtype == 'INTEGER':
                df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')
            elif dtype == 'TEXT':
                df[column] = df[column].astype(str)
            elif dtype == 'FLOAT':
                df[column] = pd.to_numeric(df[column], errors='coerce', downcast='float')
            elif dtype == 'DATE':
                df[column] = pd.to_datetime(df[column], errors='coerce')

            elif dtype == 'DATETIME':
                if column in df.columns:
                    df[column] = pd.to_datetime(df[column], format='%d.%m.%Y %H:%M:%S', errors='coerce')

    return df

def process_data_to_structures(df: pd.DataFrame):
    df_copy = df.copy()
    df = df.drop([0, 1])
    df = df.reset_index(drop=True)
    trains = df[['NrPociągu']].drop_duplicates().rename(columns={'NrPociągu': 'TrainNumber'})
    trains['TrainID'] = range(1, len(trains) + 1)
    print(trains.head())

    stations = df[['Stacja']].drop_duplicates().rename(columns={'Stacja': 'StationName'})
    stations['StationID'] = range(1, len(stations) + 1)
    print(stations.head())

    train_journeys = df[['NrPociągu', 'Stacja', 'Przyjazd', 'Odjazd', 'Pojazd 1']].drop_duplicates().rename(columns={'NrPociągu': 'TrainNumber', 'Stacja': 'StationName', 'Pojazd': 'Vehicle'})
    train_journeys = train_journeys.merge(trains, on='TrainNumber')
    train_journeys = train_journeys.merge(stations, on='StationName')
    train_journeys = train_journeys.rename(columns={'Przyjazd': 'ArrivalTime', 'Odjazd': 'DepartureTime'})
    train_journeys['JourneyID'] = range(1, len(train_journeys) + 1)
    print(train_journeys.head())

    df_copy2 = df_copy.copy()
    header = []
    header.append(df_copy2.iat[0, 8])
    header.append(df_copy2.iat[0, 9])
    print(header)
    df_copy2.columns = list(df_copy2.columns[:8]) + list(header) + list(df_copy2.columns[10:])
    passenger_counts = df_copy2[['NrPociągu', 'Przyjazd', 'Odjazd', 'Liczba osób', 'In', 'Out']].rename(columns={
        'NrPociągu': 'TrainNumber',
        'Stacja': 'StationName',
        'Liczba osób': 'CurrentLoad',
        'Przyjazd': 'ArrivalTime',
        'Odjazd': 'DepartureTime',
    })
    passenger_counts = passenger_counts.merge(train_journeys, on=['ArrivalTime', 'DepartureTime'])
    print(passenger_counts.head())

    last_value = None
    for i in range(11, len(df_copy.iloc[0])):
        if pd.notna(df_copy.iloc[0, i]):
            last_value = df_copy.iloc[0, i]
        else:
            df_copy.iloc[0, i] = last_value

    df_copy = df_copy.ffill()

    header_row_1 = df_copy.iloc[0, 11:]
    header_row_2 = df_copy.iloc[1, 11:]
    header_row_3 = df_copy.iloc[2, 11:]

    new_headers = []
    for col_1, col_2, col_3 in zip(header_row_1, header_row_2, header_row_3):
        if pd.notna(col_1):
            new_headers.append(f"{col_1}.{col_2}" if pd.notna(col_2) else col_1)
        elif pd.notna(col_2):
            new_headers.append(f"{col_2}.{col_3}" if pd.notna(col_3) else col_2)
        else:
            new_headers.append(col_3)

    df_copy = df_copy.drop([0, 1])
    df_copy = df_copy.reset_index(drop=True)
    print(df_copy.head())

    df_copy.columns = list(df_copy.columns[:11]) + new_headers

    door_columns = [col for col in df_copy.columns if "Drzwi" in col]

    door_data = pd.melt(
        df_copy,
        id_vars=['NrPociągu', 'Stacja'],
        value_vars=door_columns,
        var_name="DoorAndDirection",
        value_name="Count"
    )

    print(door_data.head())

    door_data[['Door', 'Direction']] = door_data['DoorAndDirection'].str.split('.', expand=True)
    door_data['Door'] = door_data['Door'].str.replace("Drzwi: ", "").astype(int)

    door_data = door_data.rename(columns={'NrPociągu': 'TrainNumber', 'Stacja': 'StationName'})
    door_data = door_data.merge(trains, on='TrainNumber').merge(stations, on='StationName')

    door_usage = door_data[['TrainID', 'StationID', 'Door', 'Direction', 'Count']]

    print(door_usage.head())

    return {
        'Trains': trains[['TrainID', 'TrainNumber']],
        'Stations': stations[['StationID', 'StationName']],
        'TrainJourneys': train_journeys[['JourneyID', 'StationID', 'TrainID', 'ArrivalTime', 'DepartureTime']],
        'PassengerCounts': passenger_counts[['JourneyID', 'CurrentLoad', 'In', 'Out']],
        'DoorUsage': door_usage
    }

def save_to_sql(df_dict: dict, db_url: str):
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            for table_name, df in df_dict.items():

                df = clean_data(df)
                df = set_column_types(df)

                df.to_sql(table_name, con=connection, if_exists='replace', index=False)
                print(f"Data saved to table '{table_name}'.")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    folder_path = "F:\\PAD\\pythonProject\\01.23"
    db_url = 'sqlite:///lka_data_passengers.db'

    raw_data = import_all_excels(folder_path)

    if not raw_data.empty:
        print(raw_data.head())
        print("Processing data into structured format...")
        structured_data = process_data_to_structures(raw_data)

        print("Saving structured data to database...")
        save_to_sql(structured_data, db_url)

        print("Data import and processing complete.")
    else:
        print("No data found to process.")