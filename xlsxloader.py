import os

import pandas as pd
from sqlalchemy import create_engine

def import_excel_to_dataframe(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
        return df
    except FileNotFoundError:
        print(f"Plik {file_path} nie został znaleziony.")
    except Exception as e:
        print(e)


def import_all_excels(folder_path: str) -> pd.DataFrame:
    all_data = []

    for file in os.listdir(folder_path):
        if file.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file)
            try:
                df = pd.read_excel(file_path, sheet_name='baza_danych', engine='openpyxl')
                all_data.append(df)
            except Exception as e:
                print(e)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()


def save_to_sql(df: pd.DataFrame, db_url: str, table_name: str):
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df.to_sql(table_name, con=connection, if_exists='replace', index=False)
        print(f"Dane zapisane do tabeli '{table_name}' w bazie danych.")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    folder_path = "F:\PAD\pythonProject\luty24"

    combined_df = import_all_excels(folder_path)

    if not combined_df.empty:
        print("Podgląd danych:")
        print(combined_df.head())

        db_url = 'sqlite:///test.db'
        table_name = 'test'

        save_to_sql(combined_df, db_url, table_name)
    else:
        print("Brak danych do zapisania.")