import pandas as pd
import sqlite3

file_path = "opoznienia.xlsx"

miesiac_map = {
    "styczeń": 1, "luty": 2, "marzec": 3, "kwiecień": 4,
    "maj": 5, "czerwiec": 6, "lipiec": 7, "sierpień": 8,
    "wrzesień": 9, "październik": 10, "listopad": 11, "grudzień": 12
}

def transform_data(df):
    df.columns = df.columns.str.strip()

    df.dropna(how='all', inplace=True)

    data_list = []

    for _, row in df.iterrows():
        rok = int(row['rok'])
        miesiac = row['miesiąc']

        if miesiac in miesiac_map:
            miesiac_num = miesiac_map[miesiac]
        else:
            continue

        data = pd.Timestamp(year=rok, month=miesiac_num, day=1)
        print(data)

        for przyczyna, liczba_opoznien in row.iloc[2:].items():
            print(przyczyna)
            print(liczba_opoznien)
            if pd.notna(liczba_opoznien):
                data_list.append({
                    'data': data,
                    'przyczyna': przyczyna,
                    'liczba_opoznien': int(liczba_opoznien)
                })

    transformed_df = pd.DataFrame(data_list)
    return transformed_df


def save_to_sqlite(df, db_name='test.db'):
    conn = sqlite3.connect(db_name)
    df.to_sql('opoznienia', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Zapisano dane do bazy '{db_name}'.")


def main():
    df = pd.read_excel(file_path, 'pkt_kody', engine='openpyxl', skiprows=2, nrows=73, usecols='B:X')
    #print(df.head())

    transformed_df = transform_data(df)
    print(transformed_df.head())

    save_to_sqlite(transformed_df)


if __name__ == "__main__":
    main()