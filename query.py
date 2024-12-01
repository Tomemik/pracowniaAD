import sqlite3
import os

def execute_query(database_path, query):
    try:
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()

        cursor.execute(query)
        results = cursor.fetchall()

        return results
    except sqlite3.Error as e:
        print(f"Error while executing query: {e}")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":

    query_LoadPassengersOnRoute = """
        CREATE VIEW LoadPassengersOnRoute AS
        SELECT
            T.TrainNumber,
            S.StationName,
            Min(time(TJ.ArrivalTime)) as EarliestArrival,
            Max(time(TJ.ArrivalTime)) as LateArrival,
            Min(time(tj.DepartureTime)) as EarliestDeparture,
            Max(time(TJ.DepartureTime)) as LateDeparture,
            SUM(PC.CurrentLoad) AS TotalPassengers,
            count(PC.CurrentLoad) AS NumberArrival
        FROM
            PassengerCounts PC
        JOIN
            TrainJourneys TJ ON PC.JourneyID = TJ.JourneyID
        JOIN
            Trains T ON TJ.TrainID = T.TrainID
        JOIN
            Stations S ON TJ.StationID = S.StationID
        WHERE
            TJ.ArrivalTime BETWEEN '2023-01-01' AND '2023-01-31'  -- Adjust date range if needed
        GROUP BY
            T.TrainNumber, S.StationName
        ORDER BY
            TotalPassengers DESC;
    """

    query_DoorUse = """
    CREATE VIEW DoorUse AS
    SELECT
        DU.door,
        SUM(CASE WHEN DU.direction = 'In' THEN DU.count ELSE 0 END) AS total_in,
        SUM(CASE WHEN DU.direction = 'Out' THEN DU.count ELSE 0 END) AS total_out,
        SUM(Du.count) as TotalTransitions
    FROM
        DoorUsage as DU
    GROUP BY
        door
    ORDER BY
        door;
    """

    query_TrainLoadInHour = """CREATE VIEW HourlyPassengerLoad AS
    WITH temp_table AS (
        SELECT
            ArrivalTime,
            DepartureTime,
            CurrentLoad,
            Cast(strftime('%H', DepartureTime) AS INT) AS DepartureHour,  -- Godzina odjazdu
            Cast(strftime('%H', ArrivalTime) AS INT) AS ArrivalHour         -- Godzina przyjazdu
        FROM
            PassengerCounts PC  -- Zakładamy, że dane są w tabeli PassengerCounts
        JOIN
            TrainJourneys TJ ON PC.JourneyID = TJ.JourneyID
        WHERE
            TJ.ArrivalTime BETWEEN '2023-01-01' AND '2023-01-31'  -- Określenie zakresu dat
    )
    SELECT
        hour,
        SUM(CASE WHEN DepartureHour = hour OR ArrivalHour = hour THEN CurrentLoad ELSE 0 END) AS TotalLoad
    FROM ( temp_table )
    CROSS JOIN (
        SELECT 0 AS hour UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL
        SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL
        SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL
        SELECT 19 UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23
    ) AS hours
    GROUP BY hour
    ORDER BY hour;
    """

    query_PassengersFlowAtStations = """
    CREATE VIEW PassengersFlowAtStations AS
    SELECT
        S.StationName,
        SUM(PC."In") AS TotalIn,
        SUM(PC.Out) AS TotalOut
    FROM
        PassengerCounts PC
    JOIN
        TrainJourneys TJ ON PC.JourneyID = TJ.JourneyID
    JOIN
        Stations S ON TJ.StationID = S.StationID
    WHERE
        TJ.ArrivalTime BETWEEN '2023-01-01' AND '2023-01-31'  
    GROUP BY
        S.StationName
    ORDER BY
        TotalIn DESC;
    """

    queries = [query_LoadPassengersOnRoute, query_DoorUse, query_TrainLoadInHour, query_PassengersFlowAtStations]
    db_path = r"lka_data_passengers.db"

    for query in queries:
        execute_query(db_path, query)




