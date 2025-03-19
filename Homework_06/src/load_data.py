import csv
import json
import gzip
from time import time
from kafka import KafkaProducer

# Columnas seleccionadas
SELECTED_COLUMNS = ['lpep_pickup_datetime',
                    'lpep_dropoff_datetime',
                    'PULocationID',
                    'DOLocationID',
                    'passenger_count',
                    'trip_distance',
                    'tip_amount']

def main():
    t0 = time()

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = 'data/green_tripdata_2019-10.csv.gz' 

    with gzip.open(csv_file, 'rt', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            filtered_row = {key: row[key] for key in SELECTED_COLUMNS}
            producer.send('green-trips', value=filtered_row)

    producer.flush()
    producer.close()

    t1 = time()
    took = t1 - t0

    print(f'took {(took):.2f} seconds')


if __name__ == "__main__":
    main()
