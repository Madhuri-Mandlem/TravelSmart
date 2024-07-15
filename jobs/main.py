import os
import random
import time
import uuid
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
import simplejson as json

LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# Calculate movements increment :
LATIITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# environment variables for config
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAO_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42)
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time
    # update frequency
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 50),  # km/hr
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }


def generate_traffic_camera_data(device_id, timestamp, location,  camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'cameraSnapshot': 'Base64EncodedString'
    }


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(-5, 28),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Raining', 'Snowing']),
        'precipitation': random.uniform(0, 25),
        'humidity': random.uniform(0, 100),  # percentage
        'airQualityIndex': random.uniform(0, 500)  # AQL value goes here
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incident_id': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Police', 'Medical', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'  # descp can be put here

    }


def simulate_vehicle_movement():
    global start_location

    # movement towards birmingham
    start_location['latitude'] += LATIITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # add some randomness to stimulate actual road travel
    start_location['latitude'] += random.uniform(-0.005, 0.005)
    start_location['longitude'] += random.uniform(-0.005, 0.005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.randint(10, 50),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuel_type': 'Hybrid'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} not JSON serializable')


def delivery_report(err, msg):
    if err is not None:
        print('Delivery failed: {err}')
    else:
        print(f'Delivery completed to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data[id]),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report)
    producer.flush()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],
                                                           vehicle_data['location'], 'Nikon-Cam234')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],
                                                                   vehicle_data['location'])
        print(vehicle_data)
        print(gps_data)
        print(traffic_camera_data)
        print(weather_data)
        print(emergency_incident_data)
        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] and
                vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending ...')
            break
        produce_data_to_kafka = (producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka = (producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka = (producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka = (producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka = (producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Madhuri-1')
    except KeyboardInterrupt:
        print('simulation ended by the user')
    except Exception as e:
        print(f'Unexpected error occured: {e}')
