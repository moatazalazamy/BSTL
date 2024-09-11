import json
from datetime import datetime
from sqlalchemy.orm import Session
from models import Base, DropOffAddress, PickupAddress, Receiver, Star, Tracker, Order
from database import engine, SessionLocal
import traceback

def transform_json(json_data):
    transformed_data = []
    
    for record in json_data:
        transformed_record = {
            "id": record.get("_id", ""),
            "cod_amount": record.get("cod", {}).get("amount", 0),
            "cod_is_paid_back": record.get("cod", {}).get("isPaidBack", False),
            "cod_collected_amount": record.get("cod", {}).get("collectedAmount", 0),
            "collected_from_business_date": datetime.fromisoformat(record.get("collectedFromBusiness", {}).get("$date", "1970-01-01T00:00:00Z")[:-1]),
            "confirmation_is_confirmed": record.get("confirmation", {}).get("isConfirmed", False),
            "confirmation_number_of_sms_trials": record.get("confirmation", {}).get("numberOfSmsTrials", 0),
            "created_at": datetime.fromisoformat(record.get("createdAt", {}).get("$date", "1970-01-01T00:00:00Z")[:-1]),
            "drop_off_address": {
                "id": record.get("dropOffAddress", {}).get("_id", ""),
                "second_line": record.get("dropOffAddress", {}).get("secondLine", ""),
                "city_id": record.get("dropOffAddress", {}).get("city", {}).get("_id", ""),
                "name": record.get("dropOffAddress", {}).get("name", ""),
                "zone_name": record.get("dropOffAddress", {}).get("zone", {}).get("name", ""),
                "district": record.get("dropOffAddress", {}).get("district", ""),
                "first_line": record.get("dropOffAddress", {}).get("firstLine", ""),
                "geo_location_lat": record.get("dropOffAddress", {}).get("geoLocation", [0, 0])[0],
                "geo_location_long": record.get("dropOffAddress", {}).get("geoLocation", [0, 0])[1]
            },
            "pickup_address": {
                "id": record.get("pickupAddress", {}).get("_id", ""),
                "floor": record.get("pickupAddress", {}).get("floor", ""),
                "apartment": record.get("pickupAddress", {}).get("apartment", ""),
                "second_line": record.get("pickupAddress", {}).get("secondLine", ""),
                "city_id": record.get("pickupAddress", {}).get("city", {}).get("_id", ""),
                "name": record.get("pickupAddress", {}).get("name", ""),
                "zone_name": record.get("pickupAddress", {}).get("zone", {}).get("name", ""),
                "district": record.get("pickupAddress", {}).get("district", ""),
                "first_line": record.get("pickupAddress", {}).get("firstLine", ""),
                "geo_location_lat": record.get("pickupAddress", {}).get("geoLocation", [0, 0])[0],
                "geo_location_long": record.get("pickupAddress", {}).get("geoLocation", [0, 0])[1],
                "country_id": record.get("pickupAddress", {}).get("country", {}).get("_id", ""),
                "country_name": record.get("pickupAddress", {}).get("country", {}).get("name", ""),
                "country_code": record.get("pickupAddress", {}).get("country", {}).get("code", "")
            },
            "receiver": {
                "id": record.get("receiver", {}).get("_id", ""),
                "first_name": record.get("receiver", {}).get("firstName", ""),
                "last_name": record.get("receiver", {}).get("lastName", ""),
                "phone": record.get("receiver", {}).get("phone", "")
            },
            "star": {
                "id": record.get("star", {}).get("_id", ""),
                "name": record.get("star", {}).get("name", ""),
                "phone": record.get("star", {}).get("phone", "")
            },
            "tracker": {
                "id": record.get("tracker", {}).get("trackerId", ""),
                "tracker_id": record.get("tracker", {}).get("trackerId", ""),
                "order_id": record.get("tracker", {}).get("order_id", "")
            },
            "order_id": record.get("order_id", ""),
            "type": record.get("type", ""),
            "updated_at": datetime.fromisoformat(record.get("updatedAt", {}).get("$date", "1970-01-01T00:00:00Z")[:-1])
        }
        transformed_data.append(transformed_record)
    
    return transformed_data

def load_data_to_db(data, db):
    try:
        for record in data:
            drop_off_address = DropOffAddress(
                id=record['drop_off_address']['id'],
                second_line=record['drop_off_address'].get('second_line', ''),
                city_id=record['drop_off_address'].get('city_id', ''),
                name=record['drop_off_address'].get('name', ''),
                zone_name=record['drop_off_address'].get('zone_name', ''),
                district=record['drop_off_address'].get('district', ''),
                first_line=record['drop_off_address'].get('first_line', ''),
                geo_location_lat=record['drop_off_address'].get('geo_location_lat', 0),
                geo_location_long=record['drop_off_address'].get('geo_location_long', 0)
            )
            
            existing_record = db.query(DropOffAddress).filter(DropOffAddress.id == drop_off_address.id).first()
            if existing_record:
                db.delete(existing_record)  # Remove existing record
            db.add(drop_off_address)

            pickup_address = PickupAddress(
                id=record['pickup_address']['id'],
                floor=record['pickup_address'].get('floor', ''),
                apartment=record['pickup_address'].get('apartment', ''),
                second_line=record['pickup_address'].get('second_line', ''),
                city_id=record['pickup_address'].get('city_id', ''),
                name=record['pickup_address'].get('name', ''),
                zone_name=record['pickup_address'].get('zone_name', ''),
                district=record['pickup_address'].get('district', ''),
                first_line=record['pickup_address'].get('first_line', ''),
                geo_location_lat=record['pickup_address'].get('geo_location_lat', 0),
                geo_location_long=record['pickup_address'].get('geo_location_long', 0),
                country_id=record['pickup_address'].get('country_id', ''),
                country_name=record['pickup_address'].get('country_name', ''),
                country_code=record['pickup_address'].get('country_code', '')
            )
            
            existing_record = db.query(PickupAddress).filter(PickupAddress.id == pickup_address.id).first()
            if existing_record:
                db.delete(existing_record)  
            db.add(pickup_address)

            receiver = Receiver(
                id=record['receiver']['id'],
                first_name=record['receiver'].get('first_name', ''),
                last_name=record['receiver'].get('last_name', ''),
                phone=record['receiver'].get('phone', '')
            )
            
            existing_record = db.query(Receiver).filter(Receiver.id == receiver.id).first()
            if existing_record:
                db.delete(existing_record)  # Remove existing record
            db.add(receiver)

            star = Star(
                id=record['star']['id'],
                name=record['star'].get('name', ''),
                phone=record['star'].get('phone', '')
            )
            
            existing_record = db.query(Star).filter(Star.id == star.id).first()
            if existing_record:
                db.delete(existing_record)  # Remove existing record
            db.add(star)

            tracker = Tracker(
                id=record['tracker']['id'],
                tracker_id=record['tracker'].get('tracker_id', ''),
                order_id=record['tracker'].get('order_id', '')
            )
            
            existing_record = db.query(Tracker).filter(Tracker.id == tracker.id).first()
            if existing_record:
                db.delete(existing_record)  # Remove existing record
            db.add(tracker)

            order = Order(
                id=record['order_id'],
                type=record.get('type', ''),
                updated_at=record.get('updated_at', datetime.now()),
                drop_off_address_id=record['drop_off_address']['id'],
                pickup_address_id=record['pickup_address']['id'],
                receiver_id=record['receiver']['id'],
                star_id=record['star']['id'],
                tracker_id=record['tracker']['id']
            )
            
            existing_record = db.query(Order).filter(Order.id == order.id).first()
            if existing_record:
                db.delete(existing_record)  # Remove existing record
            db.add(order)

        db.commit()
    except Exception as e:
        db.rollback()
        raise e

def main():
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Sample JSON data
    json_data = [
        {
            "_id": "ThttzTTL76Low9Xav",
            "cod": {
                "amount": 330,
                "isPaidBack": False,
                "collectedAmount": 330
            },
            "collectedFromBusiness": {
                "$date": "2020-01-01T15:04:53.1222"
            },
            "confirmation": {
                "isConfirmed": False,
                "numberOfSmsTrials": 0
            },
            "createdAt": {
                "$date": "2020-01-01T06:03:20.687Z"
            },
            "dropOffAddress": {
                "secondLine": "Giza, Pyramids Gardens",
                "city": {
                    "_id": "FchhHXwpSYYF9zGW"
                },
                "name": "Cairo",
                "zone": {
                    "name": "October"
                },
                "_id": "jMYRvhhheSXsiYH",
                "district": "6th of October City",
                "firstLine": "6th of October City, Egypt",
                "geoLocation": [30.9187827, 29.9285429]
            },
            "pickupAddress": {
                "floor": "2",
                "apartment": "12",
                "secondLine": "27 Dr Mohamed Yusuf Moussa street, New Cairo",
                "city": {
                    "_id": "FceDyHXwpSYYF9zGW"
                },
                "name": "Cairo",
                "zone": {
                    "name": "Cairo",
                    "id": "KxzeJ5RZEszYYbok9"
                },
                "district": "New Cairo",
                "firstLine": "New Cairo, Cairo",
                "geoLocation": [31.3301076, 30.0566104],
                "country": {
                    "_id": "60e4482c7cb7d4bc4849c4d5",
                    "name": "Egypt",
                    "code": "EG"
                }
            },
            "receiver": {
                "_id": "mGokM6pxZ6Erc8FYn",
                "firstName": "Ahmed",
                "lastName": "Hesham",
                "phone": "+20101111111"
            },
            "star": {
                "_id": "BSvweJSg4zkQ6s7Ja",
                "name": "Mahmoud Ali",
                "phone": "1111316544"
            },
            "tracker": {
                "trackerId": "c9rrWi5vWyY35qEwKa",
                "order_id": "4233895"
            },
            "order_id": "4233895",
            "type": "SEND",
            "updatedAt": {
                "$date": "2021-05-23T13:53:28.4482"
            }
        }
        
    ]
    try:
        # Transform JSON data
        transformed_data = transform_json(json_data)
        
        # Load data to database
        db = SessionLocal()
        load_data_to_db(transformed_data, db)
        db.close()
        print("ETL job completed successfully.")
    except Exception as e:
        # Print error details
        error_message = f"Error: {str(e)}\n\nTraceback: {traceback.format_exc()}"
        print(error_message)
        # send_failure_email(error_message)
        print("ETL job failed. Notification sent.")

if __name__ == "__main__":
    main()
