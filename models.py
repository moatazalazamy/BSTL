from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class DropOffAddress(Base):
    __tablename__ = 'drop_off_address'
    
    id = Column(String, primary_key=True)
    second_line = Column(String)
    city_id = Column(String)
    name = Column(String)
    zone_name = Column(String)
    district = Column(String)
    first_line = Column(String)
    geo_location_lat = Column(Float)
    geo_location_long = Column(Float)

class PickupAddress(Base):
    __tablename__ = 'pickup_address'
    
    id = Column(String, primary_key=True)
    floor = Column(String)
    apartment = Column(String)
    second_line = Column(String)
    city_id = Column(String)
    name = Column(String)
    zone_name = Column(String)
    district = Column(String)
    first_line = Column(String)
    geo_location_lat = Column(Float)
    geo_location_long = Column(Float)
    country_id = Column(String)
    country_name = Column(String)
    country_code = Column(String)

class Receiver(Base):
    __tablename__ = 'receiver'
    
    id = Column(String, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    phone = Column(String)

class Star(Base):
    __tablename__ = 'star'
    
    id = Column(String, primary_key=True)
    name = Column(String)
    phone = Column(String)

class Tracker(Base):
    __tablename__ = 'tracker'
    
    id = Column(String, primary_key=True)
    tracker_id = Column(String)
    order_id = Column(String)

class Order(Base):
    __tablename__ = 'order'
    
    id = Column(String, primary_key=True)
    cod_amount = Column(Integer)
    cod_is_paid_back = Column(Boolean)
    cod_collected_amount = Column(Integer)
    collected_from_business_date = Column(DateTime)
    confirmation_is_confirmed = Column(Boolean)
    confirmation_number_of_sms_trials = Column(Integer)
    created_at = Column(DateTime)
    drop_off_address_id = Column(String, ForeignKey('drop_off_address.id'))
    pickup_address_id = Column(String, ForeignKey('pickup_address.id'))
    receiver_id = Column(String, ForeignKey('receiver.id'))
    star_id = Column(String, ForeignKey('star.id'))
    tracker_id = Column(String, ForeignKey('tracker.id'))
    order_id = Column(String, unique=True)
    type = Column(String)
    updated_at = Column(DateTime)

    drop_off_address = relationship('DropOffAddress')
    pickup_address = relationship('PickupAddress')
    receiver = relationship('Receiver')
    star = relationship('Star')
    tracker = relationship('Tracker')
