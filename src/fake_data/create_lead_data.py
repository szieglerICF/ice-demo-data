import json
import random
from datetime import datetime
from uuid import uuid4

import pandas as pd
from faker import Faker
from indic_transliteration import sanscript
from indic_transliteration.sanscript import transliterate

# Configuration data
config_data = {
    "Mexico": {
        "country_code": "MX",
        "faker_locale": "es_MX",
        "count": 10,
        "destinations": [
            {
                "city": "Los Angeles",
                "state": "CA",
                "latitude": 34.0522,
                "longitude": -118.2437,
            },
            {
                "city": "Houston",
                "state": "TX",
                "latitude": 29.7604,
                "longitude": -95.3698,
            },
            {
                "city": "Chicago",
                "state": "IL",
                "latitude": 41.8781,
                "longitude": -87.6298,
            },
            {
                "city": "Dallas",
                "state": "TX",
                "latitude": 32.7767,
                "longitude": -96.7970,
            },
            {
                "city": "Phoenix",
                "state": "AZ",
                "latitude": 33.4484,
                "longitude": -112.0740,
            },
        ],
    },
    "El Salvador": {
        "country_code": "SV",
        "faker_locale": "es_MX",
        "count": 10,
        "destinations": [
            {
                "city": "Los Angeles",
                "state": "CA",
                "latitude": 34.0522,
                "longitude": -118.2437,
            },
            {
                "city": "Washington",
                "state": "DC",
                "latitude": 38.9072,
                "longitude": -77.0369,
            },
            {
                "city": "Houston",
                "state": "TX",
                "latitude": 29.7604,
                "longitude": -95.3698,
            },
            {
                "city": "New York City",
                "state": "NY",
                "latitude": 40.7128,
                "longitude": -74.0060,
            },
            {
                "city": "Dallas",
                "state": "TX",
                "latitude": 32.7767,
                "longitude": -96.7970,
            },
        ],
    },
    "Guatemala": {
        "country_code": "GT",
        "faker_locale": "es_MX",
        "count": 10,
        "destinations": [
            {
                "city": "Los Angeles",
                "state": "CA",
                "latitude": 34.0522,
                "longitude": -118.2437,
            },
            {
                "city": "Houston",
                "state": "TX",
                "latitude": 29.7604,
                "longitude": -95.3698,
            },
            {
                "city": "New York City",
                "state": "NY",
                "latitude": 40.7128,
                "longitude": -74.0060,
            },
            {
                "city": "San Francisco",
                "state": "CA",
                "latitude": 37.7749,
                "longitude": -122.4194,
            },
            {
                "city": "Providence",
                "state": "RI",
                "latitude": 41.8240,
                "longitude": -71.4128,
            },
        ],
    },
    "Honduras": {
        "country_code": "HN",
        "faker_locale": "es_MX",
        "count": 10,
        "destinations": [
            {
                "city": "Houston",
                "state": "TX",
                "latitude": 29.7604,
                "longitude": -95.3698,
            },
            {
                "city": "New York City",
                "state": "NY",
                "latitude": 40.7128,
                "longitude": -74.0060,
            },
            {
                "city": "Miami",
                "state": "FL",
                "latitude": 25.7617,
                "longitude": -80.1918,
            },
            {
                "city": "Los Angeles",
                "state": "CA",
                "latitude": 34.0522,
                "longitude": -118.2437,
            },
            {
                "city": "New Orleans",
                "state": "LA",
                "latitude": 29.9511,
                "longitude": -90.0715,
            },
        ],
    },
    "India": {
        "country_code": "IN",
        "faker_locale": "hi_IN",
        "count": 10,
        "destinations": [
            {
                "city": "New York City",
                "state": "NY",
                "latitude": 40.7128,
                "longitude": -74.0060,
            },
            {
                "city": "San Francisco Bay Area",
                "state": "CA",
                "latitude": 37.7749,
                "longitude": -122.4194,
            },
            {
                "city": "Chicago",
                "state": "IL",
                "latitude": 41.8781,
                "longitude": -87.6298,
            },
            {
                "city": "Houston",
                "state": "TX",
                "latitude": 29.7604,
                "longitude": -95.3698,
            },
            {
                "city": "Dallas",
                "state": "TX",
                "latitude": 32.7767,
                "longitude": -96.7970,
            },
        ],
    },
}

indian_names = {
    "male_first_names": [
        "Aarav",
        "Advait",
        "Akshay",
        "Amit",
        "Anirudh",
        "Arjun",
        "Deepak",
        "Harish",
        "Kunal",
        "Manish",
        "Mohan",
        "Nitin",
        "Pranav",
        "Rahul",
        "Rajesh",
        "Rohan",
        "Sandeep",
        "Sanjay",
        "Suraj",
        "Vikram",
    ],
    "female_first_names": [
        "Aditi",
        "Ananya",
        "Bhavya",
        "Deepika",
        "Divya",
        "Geeta",
        "Harini",
        "Ishita",
        "Kavya",
        "Meera",
        "Nisha",
        "Pooja",
        "Priya",
        "Radha",
        "Riya",
        "Sakshi",
        "Sanya",
        "Shreya",
        "Sneha",
        "Swati",
    ],
    "last_names": [
        "Agarwal",
        "Bhat",
        "Choudhury",
        "Deshmukh",
        "Ghosh",
        "Iyer",
        "Jha",
        "Joshi",
        "Kapoor",
        "Khanna",
        "Mehta",
        "Menon",
        "Mishra",
        "Nair",
        "Patel",
        "Reddy",
        "Sharma",
        "Singh",
        "Varma",
        "Yadav",
    ],
    "combined_last_names": [
        "Agarwal Sharma",
        "Bhat Joshi",
        "Choudhury Singh",
        "Deshmukh Patil",
        "Ghosh Mukherjee",
        "Iyer Subramanian",
        "Jha Pandey",
        "Joshi Kulkarni",
        "Kapoor Khanna",
        "Mehta Shah",
        "Menon Nair",
        "Mishra Tiwari",
        "Patel Reddy",
        "Reddy Rao",
        "Sharma Verma",
        "Singh Chauhan",
        "Tiwari Tripathi",
        "Varma Saxena",
        "Yadav Chaturvedi",
        "Chatterjee Sen",
    ],
}


# Generate fake immigrant records
fake_records = []

for country, details in config_data.items():
    faker_locale = details["faker_locale"]
    count = details["count"]
    destinations = details["destinations"]

    fake = Faker(faker_locale)

    for _ in range(count):
        gender = fake.random_element(["M", "F"])
        height_feet = random.randint(4, 6)
        height_inches = random.randint(0, 11) + 1
        weight = random.randint(100, 250)
        fingerprint_hash = uuid4().hex[0:10]
        first_name = fake.first_name()
        if faker_locale == "es_MX" and random.random() < 0.15:
            last_name = f"{fake.last_name()} {fake.last_name()}"
        else:
            last_name = fake.last_name()

        if faker_locale == "hi_IN":
            first_name = random.choice(
                indian_names["male_first_names"]
                if fake.random_element(["male", "female"]) == "male"
                else indian_names["female_first_names"]
            )
            last_name = random.choice(indian_names["last_names"])
            additional_last_names = ""
            if random.random() < 0.15:
                additional_last_names = " ".join(
                    [
                        random.choice(indian_names["last_names"])
                        for _ in range(random.choice([1, 2]))
                    ]
                )
            last_name = f"{last_name} {additional_last_names}"
        country_of_origin = country
        last_known_address = random.choice(destinations)
        full_address = f"{last_known_address['city']}, {last_known_address['state']}"

        # Add random degrees within up to 50 miles (~0.7 degrees latitude/longitude)
        latitude = last_known_address["latitude"] + random.uniform(-0.7, 0.7)
        longitude = last_known_address["longitude"] + random.uniform(-0.7, 0.7)

        dob = fake.date_of_birth(minimum_age=18, maximum_age=65).strftime("%Y-%m-%d")   
        lead_source = fake.random_element(["CBP", "Local LE", "Tip", "Other"])

        fake_records.append(
            {
                "Lead Source": lead_source,
                "First Name": first_name,
                "Last Name": last_name,
                "Date of Birth": dob,
                "Country of Origin": country_of_origin,
                "Gender" : gender,
                "Height (feet)": height_feet,
                "Height (inches)": height_inches,
                "Weight": weight,
                "Fingerprint Hash": fingerprint_hash,
                "Last Known Address": full_address,
                "Latitude": latitude,
                "Longitude": longitude,
            }
        )
        # # Introduce dirty data
        # for record in fake_records:
        #     if random.random() < 0.01:
        #         field_to_nullify = random.choice(
        #             ["First Name", "Last Name", "Last Known Address"]
        #         )
        #         record[field_to_nullify] = None

# Convert to DataFrame
df = pd.DataFrame(fake_records)

# Save to CSV
csv_filename = "fake_immigrant_records.csv"
df.to_csv(csv_filename, index=False, encoding="utf-8")

print(f"Fake immigrant records have been saved to {csv_filename}")
