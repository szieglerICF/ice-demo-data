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

def create_indian_name():
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
    return first_name,last_name

def create_name( faker_locale):
    fake = Faker(faker_locale)
    fake_us = Faker("en_US")
    first_name = fake.first_name()
    if faker_locale == "es_MX" and random.random() < 0.15:
        last_name = f"{fake.last_name()} {fake.last_name()}"
    else:
        last_name = fake.last_name()

    if faker_locale == "hi_IN":
        first_name, last_name = create_indian_name()
    if faker_locale == "en_US":
        first_name = fake_us.first_name()
        last_name = fake_us.last_name()
    return first_name,last_name

for country, details in config_data.items():
    faker_locale = details["faker_locale"]
    count = details["count"]
    destinations = details["destinations"]

    fake = Faker(faker_locale)
    fake_us = Faker("en_US")

    for _ in range(count):
        lead_id = uuid4().hex[0:16]
        duplicate_lead_id = uuid4().hex[0:16]

        gender = fake.random_element(["M", "F"])
        height_feet = random.randint(4, 6)
        height_inches = random.randint(0, 11) + 1
        weight = random.randint(100, 250)
        fingerprint_hash = uuid4().hex[0:10]
        first_name, last_name = create_name(faker_locale)
        country_of_origin = country
        last_known_address = random.choice(destinations)
        city = last_known_address["city"]
        state = last_known_address["state"]
        street = fake_us.street_address()
        full_address = ""
        zip_code_5 = fake_us.zipcode()
        zip_code_9 = fake_us.zipcode_plus4()
        # 80% of time show full address
        if random.random() < 0.8:
            zip = zip_code_5
            # 30% of time show zip code 9
            if random.random() < 0.3:
                zip = zip_code_9

            full_address = f"{street}, {city}, {state} {zip}"

        # Add random degrees within up to 50 miles (~0.7 degrees latitude/longitude)
        latitude = last_known_address["latitude"] + random.uniform(-0.7, 0.7)
        longitude = last_known_address["longitude"] + random.uniform(-0.7, 0.7)

        dob = fake.date_of_birth(minimum_age=18, maximum_age=65).strftime("%Y-%m-%d")   
        

        lead_source = fake.random_element(["CBP", "Local LE", "Tip", "Other"])
        duplicate_lead_source = fake.random_element(["CBP", "Local LE", "Tip", "Other"])

        # •	Immigration Status: (e.g., Visa Overstay, Undocumented, Pending Asylum, Deported & Reentered, Legal Resident)
        immigration_status = fake.random_element(
            ["Visa Overstay", "Undocumented", "Pending Asylum", "Deported & Reentered", "Legal Resident"]
        )
        # •	Visa Type & Expiration Date: (If applicable)
        visa_type = fake.random_element(["", "B1/B2", "F1", "H1B", "H2B", "J1", "L1", "O1", "TN"])
        visa_expiration_date = ""
        if visa_type != "":
            visa_expiration_date = fake.date_between(start_date="+1d", end_date="+5y").strftime("%Y-%m-%d")
        # •	Known Border Crossing(s): Location, Date/Time, and Method of Entry
        known_border_crossings_single = fake.random_element(
            ["", "San Ysidro, CA", "Nogales, AZ", "El Paso, TX", "Laredo, TX", "Detroit, MI"]
        )

        # •	Past Deportations: Dates, Case Numbers, Deportation Orders
        # 15% of time, simulate past deportation
        past_deportation = ""
        if random.random() < 0.15:
            past_deportation = f"Deported on {fake.date_this_decade().strftime('%Y-%m-%d')} (Case # {fake.random_int(min=10000, max=99999)})"

        # •	Current Location: (e.g., ICE Detention Center, Sanctuary City, Home Address)
        current_location = fake.random_element(
            ["", "ICE Detention Center", "Sanctuary City", "Home Address"]
        )
        # country of origin risk level
        risk_level = fake.random_element(["", "Low", "Medium", "High"])

        # 5% of time show alias
        alias = ""
        if random.random() < 0.05:
            alias = create_name(faker_locale)


        phone_number = fake.phone_number()
        # change area code based on country
        if random.random() < 0.5:
            phone_number = fake.phone_number()
        else:
            if country == "Mexico":
                phone_number = phone_number.replace("+1-", "+52-")
            elif country == "El Salvador":
                phone_number = phone_number.replace("+1-", "+503-")
            elif country == "Guatemala":
                phone_number = phone_number.replace("+1-", "+502-")
            elif country == "Honduras":
                phone_number = phone_number.replace("+1-", "+504-")
            elif country == "India":
                phone_number = phone_number.replace("+1-", "+91-")

        # 10% of time show family assoicate
        family_associate = ""
        if random.random() < 0.1:
            family_associate_first, family_associate_last = create_name(faker_locale) 
            family_associate = f"{family_associate_first} {family_associate_last} (Family)"
        # 5% of time show known accomplice
        known_accomplice = ""
        if random.random() < 0.05:
            known_accomplice_first, known_accomplice_last = create_name(faker_locale) 
            known_accomplice = f"{known_accomplice_first} {known_accomplice_last} (Accomplice)"
        
        # •	Organized Crime Links: Ties to gangs, cartels, or trafficking rings
        # 5% of time show organized crime links
        organized_crime_links = ""
        if random.random() < 0.05:
            organized_crime_links = fake.random_element(["Gangs", "Cartels", "Trafficking Rings", ""])


        # if both family associate and known accomplice are present, combine them separated by comma
        if family_associate and known_accomplice:
            known_associates = family_associate + ", " + known_accomplice
        else:
            known_associates = family_associate + known_accomplice
        
        # •	Case Officer Assigned: (Investigator Name, Badge/ID)
        # 65% of time show case officer assigned
        case_officer_assigned = ""
        if random.random() < 0.65:
            first, last = create_name("en_US")
            case_officer_assigned = f"{last}, {first} (Badge # {fake.random_int(min=1000, max=9999)})"
        if random.random() < 0.45:
            first, last = create_name("en_US")
            duplicate_case_officer_assigned =  f"{first} {last} (Badge # {fake.random_int(min=1000, max=9999)})"

        # •	Investigation Status: (Open, Pending Review, Verified, Closed)
        investigation_status = ""
        if case_officer_assigned:
            investigation_status = fake.random_element(["Open", "Pending Review", "Verified", "Closed"])


        # •	Detention Status: (In Custody, Under Surveillance, Released, Unknown)
        # 30% of time show detention status
        if random.random() < 0.3:
            dentention_status = fake.random_element(["In Custody", "Under Surveillance", "Released", "Unknown", ""])

        # •	Legal Proceedings: Court case reference numbers, asylum claims, appeals
        # 25% of time show legal proceedings
        legal_proceedings = ""
        if random.random() < 0.25:
            reference_number = fake.random_int(min=10000, max=99999)
            legal_proceedings = fake.random_element([f"Case {reference_number}", f"asylum claims {reference_number}", "appeals", ""])

        # •	Deportation Orders: Date, issuing authority, compliance status
        # 50% of time
        deportation_order = ""
        if random.random() < 0.5:
            # choose one of four random options NTA, EOIR deportation decision, appeals, expidited removale
            # if NTA, show deportation order created date
            # if EOIR deportation decision, show deportation order created date, issuing authority, deportation order
            # if appeals, show appeals status
            # if expidited removale, show deportation order created date
            deportation_order = fake.random_element(["NTA", "EOIR deportation decision", "appeals", "expidited removale"])
            if deportation_order == "NTA":
                deportation_order_created = fake.date_this_decade().strftime('%Y-%m-%d')
                issuing_authority = ""
                deportation_order = ""
            elif deportation_order == "EOIR deportation decision":
                deportation_order_created = fake.date_this_decade().strftime('%Y-%m-%d')
                issuing_authority = "EOIR"
            elif deportation_order == "appeals":
                deportation_order = "appeals status"
                deportation_order_created = ""
                issuing_authority = ""
            elif deportation_order == "expidited removal": 
                deportation_order_created = fake.date_this_decade().strftime('%Y-%m-%d')
                issuing_authority = "EOIR"
                deportation_order = f"Deportation ordered on {fake.date_this_decade().strftime('%Y-%m-%d')}"


            deportation_order_created = fake.date_this_decade().strftime('%Y-%m-%d')
            issuing_authority = "EOIR"
            deportation_order = fake.random_element([f"Deported on {fake.date_this_decade().strftime('%Y-%m-%d')}", "Compliance status", ""])

        # •	Pending Actions: Further review, ICE/CBP referral, legal stays

        dedupe_id = lead_id

        fake_records.append(
            {
                "Lead ID": lead_id,
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
                "Immigration Status" : immigration_status,
                "Visa Type": visa_type,
                "Visa Expiration Date": visa_expiration_date,
                "Known Border Crossing(s)": known_border_crossings_single,
                "Past Deportations": past_deportation,
                "Current Location": current_location,
                "Risk Level": risk_level,
                "Alias": alias, 
                "Phone Number": phone_number,
                "Known Associates": known_associates,
                "Organized Crime Links": organized_crime_links,
                "Case Officer Assigned": case_officer_assigned,
                "Investigation Status": investigation_status,
                "Legal Proceedings": legal_proceedings,
                "Deportation Orders": deportation_order,
                "Dedupe ID": dedupe_id,
                "IsDupe": False,
                "Dupe Reason": "",
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
