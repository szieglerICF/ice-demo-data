import csv
import json
import random
from datetime import datetime, timedelta
from uuid import uuid4

import pandas as pd
from dateutil import parser
from faker import Faker
from icecream import ic
from indic_transliteration import sanscript
from indic_transliteration.sanscript import transliterate

# Configuration data
PERCENT_DUPES = 0.15
mexico_count = 2800
el_salvador_count = int(mexico_count * 0.6)
guatemala_count = int(mexico_count * 0.4)
honduras_count = int(mexico_count * 0.5)
india_count = int(mexico_count * 0.1)
config_data = {
    "Mexico": {
        "country_code": "MX",
        "faker_locale": "es_MX",
        "count": mexico_count,
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
            {
                "city": "El Paso",
                "state": "TX",
                "latitude": 31.7619,
                "longitude": -106.4850,
            },
            {
                "city": "Laredo",
                "state": "TX",
                "latitude": 27.5306,
                "longitude": -99.4803,
            },
            {
                "city": "McAllen",
                "state": "TX",
                "latitude": 26.2034,
                "longitude": -98.2300,
            },
            {
                "city": "Brownsville",
                "state": "TX",
                "latitude": 25.9017,
                "longitude": -97.4975,
            },
            {
                "city": "Eagle Pass",
                "state": "TX",
                "latitude": 28.7091,
                "longitude": -100.4995,
            },
        ],
    },
    "El Salvador": {
        "country_code": "SV",
        "faker_locale": "es_MX",
        "count": el_salvador_count,
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
            {
                "city": "El Paso",
                "state": "TX",
                "latitude": 31.7619,
                "longitude": -106.4850,
            },
            {
                "city": "Laredo",
                "state": "TX",
                "latitude": 27.5306,
                "longitude": -99.4803,
            },
            {
                "city": "McAllen",
                "state": "TX",
                "latitude": 26.2034,
                "longitude": -98.2300,
            },
        ],
    },
    "Guatemala": {
        "country_code": "GT",
        "faker_locale": "es_MX",
        "count": guatemala_count,
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
            },            {
                "city": "Brownsville",
                "state": "TX",
                "latitude": 25.9017,
                "longitude": -97.4975,
            },
            {
                "city": "Eagle Pass",
                "state": "TX",
                "latitude": 28.7091,
                "longitude": -100.4995,
            },
        ],
    },
    "Honduras": {
        "country_code": "HN",
        "faker_locale": "es_MX",
        "count": honduras_count,
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
        "count": india_count,
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


def create_indian_name(sex):
    first_name = random.choice(
        indian_names["male_first_names"]
        if sex == "M"
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
    return first_name, last_name


def create_name(faker_locale, sex):
    fake = Faker(faker_locale)
    fake_us = Faker("en_US")
    first_name = fake.first_name()
    if sex == "M":
        first_name = fake.first_name_male()
    else:
        first_name = fake.first_name_female()

    if faker_locale == "es_MX" and random.random() < 0.15:
        last_name = f"{fake.last_name()} {fake.last_name()}"
    else:
        last_name = fake.last_name()

    if faker_locale == "hi_IN":
        first_name, last_name = create_indian_name(sex)
    if faker_locale == "en_US":
        first_name = fake_us.first_name()
        last_name = fake_us.last_name()
    return first_name, last_name


# def funtion to take street and introduce up to n leveinstein distance errors and any given string
def introduce_spelling_errors(text, n):
    if len(str(text)) <= 3:
        return text

    # Introduce up to n Levenshtein distance errors
    j = random.randint(0, len(text) - 1)

    # Use equal random chance to make one of the errors
    error_type = random.choice([1, 2, 3, 4, 5])
    if error_type == 1:
        # Insert character
        char_to_insert = random.choice("abcdefghijklmnopqrstuvwxyz")
        text = text[:j] + char_to_insert + text[j:]
    elif error_type == 2:
        # Delete character
        text = text[:j] + text[j + 1 :]
    elif error_type == 3:
        # Substitute character
        char_to_substitute = random.choice("abcdefghijklmnopqrstuvwxyz")
        text = text[:j] + char_to_substitute + text[j + 1 :]
    elif error_type == 4:
        # Transpose character
        if j < len(text) - 1:
            text = text[:j] + text[j + 1] + text[j] + text[j + 2 :]
    elif error_type == 5:
        # Change case
        text = text[:j] + text[j].swapcase() + text[j + 1 :]

    return text


def clear_out_random_fields(record):
    changeable_fields = [
        "date_of_birth",
        "anumber",
        "height_inches",
        "weight",
        "last_known_address",
        "immigration_status",
        "visa_type",
        "visa_expiration_date",
        "known_border_crossings",
        "past_deportations",
        "current_location",
        "alias",
        "phone_number",
        "known_associates",
        "organized_crime_links",
        "case_officer_assigned",
        "investigation_status",
        "legal_proceedings",
        "deportation_orders",
    ]
    count_of_fields_changed = random.randint(3, 7)
    fields_changed = random.sample(changeable_fields, count_of_fields_changed)
    for field in fields_changed:
        record[field] = ""
    return record


def all_caps_random_fields(record):
    changeable_fields = [
        "first_name",
        "last_name",
        "date_of_birth",
        "anumber",
        "sex",
        "height_inches",
        "weight",
        "last_known_address",
        "latitude",
        "longitude",
        "immigration_status",
        "visa_type",
        "visa_expiration_date",
        "known_border_crossings",
        "past_deportations",
        "current_location",
        "alias",
        "phone_number",
        "known_associates",
        "organized_crime_links",
        "case_officer_assigned",
        "investigation_status",
        "legal_proceedings",
        "deportation_orders",
    ]
    count_of_fields_changed = random.randint(3, 7)
    fields_changed = random.sample(changeable_fields, count_of_fields_changed)
    for field in fields_changed:
        # if string, capitalize
        if isinstance(record[field], str):
            record[field] = record[field].upper()
    return record


def misspell_random_fields(record):
    changeable_fields = [
        "height_inches",
        "weight",
        "last_known_address",
        "latitude",
        "longitude",
        "immigration_status",
        "visa_type",
        "visa_expiration_date",
        "known_border_crossings",
        "past_deportations",
        "current_location",
        "alias",
        "phone_number",
        "known_associates",
        "organized_crime_links",
        "case_officer_assigned",
        "investigation_status",
        "legal_proceedings",
        "deportation_orders",
    ]
    count_of_fields_changed = random.randint(3, 7)
    fields_changed = random.sample(changeable_fields, count_of_fields_changed)
    for field in fields_changed:
        # if the field is text, not a number, introduce spelling errors
        if isinstance(record[field], str):
            record[field] = introduce_spelling_errors(record[field], 1)
    return record


def has_accented_chars(text):
    accented_chars = ["á", "é", "í", "ó", "ú", "ñ", "ü"]
    for char in accented_chars:
        if char in text:
            return True
    return False


# replace all chars with accented marks to the American English char
def replace_accented_chars(text):
    text = text.replace("á", "a")
    text = text.replace("é", "e")
    text = text.replace("í", "i")
    text = text.replace("ó", "o")
    text = text.replace("ú", "u")
    text = text.replace("ñ", "n")
    text = text.replace("ü", "u")
    return text


def add_duplicate_record(
    base_record, faker_locale, sex, duplicate_lead_id, duplicate_lead_source
):
    dupe = base_record.copy()

    possible_changes = [
        "name change",
        "birthday change",
        "address change",
        "anumber change",
    ]
    dupe_reason = random.choice(possible_changes)

    if dupe_reason == "name change":
        name_change_options = [
            "change first name",
            "change last name",
            "flip names",
            "misspelling",
        ]

        name_change_option = random.choice(name_change_options)

        if name_change_option == "change first name":
            first_name, last_name = create_name(faker_locale, sex)
            dupe["first_name"] = first_name
        if name_change_option == "change last name":
            first_name, last_name = create_name(faker_locale, sex)
            dupe["last_name"] = last_name
        if name_change_option == "flip names":
            dupe["first_name"], dupe["last_name"] = (
                base_record["last_name"],
                base_record["first_name"],
            )
        if name_change_option == "misspelling":
            if random.random() < 0.50:
                dupe["first_name"] = introduce_spelling_errors(
                    base_record["first_name"], 1
                )
            else:
                dupe["last_name"] = introduce_spelling_errors(
                    base_record["last_name"], 1
                )

        dupe_reason += " - " + name_change_option

    if dupe_reason == "birthday change":
        birthday = parser.parse(base_record["date_of_birth"])
        delta = random.choice([-1, 1])
        new_birthday = birthday + timedelta(days=delta)
        dupe["date_of_birth"] = new_birthday.strftime("%Y-%m-%d")

    if dupe_reason == "address change":
        new_address = (
            fake_us.street_address()
            + ", "
            + fake_us.city()
            + ", "
            + fake_us.state()
            + " "
            + fake_us.zipcode()
        )
        dupe["last_known_address"] = new_address

    if dupe_reason == "anumber change":
        dupe["anumber"] = "A" + str(fake.random_int(min=1000000, max=999999999))

    dupe["lead_id"] = uuid4().hex[0:16]
    dupe["lead_source"] = lead_source = fake.random_element(
        ["CBP", "Local LE", "Tip", "Other"]
    )
    dupe["is_dupe"] = True
    dupe["dupe_reason"] = dupe_reason
    # convert record date from string to datetime
    record_date = parser.parse(base_record["record_date"]) + timedelta(
        days=random.randint(180, 365)
    )
    dupe["record_date"] = record_date.strftime("%Y-%m-%d")

    dupe = clear_out_random_fields(dupe)
    dupe = misspell_random_fields(dupe)
    dupe = all_caps_random_fields(dupe)
    dupe["first_name"] = replace_accented_chars(dupe["first_name"])
    dupe["last_name"] = replace_accented_chars(dupe["last_name"])

    return dupe


for country, details in config_data.items():
    print(country)
    faker_locale = details["faker_locale"]
    count = details["count"]
    destinations = details["destinations"]

    fake = Faker(faker_locale)
    fake_us = Faker("en_US")

    for _ in range(count):
        lead_id = uuid4().hex[0:16]
        duplicate_lead_id = uuid4().hex[0:16]
        record_date = fake.date_this_decade().strftime("%Y-%m-%d")

        sex = fake.random_element(["M", "F"])
        first_name, last_name = create_name(faker_locale, sex)
        height_inches = random.randint(
            58, 78
        )  # Total height in inches for average adult human range
        weight = random.randint(100, 250)
        # anumber is "A" followed by 7-9 digits
        alien_number = "A" + str(fake.random_int(min=1000000, max=999999999))
        distinquishing_marks = ""
        # 20% of time show distinquishing marks
        if random.random() < 0.2:
            distinquishing_marks = fake.random_element(
                [
                    "Tattoo on left arm",
                    "Tattoo on right arm",
                    "Tattoo on back",
                    "Tattoo on chest",
                    "Tattoo on left leg",
                    "Tattoo on right leg",
                    "Tattoo on neck",
                    "Tattoo on face",
                    "Scar on right cheek",
                    "Scar on left cheek",
                    "Scar on forehead",
                    "Scar on chin",
                    "Scar on right arm",
                    "Scar on left arm",
                    "Scar on right leg",
                    "Scar on left leg",
                    "Birthmark on right arm",
                    "Birthmark on left arm",
                    "Birthmark on right leg",
                    "Birthmark on left leg",
                    "Birthmark on face",
                    "Birthmark on neck",
                    "Birthmark on back",
                    "Birthmark on chest",
                ]
            )
        fingerprint_hash = uuid4().hex[0:10]

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
            [
                "Visa Overstay",
                "Undocumented",
                "Pending Asylum",
                "Deported & Reentered",
                "Legal Resident",
            ]
        )
        # •	Visa Type & Expiration Date: (If applicable)
        visa_type = fake.random_element(
            ["", "B1/B2", "F1", "H1B", "H2B", "J1", "L1", "O1", "TN"]
        )
        visa_expiration_date = ""
        if visa_type != "":
            visa_expiration_date = fake.date_between(
                start_date="+1d", end_date="+5y"
            ).strftime("%Y-%m-%d")
        # •	Known Border Crossing(s): Location, Date/Time, and Method of Entry
        known_border_crossings_single = fake.random_element(
            [
                "",
                "San Ysidro, CA",
                "Nogales, AZ",
                "El Paso, TX",
                "Laredo, TX",
                "Detroit, MI",
            ]
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
            alias_first, alias_last = create_name(faker_locale, sex)
            alias = f"{alias_first} {alias_last}"

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
            family_associate_first, family_associate_last = create_name(
                faker_locale, sex
            )
            family_associate = (
                f"{family_associate_first} {family_associate_last} (Family)"
            )
        # 5% of time show known accomplice
        known_accomplice = ""
        if random.random() < 0.05:
            known_accomplice_first, known_accomplice_last = create_name(
                faker_locale, sex
            )
            known_accomplice = (
                f"{known_accomplice_first} {known_accomplice_last} (Accomplice)"
            )

        # •	Organized Crime Links: Ties to gangs, cartels, or trafficking rings
        # 5% of time show organized crime links
        organized_crime_links = ""
        if random.random() < 0.05:
            organized_crime_links = fake.random_element(
                ["Gangs", "Cartels", "Trafficking Rings", ""]
            )

        # if both family associate and known accomplice are present, combine them separated by comma
        if family_associate and known_accomplice:
            known_associates = family_associate + ", " + known_accomplice
        else:
            known_associates = family_associate + known_accomplice

        # •	Case Officer Assigned: (Investigator Name, Badge/ID)
        # 65% of time show case officer assigned
        case_officer_assigned = ""
        if random.random() < 0.65:
            first, last = create_name("en_US", sex)
            case_officer_assigned = (
                f"{last}, {first} (Badge # {fake.random_int(min=1000, max=9999)})"
            )
        if random.random() < 0.45:
            first, last = create_name("en_US", random.choice(["M", "F"]))
            duplicate_case_officer_assigned = (
                f"{first} {last} (Badge # {fake.random_int(min=1000, max=9999)})"
            )

        # •	Investigation Status: (Open, Pending Review, Verified, Closed)
        investigation_status = ""
        if case_officer_assigned:
            investigation_status = fake.random_element(
                ["Open", "Pending Review", "Verified", "Closed"]
            )

        # •	Detention Status: (In Custody, Under Surveillance, Released, Unknown)
        # 30% of time show detention status
        if random.random() < 0.3:
            dentention_status = fake.random_element(
                ["In Custody", "Under Surveillance", "Released", "Unknown", ""]
            )

        # •	Legal Proceedings: Court case reference numbers, asylum claims, appeals
        # 25% of time show legal proceedings
        legal_proceedings = ""
        if random.random() < 0.25:
            reference_number = fake.random_int(min=10000, max=99999)
            legal_proceedings = fake.random_element(
                [
                    f"Case {reference_number}",
                    f"asylum claims {reference_number}",
                    "appeals",
                    "",
                ]
            )

        # •	Deportation Orders: Date, issuing authority, compliance status
        # 50% of time
        deportation_order = ""
        if random.random() < 0.5:
            # choose one of four random options NTA, EOIR deportation decision, appeals, expidited removale
            # if NTA, show deportation order created date
            # if EOIR deportation decision, show deportation order created date, issuing authority, deportation order
            # if appeals, show appeals status
            # if expidited removale, show deportation order created date
            deportation_order = fake.random_element(
                ["NTA", "EOIR deportation decision", "appeals", "expidited removale"]
            )
            if deportation_order == "NTA":
                deportation_order_created = fake.date_this_decade().strftime("%Y-%m-%d")
                issuing_authority = ""
                deportation_order = ""
            elif deportation_order == "EOIR deportation decision":
                deportation_order_created = fake.date_this_decade().strftime("%Y-%m-%d")
                issuing_authority = "EOIR"
            elif deportation_order == "appeals":
                deportation_order = "appeals status"
                deportation_order_created = ""
                issuing_authority = ""
            elif deportation_order == "expidited removal":
                deportation_order_created = fake.date_this_decade().strftime("%Y-%m-%d")
                issuing_authority = "EOIR"
                deportation_order = f"Deportation ordered on {fake.date_this_decade().strftime('%Y-%m-%d')}"

            deportation_order_created = fake.date_this_decade().strftime("%Y-%m-%d")
            issuing_authority = "EOIR"
            deportation_order = fake.random_element(
                [
                    f"Deported on {fake.date_this_decade().strftime('%Y-%m-%d')}",
                    "Compliance status",
                    "",
                ]
            )

        # •	Pending Actions: Further review, ICE/CBP referral, legal stays

        dedupe_id = lead_id

        base_record = {
            "dedupe_id": dedupe_id,
            "is_dupe": False,
            "dupe_reason": "",
            "lead_id": lead_id,
            "record_date": record_date,
            "lead_source": lead_source,
            "first_name": first_name,
            "last_name": last_name,
            "alias": alias,
            "date_of_birth": dob,
            "country_of_origin": country_of_origin,
            "anumber": alien_number,
            "sex": sex,
            "height_inches": height_inches,
            "weight": weight,
            "fingerprint_hash": fingerprint_hash,
            "last_known_address": full_address,
            "legal_proceedings": legal_proceedings,
            "phone_number": phone_number,
            "risk_level": risk_level,
            "distinguishing_marks": distinquishing_marks,
            "latitude": latitude,
            "longitude": longitude,
            "immigration_status": immigration_status,
            "visa_type": visa_type,
            "visa_expiration_date": visa_expiration_date,
            "known_border_crossings": known_border_crossings_single,
            "past_deportations": past_deportation,
            "current_location": current_location,
            "known_associates": known_associates,
            "organized_crime_links": organized_crime_links,
            "case_officer_assigned": case_officer_assigned,
            "investigation_status": investigation_status,
            "deportation_orders": deportation_order,
        }
        fake_records.append(base_record)

        # Add duplicate records

        if random.random() < PERCENT_DUPES:
            number_of_fakes = random.randint(1, 5)
            for i in range(number_of_fakes):
                dupe = add_duplicate_record(
                    base_record,
                    faker_locale,
                    sex,
                    duplicate_lead_id,
                    duplicate_lead_source,
                )
                fake_records.append(dupe)


hardcoded_data = [
    {
        "dedupe_id": "0ffa8be931c44600",
        "is_dupe": False,
        "dupe_reason": "",
        "lead_id": "0ffa8be931c44600",
        "record_date": "2023-10-01",
        "lead_source": "Local LE",
        "first_name": "Emilio",
        "last_name": "Salamanca",
        "date_of_birth": "1997-08-22",
        "country_of_origin": "Mexico",
        "anumber": "",
        "sex": "M",
        "height_inches": 74,
        "weight": 229,
        "distinguishing_marks": "Skull tattoo on calf",
        "fingerprint_hash": "",
        "last_known_address": "688 Jensen Circle Suite 512, Los Angeles, CA 20926",
        "latitude": 34.62970061575755,
        "longitude": -118.07215299038508,
        "past_deportations": "",
        "risk_level": "Low",
        "alias": "",
        "phone_number": "698-686-8675",
        "known_associates": "",
        "organized_crime_links": "",
        "case_officer_assigned": "Mann, Vickie (Badge # 6870)",
        "deportation_orders": "",
        "legal_proceedings": "Arrested for DUI",
    },
    {
        "dedupe_id": "0ffa8be931c44600",
        "is_dupe": True,
        "dupe_reason": "",
        "lead_id": "0ffa8be931c44601",
        "record_date": "2024-06-12",
        "lead_source": "Local LE",
        "first_name": "Emilio",
        "last_name": "Salomanca",
        "date_of_birth": "1997-08-22",
        "country_of_origin": "Mexico",
        "anumber": "A571306955",
        "sex": "M",
        "height_inches": 74,
        "weight": 229,
        "distinguishing_marks": "Skull tattoo on calf",
        "fingerprint_hash": "94k2j82730",
        "last_known_address": "688 Jensen Circle Suite 512, Los Angeles, CA 20926",
        "latitude": 34.62970061575755,
        "longitude": -118.07215299038508,
        "past_deportations": "",
        "risk_level": "High",
        "alias": "Milo",
        "phone_number": "698-686-8657",
        "known_associates": "",
        "organized_crime_links": "",
        "case_officer_assigned": "",
        "deportation_orders": "",
        "legal_proceedings": "Arrested for assault",
    },
    {
        "dedupe_id": "0ffa8be931c44600",
        "is_dupe": True,
        "dupe_reason": "",
        "lead_id": "0ffa8be931c44602",
        "record_date": "2025-01-03",
        "lead_source": "Tip",
        "first_name": "Milo",
        "last_name": "Salamanca",
        "date_of_birth": "1997-08-22",
        "country_of_origin": "Mexico",
        "anumber": "A571306955",
        "sex": "M",
        "height_inches": 74,
        "weight": 229,
        "distinguishing_marks": "Skull tattoo on calf",
        "fingerprint_hash": "94k2j82730",
        "last_known_address": "78654 Chavez Passage, Los Angeles, CA 85700-6041",
        "latitude": 33.53413927,
        "longitude": -117.732472,
        "past_deportations": "",
        "risk_level": "Medium",
        "alias": "",
        "phone_number": "698-686-8675",
        "known_associates": "",
        "organized_crime_links": "",
        "case_officer_assigned": "",
        "deportation_orders": "",
        "legal_proceedings": "",
    },
]

fake_records.extend(hardcoded_data)


fields_to_keep = [
    "dedupe_id",
    "is_dupe",
    "dupe_reason",
    "lead_id",
    "record_date",
    "lead_source",
    "first_name",
    "last_name",
    "date_of_birth",
    "country_of_origin",
    "anumber",
    "sex",
    "height_inches",
    "weight",
    "distinguishing_marks",
    "fingerprint_hash",
    "last_known_address",
    "latitude",
    "longitude",
    "past_deportations",
    "risk_level",
    "alias",
    "phone_number",
    "legal_proceedings",
    "known_associates",
    "organized_crime_links",
    "case_officer_assigned",
    "deportation_orders",
]
assert "record_date" in fake_records[0], "before trimming"

# filter the records to only keep the fields we want
fake_records = [
    {k: v for k, v in record.items() if k in fields_to_keep} for record in fake_records
]
assert "record_date" in fake_records[0]

print(json.dumps(fake_records[0:3], indent=3, default=str))

# add others to each record
for i, record in enumerate(fake_records):
    record["row_id"] = i + 1
    # set authoritative_record to opposite of is_dupe
    record["authoritative_record"] = not record["is_dupe"]
    record["lead_priority"] = ""


# Convert to DataFrame
df = pd.DataFrame(fake_records)

# Save the training version with duplicate training fields
tmsp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
csv_filename = f"ice_data_{tmsp}.csv"
df.to_csv(csv_filename, index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)


print(f"{len(fake_records):,} fake immigrant records have been saved to {csv_filename}")
