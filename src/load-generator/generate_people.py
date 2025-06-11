from faker import Faker
import json
from datetime import datetime, timedelta
import random

def generate_people(num_people=1000):
    fake = Faker()
    people = []
    
    # Set seed for reproducibility if needed
    # fake.seed(1234)
    min_year = datetime.now().year + 1  # ensure > May 2025
    max_year = min_year + 14 
    for _ in range(num_people):
        # Generate random dates
        start_date = datetime(2020, 1, 1)
        end_date = datetime.now()
        random_days = random.randint(0, (end_date - start_date).days)
        created_at = start_date + timedelta(days=random_days)
        
        # Ensure expiration is always in future
        exp_year = random.randint(min_year, max_year)
        exp_month = random.randint(1, 12)

        # Generate random person
        person = {
            "email": fake.email(),
            "address": {
                "streetAddress": fake.street_address(),
                "zipCode": fake.zipcode(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": "United States"
            },
            "userCurrency": "USD",
            "creditCard": {
                "creditCardExpirationMonth": exp_month,
                "creditCardExpirationYear": exp_year,
                "creditCardCvv": random.randint(100, 999)
            }
        }
        people.append(person)
    
    return people

def main():
    # Generate 100 people
    people = generate_people(1000)
    
    # Write to people.json
    with open('people_1000.json', 'w') as f:
        json.dump(people, f, indent=2)
    
    print(f"Generated people.json with {len(people)} entries")

if __name__ == "__main__":
    main()
