import csv
from faker import Faker
import random
import string
from google.cloud import storage

# Initialize Faker
fake = Faker()

# Function to generate a random password
def generate_password(length=10):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for i in range(length))

# Function to generate dummy employee data
def generate_employee_data(num_records):
    employee_data = []
    
    for _ in range(num_records):
        employee_id = fake.unique.random_int(min=100000, max=99990000)
        name = fake.name()
        job_title = fake.job()
        department = fake.random_element(elements=("Finance", "Engineering", "Sales", "HR", "Marketing"))
        email = fake.email()
        phone_number = fake.phone_number()
        hire_date = fake.date_between(start_date='-10y', end_date='today')
        password = generate_password()
        salary = fake.random_int(min=30000, max=120000)
        
        employee_data.append([
            employee_id,
            name,
            job_title,
            department,
            email,
            phone_number,
            hire_date,
            password,
            salary
        ])
    
    return employee_data

# Function to upload file to GCS
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    # Initialize a GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # Upload the file
    blob.upload_from_filename(source_file_name)
    
    print(f'File {source_file_name} uploaded to {destination_blob_name}.')
# Number of dummy records to generate
num_records = 10000

# Generate employee data
data = generate_employee_data(num_records)

# Define CSV file name
csv_file = 'dummy_employee_data.csv'

# Write data to CSV file
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Employee ID", "Name", "Job Title", "Department", "Email", "Phone Number", "Hire Date", "Password", "Salary"])
    writer.writerows(data)

print(f'Dummy employee data generated and saved to {csv_file}')

bucket_name = 'employee-data-divyansh'
destination_blob_name = 'path/in/gcs/dummy_employee_data.csv'

upload_to_gcs(bucket_name, csv_file, destination_blob_name)
