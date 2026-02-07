"""Generate mock employee data"""

import random
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from faker import Faker
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

from performance.src.utilities import common


def generate_fake_employees(input_args) -> None:
    """Generates fake employee data using the Faker library and exports it to the specified format (Parquet or CSV)."""
    worker, kwargs = input_args
    fake = Faker("en_US")
    employees = []

    # Define possible job roles and departments
    with open(Path(common.project_path(), "sample_datasets/employee/departments.csv")) as j_fp:
        job_titles = j_fp.readlines()

    with open(Path(common.project_path(), "sample_datasets/employee/departments.csv")) as d_fp:
        departments = d_fp.readlines()

    with open(Path(common.project_path(), "sample_datasets/employee/worker_type.csv")) as wt_fp:
        worker_type = wt_fp.readlines()

    work_location = [fake.city() for _ in range(10)]
    employee_compensation_frequency = ["Monthly", "Annually"]

    for cnt in range(int(kwargs.get("scale_factor") / kwargs.get("num_of_threads"))):
        emp_id = 1000 * worker + cnt
        first_name = fake.first_name()
        last_name = fake.last_name()
        # emp_id = fake.unique.random_number(digits=5)

        employee_data = {
            "employee_id": emp_id,
            "first_name": first_name,
            "last_name": last_name,
            "manager_id": None if emp_id < 10 else random.randint(0, emp_id - 1),
            "email": f"{first_name.lower()}.{last_name.lower()}@fakecompany.com",
            "phone_number": fake.phone_number(),
            "work_location": random.choice(work_location).strip(),
            "worker_type": random.choice(worker_type).strip(),
            "job_title": random.choice(job_titles).strip(),
            "department": random.choice(departments).strip(),
            "annual_summary_currency": "USD",
            "annual_summary_total_base_pay": float(random.randrange(60000, 150000, 5000)),
            "is_hispanic_or_latino": fake.boolean(chance_of_getting_true=20),
            "military_status": fake.boolean(chance_of_getting_true=10),
            "city": fake.city(),
            "country": fake.country(),
            "address": fake.address().replace("\n", ", "),
            "ssn": fake.ssn(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat(),
            "start_date": fake.date_between(start_date="-30y", end_date="today").isoformat(),
            "is_user_active": fake.boolean(chance_of_getting_true=90),
            "compensation_eligible": fake.boolean(chance_of_getting_true=95),
        }
        state_date_datetime = datetime.fromisoformat(employee_data["start_date"]).date()

        # Calculate days employed based on whether the user is active or not
        employee_data["days_employed"] = (
            datetime.today().date() - state_date_datetime
            if employee_data["is_user_active"]
            else fake.date_between(start_date=state_date_datetime, end_date="today") - state_date_datetime
        )

        # Set employment duration flags based on days employed
        employee_data["is_employed_one_year"] = employee_data["days_employed"].days >= 365
        employee_data["is_employed_five_years"] = employee_data["days_employed"].days >= 365 * 5
        employee_data["is_employed_ten_years"] = employee_data["days_employed"].days >= 365 * 10
        employee_data["is_employed_twenty_years"] = employee_data["days_employed"].days >= 365 * 20
        employee_data["is_employed_thirty_years"] = employee_data["days_employed"].days >= 365 * 30

        # Set is_terminated based on is_user_active and randomly mark some terminations as regrettable
        employee_data["is_terminated"] = not employee_data["is_user_active"]
        employee_data["is_regrettable_termination"] = employee_data["is_terminated"] and fake.boolean(
            chance_of_getting_true=30
        )

        # Set terminate_date if the employee is terminated, otherwise set it to None
        employee_data["terminate_date"] = (
            fake.date_between(start_date=state_date_datetime, end_date="today").isoformat()
            if employee_data["is_terminated"]
            else None
        )

        # Set compensation_effective_date if the employee is compensation eligible, otherwise set it to None
        employee_data["compensation_effective_date"] = (
            fake.date_between(start_date=state_date_datetime, end_date="today").isoformat()
            if employee_data["compensation_eligible"]
            else None
        )

        # Set employee_compensation_frequency if the employee is compensation eligible, otherwise set it to None
        employee_data["employee_compensation_frequency"] = (
            random.choice(employee_compensation_frequency) if employee_data["compensation_eligible"] else None
        )
        employees.append(employee_data)

    pyarrow_table = pa.Table.from_pylist(employees)
    if kwargs.get("export_type") == "csv":
        csv.write_csv(pyarrow_table, f"{kwargs.get('target_path')}/part_{worker}.csv")
    elif kwargs.get("export_type") == "parquet":
        pq.write_table(pyarrow_table, f"{kwargs.get('target_path')}/part_{worker}.parquet")


def main(**kwargs) -> None:
    """Main function to generate fake employee data in parallel using multiprocessing.
    It creates the target directory if it doesn't exist, prepares the input arguments for each worker thread,
    and then uses a multiprocessing Pool to execute the data generation function across multiple threads.
    """
    Path(kwargs.get("target_path")).mkdir(parents=True, exist_ok=True)
    input_args = [(i, kwargs) for i in range(kwargs.get("num_of_threads"))]
    with Pool(processes=kwargs.get("num_of_threads")) as pool:
        pool.map(generate_fake_employees, input_args)

    print("Successfully generated fake employee data")


# if __name__ == "__main__":
#     app()
