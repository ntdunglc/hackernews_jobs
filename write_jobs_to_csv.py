import json
import csv
import argparse
import logging
from typing import Dict, Any
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_json(file_path: str) -> Dict[str, Any]:
    with open(file_path, "r") as f:
        return json.load(f)


def format_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def flatten_job_posting(
    job_posting: Dict[str, Any], original: Dict[str, Any]
) -> Dict[str, str]:
    return {
        "URL": f'https://news.ycombinator.com/item?id={original["id"]}',
        "Time": format_timestamp(original["time"]),
        "Company Name": job_posting.get("company_name", ""),
        "Positions": ", ".join(job_posting.get("positions", [])),
        "Location": job_posting.get("location", ""),
        "Job Type": job_posting.get("job_type", ""),
        "Salary Range": job_posting.get("salary_range", ""),
        "Benefits": ", ".join(job_posting.get("benefits", [])),
        "Required Skills": ", ".join(job_posting.get("required_skills", [])),
        "Additional Requirements": ", ".join(
            job_posting.get("additional_requirements", [])
        ),
        "Work Environment": job_posting.get("work_environment", ""),
        "Application Instructions": job_posting.get("application_instructions", ""),
        "Is Remote": str(job_posting.get("is_remote", False)),
        "Is Remote in US": str(job_posting.get("is_remote_in_us", False)),
        "Is Remote Global": str(job_posting.get("is_remote_global", False)),
        "Timezone": job_posting.get("timezone", ""),
        "Industry": job_posting.get("industry", ""),
        "Startup Series": job_posting.get("startup_series", ""),
        "Is ML": str(job_posting.get("is_ml", False)),
        "Is Datacenter": str(job_posting.get("is_datacenter", False)),
        "Years of Experience": job_posting.get("year_of_experience", ""),
        "Job Description": job_posting.get("job_description", ""),
        "Original Text": original.get("text", ""),
    }


def json_to_csv(json_file: str, csv_file: str):
    # Load the JSON data
    data = load_json(json_file)

    # Prepare the data for CSV
    headers = list(
        flatten_job_posting(
            data["classified_comments"][0]["classified"],
            data["classified_comments"][0]["original"],
        ).keys()
    )
    rows = []
    for item in data["classified_comments"]:
        flattened = flatten_job_posting(item["classified"], item["original"])
        rows.append([flattened[header] for header in headers])

    # Write to CSV
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    logging.info(f"CSV file created: {csv_file}")
    logging.info(f"Total rows written: {len(rows)}")


def main():
    parser = argparse.ArgumentParser(description="Convert JSON file to CSV")
    parser.add_argument("--input", required=True, help="Path to the input JSON file")
    parser.add_argument("--output", required=True, help="Path to the output CSV file")
    args = parser.parse_args()

    json_to_csv(args.input, args.output)


if __name__ == "__main__":
    main()
