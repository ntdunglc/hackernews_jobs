import json
import csv
import argparse
import logging
from typing import Dict, Any, List
from datetime import datetime
from models import JobPosting

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
    job_posting: JobPosting, original: Dict[str, Any]
) -> Dict[str, str]:
    return {
        "URL": f'https://news.ycombinator.com/item?id={original["id"]}',
        "Time": format_timestamp(original["time"]),
        "Company Name": job_posting.company_name,
        "Positions": ", ".join(job_posting.positions),
        "Location": job_posting.location,
        "Job Type": job_posting.job_type,
        "Salary Range": job_posting.salary_range or "",
        "Benefits": ", ".join(job_posting.benefits),
        "Required Skills": ", ".join(job_posting.required_skills),
        "Additional Requirements": ", ".join(job_posting.additional_requirements),
        "Work Environment": job_posting.work_environment or "",
        "Application Instructions": job_posting.application_instructions or "",
        "Is Remote": str(job_posting.is_remote),
        "Is Remote in US": str(job_posting.is_remote_in_us),
        "Is Remote Global": str(job_posting.is_remote_global),
        "Timezone": job_posting.timezone or "",
        "Industry": job_posting.industry or "",
        "Startup Series": job_posting.startup_series,
        "Is ML": str(job_posting.is_ml),
        "Is Datacenter": str(job_posting.is_datacenter),
        "Years of Experience": job_posting.year_of_experience or "",
        "Job Description": job_posting.job_description,
        "Original Text": original.get("text", ""),
    }


def json_to_csv(json_file: str, csv_file: str):
    # Load the JSON data
    data = load_json(json_file)

    # Parse the classified comments into JobPosting objects
    classified_comments: List[Dict[str, Any]] = []
    for item in data["classified_comments"]:
        try:
            job_posting = JobPosting.model_validate(item["classified"])
            classified_comments.append(
                {"original": item["original"], "classified": job_posting}
            )
        except Exception as e:
            logging.error(f"Error parsing job posting: {e}")
            continue

    if not classified_comments:
        logging.error("No valid job postings found.")
        return

    # Prepare the data for CSV
    headers = list(
        flatten_job_posting(
            classified_comments[0]["classified"], classified_comments[0]["original"]
        ).keys()
    )
    rows = []
    for item in classified_comments:
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
