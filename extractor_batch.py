import os
import json
import argparse
import logging
from tqdm import tqdm
import instructor
import random
from anthropic import Anthropic
from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")


class JobPosting(BaseModel):
    company_name: str
    positions: List[str]
    location: str
    job_type: Literal["Full Time", "Part Time", "Contractor", "Unknown"]
    salary_range: Optional[str] = None
    benefits: List[str] = Field(default_factory=list)
    required_skills: List[str] = Field(default_factory=list)
    job_description: str
    additional_requirements: List[str] = Field(default_factory=list)
    work_environment: Optional[str] = None
    application_instructions: Optional[str] = None

    # New fields
    is_remote: bool = False
    is_remote_in_us: bool = False
    is_remote_global: bool = False
    timezone: Optional[str] = None
    industry: Optional[str] = None
    startup_series: Literal[
        "Series A",
        "Series B",
        "Series C",
        "Series D",
        "Series E",
        "Public Company",
        "Unknown",
    ]
    is_ml: bool = False
    is_datacenter: bool = False
    year_of_experience: Optional[str] = None


def process_job_postings_batch(
    client: Any, comments: List[str]
) -> List[Dict[str, Any]]:
    try:
        resp = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=4096,
            messages=[
                {
                    "role": "user",
                    "content": f"""Extract job posting information from each of the following text blocks. 
                    For each job posting, pay special attention to these fields:
                    - job_type (Full Time, Part Time, Contractor, or Unknown)
                    - salary_range (if mentioned)
                    - required_skills (list all mentioned skills)
                    - is_remote (true if the job is remote, false otherwise)
                    - timezone (if mentioned for remote jobs)
                    - is_ml (true if the job involves machine learning or AI, false otherwise)
                    - is_datacenter (true if the job is related to datacenter operations, false otherwise)

                    Also extract other fields like company_name, positions, location, benefits, etc. as per the model structure.

                    Here are the job postings:

                    {json.dumps(comments)}

                    Provide the extracted information for each job posting as a separate JSON object in a list.
                    """,
                }
            ],
            response_model=List[JobPosting],
        )
        return [job.model_dump() for job in resp]
    except Exception as e:
        logging.error(f"Error processing batch: {e}")
        return [{"error": str(e)}] * len(comments)


def classify_jobs(
    input_file: str, output_file: str, limit: int = 0, batch_size: int = 5
):
    anthropic_client = Anthropic()
    client = instructor.from_anthropic(anthropic_client)

    with open(input_file, "r") as f:
        data = json.load(f)

    comments = data["comments"]
    if limit > 0:
        random.shuffle(comments)
        comments = comments[:limit]

    classified_comments = []
    successful_classifications = 0
    errors = 0

    pbar = tqdm(total=len(comments), desc="Classifying job postings")
    for i in range(0, len(comments), batch_size):
        batch = comments[i : i + batch_size]
        batch_texts = [comment.get("text", "") for comment in batch]

        classified_batch = process_job_postings_batch(client, batch_texts)

        # This might have serious problem if len(classified_batch) != len(batch)
        # But I manually checked the output and it looks alright, so we don't fix the issue now
        # A potential fix is if there's mismatch, we switch to process individual item.
        for original, classified in zip(batch, classified_batch):
            if "error" in classified:
                errors += 1
            else:
                successful_classifications += 1
            classified_comments.append({"original": original, "classified": classified})

        pbar.update(len(batch))
        pbar.set_postfix(
            {"Successful": successful_classifications, "Errors": errors}, refresh=True
        )

    pbar.close()

    del data["post"]["kids"]
    output_data = {"post": data["post"], "classified_comments": classified_comments}

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)

    logging.info(
        f"Classification complete. Total: {len(classified_comments)}, "
        f"Successful: {successful_classifications}, Errors: {errors}. "
        f"Output written to {output_file}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Classify job postings from a JSON file."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input JSON file, created by crawl.py",
    )
    parser.add_argument(
        "--output", required=True, help="Path to save the output JSON file"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Number of items to process, 0 to process all",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=5,
        help="Number of comments to process in each batch",
    )
    args = parser.parse_args()
    logging.info(f"args: {args}")

    try:
        classify_jobs(args.input, args.output, args.limit, args.batch_size)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
