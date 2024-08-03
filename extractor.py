import os
import json
import argparse
import logging
from tqdm import tqdm
import instructor
from anthropic import Anthropic
from typing import Dict, Any, List
from models import JobPosting
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")

SYSTEM_PROMPT = """You are an AI assistant specialized in extracting job posting information. 
Your task is to analyze job postings and extract key details accurately. 
Focus on all fields defined in the JobPosting model, paying special attention to remote work details, 
industry, startup funding stage, ML/AI involvement, datacenter operations, and experience requirements. 
Provide concise and accurate information for each field."""


def process_job_posting(client: Any, comment: str) -> Dict[str, Any]:
    try:
        resp = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": f"""{SYSTEM_PROMPT}

Extract job posting information from the following text:

{comment}

Provide the extracted information as a JSON object matching the JobPosting model structure.""",
                }
            ],
            response_model=JobPosting,
        )
        return resp.model_dump()
    except Exception as e:
        logging.error(f"Error processing comment: {e}")
        return {"error": str(e)}


def load_cache(output_file: str) -> Dict[str, Any]:
    try:
        with open(output_file, "r") as f:
            data = json.load(f)
        return {
            comment["original"]["id"]: comment["classified"]
            for comment in data.get("classified_comments", [])
        }
    except FileNotFoundError:
        return {}


def process_comment(client, comment, cache):
    comment_id = comment["id"]
    if comment_id in cache and "error" not in cache[comment_id]:
        return comment, cache[comment_id], True
    else:
        original_text = comment.get("text", "")
        classified_data = process_job_posting(client, original_text)
        return comment, classified_data, False


def save_results(
    temp_file: str, post_data: Dict[str, Any], classified_comments: List[Dict[str, Any]]
):
    output_data = {"post": post_data, "classified_comments": classified_comments}
    with open(temp_file, "w") as f:
        json.dump(output_data, f, indent=2)
    logging.info(f"Intermediate results saved to {temp_file}")


def classify_jobs(
    client: instructor.Instructor,
    input_file: str,
    output_file: str,
    limit: int = 0,
    max_workers: int = 10,
):
    with open(input_file, "r") as f:
        data = json.load(f)

    comments = data["comments"]
    if limit > 0:
        comments = comments[:limit]

    temp_file = f"{output_file}.temp"
    cache = load_cache(output_file)
    classified_comments = []
    successful_classifications = 0
    errors = 0
    cached_results = 0
    processed_count = 0
    save_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_comment, client, comment, cache)
            for comment in comments
        ]

        pbar = tqdm(total=len(comments), desc="Classifying job postings")
        for future in as_completed(futures):
            original, classified_data, is_cached = future.result()

            if is_cached:
                cached_results += 1
            elif "error" in classified_data:
                errors += 1
            else:
                successful_classifications += 1

            classified_comments.append(
                {"original": original, "classified": classified_data}
            )

            processed_count += 1
            if processed_count % 10 == 0:
                with save_lock:
                    save_results(temp_file, data["post"], classified_comments)

            pbar.update(1)
            pbar.set_postfix(
                {
                    "Successful": successful_classifications,
                    "Errors": errors,
                    "Cached": cached_results,
                },
                refresh=True,
            )

        pbar.close()

    data["post"].pop("kids", None)  # clean unnecessary data
    save_results(temp_file, data["post"], classified_comments)

    # Rename temp file to final output file
    os.replace(temp_file, output_file)

    logging.info(
        f"Classification complete. Total: {len(classified_comments)}, "
        f"Successful: {successful_classifications}, Errors: {errors}, "
        f"Cached: {cached_results}. Final output written to {output_file}"
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
        "--workers",
        type=int,
        default=10,
        help="Number of worker threads to use",
    )
    args = parser.parse_args()
    logging.info(f"args: {args}")

    try:
        anthropic_client = Anthropic()
        client = instructor.from_anthropic(anthropic_client)
        classify_jobs(client, args.input, args.output, args.limit, args.workers)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
