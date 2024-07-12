"""
Script to download hackernews comments and store to JSON.
Example:
    python crawl.py --url=https://news.ycombinator.com/item\?id\=40846428 --output_path=hn_comments.json
"""

import requests
import time
import argparse
import json
import logging
from urllib.parse import urlparse, parse_qs
from tqdm import tqdm

BASE_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_item(item_id):
    response = requests.get(BASE_URL.format(item_id))
    return response.json()


def get_top_level_comments(story_id):
    story = get_item(story_id)
    if not story or "kids" not in story:
        return [], story

    logging.info(f"Story title: {story.get('title', 'N/A')}")
    logging.info(f"Story text (first 100 chars): {story.get('text', 'N/A')[:100]}")

    total_comments = len(story["kids"])
    logging.info(f"Total number of top-level comments: {total_comments}")

    top_level_comments = []
    for comment_id in tqdm(story["kids"], desc="Fetching comments", unit="comment"):
        comment = get_item(comment_id)
        if comment and comment["type"] == "comment":
            top_level_comments.append(comment)
        time.sleep(0.1)  # Be nice to the API

    return top_level_comments, story


def extract_item_id(url):
    parsed_url = urlparse(url)
    if "news.ycombinator.com" not in parsed_url.netloc:
        raise ValueError("URL must be from news.ycombinator.com")

    query_params = parse_qs(parsed_url.query)
    if "id" in query_params:
        return query_params["id"][0]

    path_parts = parsed_url.path.split("/")
    if len(path_parts) > 2 and path_parts[-2] == "item":
        return path_parts[-1]

    raise ValueError("Could not extract item ID from URL")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch top-level comments from a Hacker News post."
    )
    parser.add_argument("--url", required=True, help="URL of the Hacker News post")
    parser.add_argument("--output_path", help="Path to save the output JSON file")
    args = parser.parse_args()

    try:
        story_id = extract_item_id(args.url)
        logging.info(f"Extracting comments for story ID: {story_id}")

        comments, post = get_top_level_comments(story_id)

        output = {"post": post, "comments": comments}

        if args.output_path:
            with open(args.output_path, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=2)
            logging.info(f"Output saved to {args.output_path}")
        else:
            print(json.dumps(output, ensure_ascii=False, indent=2))

        logging.info(f"Finished processing. Found {len(comments)} top-level comments.")

    except ValueError as e:
        logging.error(f"Error: {str(e)}")
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {str(e)}")
    except IOError as e:
        logging.error(f"Error writing to file: {str(e)}")


if __name__ == "__main__":
    main()
