import unittest
from unittest.mock import Mock
import json
import tempfile
from extractor import process_job_posting, load_cache, classify_jobs
from models import JobPosting
import instructor


class TestExtractor(unittest.TestCase):

    def setUp(self):
        self.sample_comment = """Hatchet (https://hatchet.run) | New York City (IN-PERSON) | Full-time

We're hiring a founding engineer to help us with development on our open-source, distributed task queue: https://github.com/hatchet-dev/hatchet.

As a founding engineer, you'll be responsible for contributing across the entire codebase. We'll compensate accordingly and with high equity. It's currently just the two founders + a part-time contractor. We're all technical and contribute code.

Stack: Typescript/React, Go and PostgreSQL.

To apply, email alexander [at] hatchet [dot] run"""

        self.sample_job_posting = JobPosting(
            company_name="Hatchet",
            positions=["Founding Engineer"],
            location="New York City",
            job_type="Full Time",
            salary_range=None,
            benefits=[],
            required_skills=["Typescript", "React", "Go", "PostgreSQL"],
            job_description="We're hiring a founding engineer to help us with development on our open-source, distributed task queue.",
            additional_requirements=[],
            work_environment="In-person",
            application_instructions="To apply, email alexander [at] hatchet [dot] run",
            is_remote=False,
            is_remote_in_us=False,
            is_remote_global=False,
            timezone=None,
            industry="Software Development",
            startup_series="Unknown",
            is_ml=False,
            is_datacenter=False,
            year_of_experience=None,
        )

        self.valid_cache_content = {
            "classified_comments": [
                {
                    "original": {"id": 40847106},
                    "classified": self.sample_job_posting.model_dump(),
                }
            ]
        }

    def test_process_job_posting(self):
        mock_client = Mock(spec=instructor.Instructor)
        mock_client.messages.create.return_value = self.sample_job_posting

        result = process_job_posting(mock_client, self.sample_comment)
        self.assertEqual(result["company_name"], "Hatchet")
        self.assertEqual(result["positions"], ["Founding Engineer"])
        self.assertEqual(result["location"], "New York City")
        self.assertEqual(result["job_type"], "Full Time")
        self.assertFalse(result["is_remote"])
        self.assertEqual(
            result["required_skills"], ["Typescript", "React", "Go", "PostgreSQL"]
        )

    def test_load_cache(self):
        with tempfile.NamedTemporaryFile(mode="w+") as temp_file:
            json.dump(self.valid_cache_content, temp_file)
            temp_file.flush()

            cache = load_cache(temp_file.name)
            self.assertIn(40847106, cache)
            self.assertEqual(cache[40847106]["company_name"], "Hatchet")

    def test_classify_jobs(self):
        mock_client = Mock(spec=instructor.Instructor)
        mock_client.messages.create.return_value = self.sample_job_posting

        with tempfile.NamedTemporaryFile(
            mode="w+"
        ) as input_file, tempfile.NamedTemporaryFile(mode="w+") as output_file:

            json.dump(
                {
                    "comments": [
                        {"id": 40847106, "text": self.sample_comment},
                        {"id": 40847107, "text": "Another job posting"},
                    ],
                    "post": {"title": "Test Post"},
                },
                input_file,
            )
            input_file.flush()

            # Write a valid initial cache to the output file
            json.dump(self.valid_cache_content, output_file)
            output_file.flush()

            classify_jobs(mock_client, input_file.name, output_file.name, limit=2)

            output_file.seek(0)
            result = json.load(output_file)

            self.assertEqual(len(result["classified_comments"]), 2)
            self.assertEqual(
                result["classified_comments"][0]["classified"]["company_name"],
                "Hatchet",
            )
            self.assertEqual(
                result["classified_comments"][0]["classified"]["positions"],
                ["Founding Engineer"],
            )


if __name__ == "__main__":
    unittest.main()
