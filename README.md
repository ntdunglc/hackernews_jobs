## Readme

This is an experiment to use Claude. Most of the code was written by Claude chat, including
- `crawl.py` to download top-level comments of a hackernews "Who is hiring" post.
- `extractor.py` analyzing each job posting using Anthropic API.
- `adhoc.ipynb` analyze the output to find insights, mostly looking for remote job.

[Example output](https://docs.google.com/spreadsheets/d/1vNG0xco7rW_5hIARyCKwxR6KcmcGtjnEliuHhEqAC7E/edit?gid=419810057#gid=419810057)

```
python crawl.py --url https://news.ycombinator.com/item\?id\=41129813 --output_path 202408_raw.json
python extractor.py --input 202408_raw.json --output 202408_classified.json --workers 5
python write_jobs_to_csv.py --input 202408_classified.json --output 202408_classified.csv
```


