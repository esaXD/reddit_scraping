# Reddit Research Agent - Prompt-Driven, LLM-Planned, GitHub Pages

Type any prompt. The agent plans subreddits and strategy (LLM if available), scrapes/analyzes Reddit, and deploys a tailored report to GitHub Pages.

## Use
- Settings -> Pages -> Source = **GitHub Actions**
- Add secrets as needed: OPENAI_API_KEY / ANTHROPIC_API_KEY, REDDIT_CLIENT_ID/SECRET/USER_AGENT
- Actions -> **research** -> Run workflow -> Enter **user_prompt**
- Workflow seeds planning with `gpt-5`, which now only proposes a vetted subreddit list (no auto keywords/filters). The scrape runs with exactly the months/min_upvotes/limit you provide, and if the seed step fails the job stops so you can adjust the prompt.
- Workflow inputs let you set `max_subs`, `months`, `min_upvotes`, and `limit` before each run.
- *Test Reddit API health* validates the PullPush mirror; inspect `data/api_diagnostics.json` on failures.
- Scrape respects the workflow inputs for months/min_upvotes/limit; no automatic widening is performed (keywords are optional and only used if you provide them).
- The workflow prints both the LLM executive summary and a raw JSONL sample at the end so you can inspect results immediately.
