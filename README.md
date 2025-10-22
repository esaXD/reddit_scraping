# Reddit Research Agent - Prompt-Driven, LLM-Planned, GitHub Pages

Type any prompt. The agent plans subreddits and strategy (LLM if available), scrapes/analyzes Reddit, and deploys a tailored report to GitHub Pages.

## Use
- Settings -> Pages -> Source = **GitHub Actions**
- Add secrets as needed: OPENAI_API_KEY / ANTHROPIC_API_KEY, REDDIT_CLIENT_ID/SECRET/USER_AGENT
- Actions -> **research** -> Run workflow -> Enter **user_prompt**
- Workflow seeds planning with `gpt-4o-mini`, which returns focused subreddits/keywords/filters (plus validation hints). If the LLM call fails the pipeline aborts early so you can fix the issue; automated discovery is no longer used as a silent fallback.
- *Test Reddit API health* validates the PullPush mirror; inspect `data/api_diagnostics.json` on failures.
- Prompts automatically expand into English keyword sets; subreddit/keyword searches widen their scope (longer time windows, lower upvote thresholds, curated fallbacks) until usable data is found.
- The workflow prints both the LLM executive summary and a raw JSONL sample at the end so you can inspect results immediately.
