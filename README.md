# Reddit Research Agent - Prompt-Driven, LLM-Planned, GitHub Pages

Type any prompt. The agent plans subreddits and strategy (LLM if available), scrapes/analyzes Reddit, and deploys a tailored report to GitHub Pages.

## Use
- Settings -> Pages -> Source = **GitHub Actions**
- Add secrets as needed: OPENAI_API_KEY / ANTHROPIC_API_KEY, REDDIT_CLIENT_ID/SECRET/USER_AGENT
- Actions -> **research** -> Run workflow -> Enter **user_prompt**
- Workflow first discovers candidate subreddits from the prompt (PullPush search), feeds them into the planner as hints, then runs the full research pipeline.
- *Test Reddit API health* validates the PullPush mirror; inspect `data/api_diagnostics.json` on failures.
- Prompts automatically expand into English keyword sets; subreddit/keyword searches widen their scope (longer time windows, lower upvote thresholds, curated fallbacks) until usable data is found.
