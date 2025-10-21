# Reddit Research Agent — Prompt-Driven, LLM-Planned, GitHub Pages

Type any prompt. The agent plans subreddits & strategy (LLM if available), scrapes/analyzes Reddit, and deploys a tailored report to GitHub Pages.

## Use
- Settings → Pages → Source = **GitHub Actions**
- Add secrets as needed: OPENAI_API_KEY / ANTHROPIC_API_KEY, REDDIT_CLIENT_ID/SECRET/USER_AGENT
- Actions → **research** → Run workflow → Enter **user_prompt**
- Workflow otomatik olarak *Test Reddit API health* adımında PullPush bağlantısını yoklar; hata durumunda `data/api_diagnostics.json` içinde detayları bulabilirsiniz.
