from x_scraper import TwitterScraper
import os
 
# fallback bearer tokens (prefer env vars below)
FALLBACK_BEARER_TOKENS = [
    ""
]

def _clean_token(token: str) -> str:
    cleaned = str(token).strip().strip("'\"")
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1].strip()
    return cleaned.strip()

env_tokens = []
i = 1
while True:
    value = os.getenv(f"TWITTER_BEARER_TOKEN_{i}")
    if value is None:
        break
    env_tokens.append(value)
    i += 1

raw_tokens = env_tokens if env_tokens else FALLBACK_BEARER_TOKENS
BEARER_TOKENS = [_clean_token(t) for t in raw_tokens if _clean_token(t)]

if not BEARER_TOKENS:
    raise RuntimeError(
        "No bearer tokens found."
    )

TARGET_USERNAME = "DHSgov"
TOTAL_TWEETS    = 500  # change number scraped here
PER_TOKEN_MAX   = 50  # keep same as above
START_TIME      = "2026-03-27T00:00:00Z"    # change start_time here
OUTPUT_FILE     = "DHS_tweets.csv"  # change output file name here

scraper = TwitterScraper(BEARER_TOKENS[0])
for i, token in enumerate(BEARER_TOKENS):
    current_total = len(scraper.tweets_data)
    if current_total >= TOTAL_TWEETS:
        print(f"Reached target of {TOTAL_TWEETS} tweets. Stopping early.")
        break

    remaining_total = TOTAL_TWEETS - current_total
    run_target = min(PER_TOKEN_MAX, remaining_total)

    print(f"\n=== Token {i+1}/{len(BEARER_TOKENS)} ===")
    if i > 0:
        scraper.swap_token(token)

    end_time = None if i == 0 else scraper.get_oldest_time()
    scraper.scrape_by_username(
        TARGET_USERNAME,
        max_tweets=run_target,
        start_time=START_TIME,
        end_time=end_time,
    )

print(f"\nTotal scrapes: {len(scraper.tweets_data)}")
scraper.save_to_csv(OUTPUT_FILE)
