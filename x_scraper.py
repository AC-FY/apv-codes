import os
import sys
import tweepy
import json
import csv
from datetime import datetime, timezone
from typing import List, Dict, Optional, Union
import argparse
from loguru import logger as log


def start_time(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None

    raw = value.strip()
    if not raw:
        return None

    candidates = (raw, raw.replace("Z", "+00:00") if raw.endswith("Z") else raw)
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass

    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

    raise argparse.ArgumentTypeError(
        "Invalid --start-time. Use ISO 8601 (e.g. 2026-03-26T10:30:00Z) or "
        "YYYY-MM-DD[ HH:MM[:SS]]"
    )


class TwitterScraper:
    def __init__(self, bearer_token: Optional[str] = None):
        self.bearer_token = self._normalize_bearer_token(
            bearer_token or os.getenv('TWITTER_BEARER_TOKEN')
        )
        
        if not self.bearer_token:
            raise ValueError(
                "Twitter Bearer Token is required. "
                "Set TWITTER_BEARER_TOKEN environment variable or pass it as parameter."
            )
        
        self.client = tweepy.Client(bearer_token=self.bearer_token)
        self.tweets_data = []
        self.seen_tweet_ids = set()
        
        log.info("Scraper initialized")

    def _normalize_bearer_token(self, token: Optional[str]) -> Optional[str]:
        if token is None:
            return None
        cleaned = str(token).strip()
        if cleaned.startswith("[") and cleaned.endswith("]"):
            cleaned = cleaned[1:-1].strip()
        cleaned = cleaned.strip("'\"")
        return cleaned.strip()

    def swap_token(self, bearer_token: str):
        self.bearer_token = self._normalize_bearer_token(bearer_token)
        self.client = tweepy.Client(bearer_token=self.bearer_token)
        log.info("Bearer token rotated.")

    def get_oldest_time(self) -> Optional[str]:
        if not self.tweets_data:
            return None
        oldest = min(self.tweets_data, key=lambda t: t["date"])
        dt = oldest["date"]
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        dt = dt.replace(microsecond=0)
        dt = datetime.fromtimestamp(dt.timestamp() - 1, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")

    def scrape_by_username(
        self,
        username: str,
        max_tweets: int = 100,
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
    ) -> List[Dict]:
        start_dt = globals()["start_time"](start_time) if isinstance(start_time, str) else start_time
        if isinstance(end_time, str):
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        else:
            end_dt = end_time

        log.info(
            f"Scraping from @{username} | start_time={start_time} | "
            f"end_time={end_time} | target={max_tweets} tweets"
        )

        page_size = max(5, min(int(max_tweets), 100))
        remaining = max_tweets
        pagination_token = None

        try:
            user_resp = self.client.get_user(username=username.lstrip("@"))
            if not user_resp or not user_resp.data:
                log.warning(f"User @{username} not found")
                return self.tweets_data

            user_id = user_resp.data.id

            while remaining > 0:
                request_kwargs = {
                    "id": user_id,
                    "max_results": min(page_size, remaining),
                    "tweet_fields": ["created_at", "public_metrics", "text"],
                }
                if start_dt is not None:
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=timezone.utc)
                    request_kwargs["start_time"] = start_dt.astimezone(timezone.utc)
                if end_dt is not None:
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=timezone.utc)
                    request_kwargs["end_time"] = end_dt.astimezone(timezone.utc)
                if pagination_token:
                    request_kwargs["pagination_token"] = pagination_token

                tweets_resp = self.client.get_users_tweets(**request_kwargs)
                if not tweets_resp or not tweets_resp.data:
                    break

                added = 0
                for tweet in tweets_resp.data:
                    if tweet.id in self.seen_tweet_ids:
                        continue
                    self.tweets_data.append(
                        {
                            "date": tweet.created_at,
                            "username": username.lstrip("@"),
                            "content": tweet.text,
                            "url": f"https://twitter.com/{username.lstrip('@')}/status/{tweet.id}",
                        }
                    )
                    self.seen_tweet_ids.add(tweet.id)
                    added += 1

                remaining -= added
                meta = getattr(tweets_resp, "meta", None) or {}
                pagination_token = meta.get("next_token")
                if not pagination_token:
                    break
        except tweepy.Unauthorized:
            log.error(f"401 Unauthorized.")
            return self.tweets_data
        except tweepy.TooManyRequests:
            log.error(f"Rate-limited for current token.")
            return self.tweets_data
        except Exception as e:
            log.error(f"Unexpected scrape error: {e}")
            return self.tweets_data

        log.info(f"Fetched {len(self.tweets_data)} total tweets (duplicates skipped)")
        return self.tweets_data

    def save_to_csv(self, filename: str = "tweets.csv"):
        if not self.tweets_data:
            log.warning("No data to save.")
            return
        rows = sorted(self.tweets_data, key=lambda x: x["date"])
        with open(filename, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["date", "username", "content", "url"]
            )
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"Saved {len(rows)} tweets to {filename}")
    
    def scrape_user_tweets(
        self,
        user_identifier: Union[str, int],
        max_tweets: int = 2,
        start_time_dt: Optional[datetime] = None,
    ) -> List[Dict]:
        log.info(f"Scraping {max_tweets} recent tweets from user: {user_identifier}")
        
        try:
            resolved_user_id = user_identifier
            if isinstance(user_identifier, str) and not user_identifier.isdigit():
                username = user_identifier.lstrip("@")
                user_resp = self.client.get_user(
                    username=username,
                    user_fields=["username", "name", "verified", "public_metrics"],
                )
                if not user_resp or not user_resp.data:
                    log.warning(f"User not found: {user_identifier}")
                    return []
                resolved_user_id = user_resp.data.id

            request_size = max(5, min(int(max_tweets), 100))

            request_kwargs = {
                "id": resolved_user_id,
                "max_results": request_size,
                "tweet_fields": [
                    "created_at",
                    "public_metrics",
                    "context_annotations",
                    "entities",
                    "lang",
                    "source",
                ],
                "expansions": ["author_id"],
            }

            if start_time_dt is not None:
                if start_time_dt.tzinfo is None:
                    start_time_dt = start_time_dt.replace(tzinfo=timezone.utc)
                request_kwargs["start_time"] = start_time_dt.astimezone(timezone.utc)

            tweets_response = self.client.get_users_tweets(**request_kwargs)
            
            if not tweets_response.data:
                log.warning(f"No tweets found for user: {user_identifier}")
                return []
            
            user_info = {}
            if tweets_response.includes and 'users' in tweets_response.includes:
                user = tweets_response.includes['users'][0]
                user_info = {
                    'user_id': user.id,
                    'username': user.username,
                    'name': user.name,
                    'verified': getattr(user, 'verified', False),
                    'followers_count': user.public_metrics.get('followers_count', 0) if user.public_metrics else 0,
                    'following_count': user.public_metrics.get('following_count', 0) if user.public_metrics else 0,
                    'tweet_count': user.public_metrics.get('tweet_count', 0) if user.public_metrics else 0
                }
            
            tweets_data = []
            for tweet in tweets_response.data:
                if start_time_dt is not None and tweet.created_at is not None:
                    tweet_created = tweet.created_at
                    if tweet_created.tzinfo is None:
                        tweet_created = tweet_created.replace(tzinfo=timezone.utc)
                    if tweet_created < start_time_dt.astimezone(timezone.utc):
                        continue

                tweet_data = {
                    'text': tweet.text,
                    'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
                    'author': user_info,
                    'metrics': {
                        'like_count': tweet.public_metrics.get('like_count', 0) if tweet.public_metrics else 0,
                        'retweet_count': tweet.public_metrics.get('retweet_count', 0) if tweet.public_metrics else 0,
                        'reply_count': tweet.public_metrics.get('reply_count', 0) if tweet.public_metrics else 0,
                        'quote_count': tweet.public_metrics.get('quote_count', 0) if tweet.public_metrics else 0,
                        'bookmark_count': tweet.public_metrics.get('bookmark_count', 0) if tweet.public_metrics else 0,
                        'impression_count': tweet.public_metrics.get('impression_count', 0) if tweet.public_metrics else 0
                    },
                    'lang': tweet.lang,
                    'source': tweet.source,
                    'entities': {
                        'hashtags': [tag['tag'] for tag in tweet.entities.get('hashtags', [])],
                        'mentions': [mention['username'] for mention in tweet.entities.get('mentions', [])],
                        'urls': [url['expanded_url'] for url in tweet.entities.get('urls', [])]
                    } if tweet.entities else {}
                }
                tweets_data.append(tweet_data)
            
            log.info(f"Successfully scraped {len(tweets_data)} tweets from @{user_info.get('username', user_identifier)}")
            return tweets_data
            
        except tweepy.TooManyRequests:
            log.error("Rate limit exceeded.")
            raise
        except tweepy.Unauthorized:
            log.error("401 Unauthorized.")
            raise
        except tweepy.NotFound:
            log.error(f"User not found: {user_identifier}")
            raise
        except Exception as e:
            log.error(f"Error scraping tweets for user {user_identifier}: {e}")
            raise
    
    def scrape_multiple_users(
        self,
        user_identifiers: List[Union[str, int]],
        max_tweets_per_user: int = 2,
        start_time_dt: Optional[datetime] = None,
    ) -> Dict[str, List[Dict]]:
        log.info(f"Scraping {max_tweets_per_user} tweets from {len(user_identifiers)} users")
        
        results = {}
        
        for user_id in user_identifiers:
            try:
                tweets = self.scrape_user_tweets(
                    user_id,
                    max_tweets=max_tweets_per_user,
                    start_time_dt=start_time_dt,
                )
                results[str(user_id)] = tweets
                log.info(f"Completed scraping for user: {user_id}")
                
            except Exception as e:
                log.error(f"Failed to scrape user {user_id}: {e}")
                results[str(user_id)] = []
        
        return results
    
    def export_to_json(self, data: Union[List[Dict], Dict], filename: str = None) -> str:
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"twitter_scrape_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        log.info(f"Data exported to: {filename}")
        return filename
    
    def print_summary(self, data: Union[List[Dict], Dict]):
        if isinstance(data, list):
            # Single user data
            if not data:
                print("No tweets found.")
                return
            
            user_info = data[0].get('author', {})
            print(f"\Summary for @{user_info.get('username', 'unknown')}")
            print("=" * 50)
            print(f"Name: {user_info.get('name', 'Unknown')}")
            print(f"Followers: {user_info.get('followers_count', 0):,}")
            print(f"Following: {user_info.get('following_count', 0):,}")
            print(f"Total Tweets: {user_info.get('tweet_count', 0):,}")
            print(f"Verified: {'Y' if user_info.get('verified') else 'N'}")
            print(f"Recent Tweets Scraped: {len(data)}")
            
            print(f"\nRecent Tweets:")
            print("-" * 30)
            for i, tweet in enumerate(data, 1):
                print(f"{i}. {tweet['text'][:100]}{'...' if len(tweet['text']) > 100 else ''}")
                print(f"{tweet['metrics']['like_count']} likes | {tweet['metrics']['retweet_count']} retweets | {tweet['metrics']['reply_count']} replies")
                print(f"{tweet['created_at']}")
                print()
        
        elif isinstance(data, dict):
            print(f"\nSummary for {len(data)} users")
            print("=" * 50)
            
            total_tweets = 0
            for user_id, tweets in data.items():
                total_tweets += len(tweets)
                if tweets:
                    user_info = tweets[0].get('author', {})
                    print(f"@{user_info.get('username', user_id)}: {len(tweets)} tweets")
                else:
                    print(f"{user_id}: No tweets found")
            
            print(f"\nTotal tweets scraped: {total_tweets}")


def main():
    parser = argparse.ArgumentParser(description="Scrape recent tweets from specific Twitter users")
    parser.add_argument("users", nargs="+", help="Twitter usernames (without @) or user IDs to scrape")
    parser.add_argument("--max-tweets", "-m", type=int, default=2, help="Maximum tweets to scrape per user (default: 2)")
    parser.add_argument(
        "--start-time",
        type=start_time,
        default=None,
        help="Only include tweets created at/after this time (ISO 8601 or YYYY-MM-DD[ HH:MM[:SS]]; interpreted as UTC if no TZ).",
    )
    parser.add_argument("--output", "-o", help="Output JSON file path (optional)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    if args.verbose:
        log.remove()
        log.add(sys.stderr, level="DEBUG")
    else:
        log.remove()
        log.add(sys.stderr, level="INFO")
    try:
        scraper = TwitterUserScraper()

        if len(args.users) == 1:
            tweets = scraper.scrape_user_tweets(
                args.users[0],
                max_tweets=args.max_tweets,
                start_time_dt=args.start_time,
            )
            scraper.print_summary(tweets)
            
            if args.output:
                scraper.export_to_json(tweets, args.output)
        else:
            results = scraper.scrape_multiple_users(
                args.users,
                max_tweets_per_user=args.max_tweets,
                start_time_dt=args.start_time,
            )
            scraper.print_summary(results)
            
            if args.output:
                scraper.export_to_json(results, args.output)
        
        print("\nComplete")
        
    except ValueError as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
