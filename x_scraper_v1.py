import tweepy
import pandas as pd
import json
from datetime import datetime, timezone

class TwitterScraper:
    
    def __init__(self, bearer_token):
        self.client = tweepy.Client(bearer_token=bearer_token)
        self.tweets_data = []
        self.seen_tweet_ids = set()
    
    def scrape_by_username(self, username, max_tweets=100, start_time=None, end_time=None):
        print(f"Scraping from @{username}")
        
        try:
            user = self.client.get_user(username=username)
            if not user.data:
                print(f"User @{username} not found")
                return []
            
            user_id = user.data.id
            
            kwargs = {
                'id': user_id,
                'max_results': min(max_tweets, 100),
                'tweet_fields': ['created_at', 'public_metrics', 'text']
            }
            
            if start_time:
                if isinstance(start_time, str):
                    try:
                        if start_time.endswith('Z'):
                            start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        else:
                            start_time = datetime.fromisoformat(start_time)
                            if start_time.tzinfo is None:
                                start_time = start_time.replace(tzinfo=timezone.utc)
                    except ValueError as e:
                        print(f"Error parsing start_time: {e}")
                        print("Expected format: YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD HH:MM:SS")
                        return []
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                kwargs['start_time'] = start_time
                print(f"Filtering tweets from: {start_time.isoformat()}")
            
            if end_time:
                if isinstance(end_time, str):
                    try:
                        if end_time.endswith('Z'):
                            end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                        else:
                            end_time = datetime.fromisoformat(end_time)
                            if end_time.tzinfo is None:
                                end_time = end_time.replace(tzinfo=timezone.utc)
                    except ValueError as e:
                        print(f"Error parsing end_time: {e}")
                        return []
                
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                
                kwargs['end_time'] = end_time
                print(f"Filtering tweets until: {end_time.isoformat()}")
            
            tweets = self.client.get_users_tweets(**kwargs)
            
            if tweets.data:
                for tweet in tweets.data:
                    if tweet.id not in self.seen_tweet_ids:
                        self.tweets_data.append({
                            'date': tweet.created_at,
                            'username': username,
                            'tweet_id': tweet.id,
                            'content': tweet.text,
                            'url': f"https://twitter.com/{username}/status/{tweet.id}"
                        })
                        self.seen_tweet_ids.add(tweet.id)
                
                print(f"Scraped {len(tweets.data)} new tweets from this request")
            else:
                print("No tweets found in the specified time range")
            
            print(f"Total tweets collected: {len(self.tweets_data)}")
            return self.tweets_data
            
        except tweepy.errors.BadRequest as e:
            print(f"Bad Request Error: {e}")
            
            print("  - Date format must be ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)")
            return []
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def save_to_csv(self, filename='tweets.csv'):
        if not self.tweets_data:
            print("No data to save")
            return
        
        df = pd.DataFrame(self.tweets_data)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    
    def save_to_json(self, filename='tweets.json'):
        if not self.tweets_data:
            print("No data to save")
            return
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.tweets_data, f, ensure_ascii=False, indent=4, default=str)
        print(f"Data saved to {filename}")
    
    def get_dataframe(self):
        if not self.tweets_data:
            print("No data available")
            return None
        
        return pd.DataFrame(self.tweets_data)
    
    def clear_data(self):
        self.tweets_data = []
        self.seen_tweet_ids = set()
        print("Data cleared")