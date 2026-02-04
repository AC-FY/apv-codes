from x_scraper import TwitterScraper

BEARER_TOKEN = " [...] "  # remember to use your own bearer token
    # bearer token 1 
    # bearer token 2
        ## use this for safekeeping

scraper = TwitterScraper(BEARER_TOKEN)
scraper.scrape_by_username("[...]", max_tweets=100, start_time="2026-01-20T00:00:00Z") # change target twitter id & start_time here
scraper.save_to_csv("[...].csv")    # change .csv file name here