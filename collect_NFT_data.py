import requests
import pandas as pd
import datetime as dt
import re
from selenium import webdriver
import time

########################################################
# twitter API
########################################################
def bearer_oauth(r):
    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAD52QAEAAAAAqzdbzPMFJgIXD2e7Yz%2Fcg7euVAA%3DcL21U5k0N97ESqPunGIBAx4e0BbdV3wqSo4spUw86r1VGPpvL3'
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r

def get_twitter(users):
    usernames = "usernames={}".format(users)
    print(users)
    user_fields = "user.fields=description,created_at,public_metrics"
    url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
    response = requests.request("GET", url, auth=bearer_oauth, )
    print(response.json())
    if response.status_code != 200:
        return [users, 0, 0, 0, 0]
    else:
        data = response.json()['data']
        followers = data[0]['public_metrics']['followers_count']
        following = data[0]['public_metrics']['following_count']
        tweets = data[0]['public_metrics']['tweet_count']
        description = data[0]['description']
        return [users, followers, following, tweets, description]

########################################################
# icy.tools data scrape
########################################################
def get_top_mints():
    PATH = 'C:\Program Files (x86)\chromedriver.exe'
    driver = webdriver.Chrome(PATH)
    driver.get('https://icy.tools/discover')
    text = driver.page_source
    add_str = r"address\":\"([\w\d]+)\""
    name_str = r"name\":\"([^\"]+)\",\"slug"
    sum_str = r"sum\":(\d+),\""
    mint_str = r"count\":(\d+),\"distinct"
    dist_str = r"distinct\":(\d+),\"index"
    slug_str = r"slug\":([^\,]+),\""
    addresses = re.findall(add_str, text)
    names = re.findall(name_str, text)
    sums = re.findall(sum_str, text)
    mints = re.findall(mint_str, text)
    distinct = re.findall(dist_str, text)
    slug = re.findall(slug_str, text)
    driver.close()
    return [addresses, names, mints, distinct, sums, slug]

########################################################
#get top ranking tokens on opensea
########################################################
def get_top_nft():
    PATH = 'C:\Program Files (x86)\chromedriver.exe'
    driver = webdriver.Chrome(PATH)
    driver.get('https://opensea.io/rankings')
    text = driver.page_source
    name_str = r"name\":\"([^\"]+)\",\"slug"
    slug_str = r"slug\":([^\,]+),\""
    names = re.findall(name_str, text)
    slug = re.findall(slug_str, text)
    top = pd.DataFrame([names, slug]).T
    top.columns = ['token', 'slug']
    driver.close()
    return top

########################################################
# opensea api
########################################################
def get_opensea(slug):
    # create empty array to be used on bad data calls
    time.sleep(2)
    fill = {'one_day_volume': 0,
            'one_day_change': 0,
            'one_day_sales': 0,
            'one_day_average_price': 0,
            'seven_day_volume': 0,
            'seven_day_change': 0,
            'seven_day_sales': 0,
            'seven_day_average_price': 0,
            'thirty_day_volume': 0,
            'thirty_day_change': 0,
            'thirty_day_sales': 0,
            'thirty_day_average_price': 0,
            'total_volume': 0,
            'total_sales': 0,
            'total_supply': 0,
            'count': 0,
            'num_owners': 0,
            'average_price': 0,
            'num_reports': 0,
            'market_cap': 0,
            'floor_price': 0,
            'slug': 0,
            'twitter_username': 0}
    url = "https://api.opensea.io/collection/{}".format(slug)
    response = requests.request("GET", url)
    if response.status_code == 404:
        return fill
    else:
        data = response.json()
        stats = data['collection']['stats']
        stats['slug'] = slug
        stats['twitter_username'] = data['collection']['twitter_username']
        return stats

########################################################
#gather all newly minted data
########################################################
def get_mint_data():
    now = dt.datetime.now().strftime('%Y%m%d%H')

    # get new mints
    erc751 = pd.DataFrame(get_top_mints()).T.rename(
        columns={0: 'address', 1: 'token', 2: 'mints_hr', 3: 'distinct', 4: 'total_mint', 5: 'slug'})
    erc751[['mints_hr', 'distinct', 'total_mint']] = erc751[['mints_hr', 'distinct', 'total_mint']].astype(float)

    # get and merge opensea data
    erc751['slug'] = erc751['slug'].str.replace('"', '')
    erc751 = erc751[erc751['slug'] != 'null']
    opensea = [get_opensea(x) for x in erc751['slug']]
    opensea = pd.DataFrame(opensea)
    erc751 = pd.merge(left=erc751, right=opensea, on='slug', how='left')

    # get and merge twitter data
    erc751 = erc751[erc751['twitter_username'].notnull()]
    twitter = [get_twitter(x) for x in erc751['twitter_username']]
    twitter = pd.DataFrame(twitter, columns=['twitter_username', 'followers', 'following', 'tweets', 'description'])
    erc751 = pd.merge(left=erc751, right=twitter, on='twitter_username', how='left')

    # add metrics
    erc751['7day_vol_mom'] = erc751['one_day_volume'] / erc751['seven_day_volume']
    erc751['7day_price_mom'] = erc751['one_day_average_price'] / erc751['seven_day_average_price']
    erc751['pct_unique_owners'] = (erc751['num_owners'] / erc751['total_mint'].astype(float)) * 100
    erc751['cap_to_followers'] = erc751['market_cap'] / erc751['followers'].astype(float)

    # drop columns for clean
    drop = ['address', 'distinct', 'following', 'one_day_volume', 'one_day_sales', 'one_day_average_price',
            'seven_day_volume', 'seven_day_sales', 'seven_day_change', 'seven_day_average_price', 'thirty_day_volume',
            'thirty_day_change',
            'thirty_day_sales', 'thirty_day_average_price', 'average_price', 'count', 'num_reports']
    nft_clean = erc751.drop(columns=drop)
    nft_clean['time'] = now

    # filters
    nft_clean = nft_clean[nft_clean['followers'] > 1000]
    nft_clean = nft_clean[nft_clean['tweets'] > 10]
    nft_clean = nft_clean[nft_clean['total_supply'] > 1000]
    nft_clean = nft_clean[nft_clean['market_cap'] > 0]
    nft_clean = nft_clean[nft_clean['num_owners'] > 1000]
    return nft_clean, erc751

########################################################
#gather top ranking data
########################################################
def gather_top_rank():
    now = dt.datetime.now().strftime('%Y%m%d')

    #scrape top ranks from openseas ranking page
    top = get_top_nft()
    top['slug'] = top['slug'].str.replace('\"', '')

    #add in new minted coins
    mints = pd.read_csv('top_mints')
    mints = mints[mints['token'].duplicated()][['token', 'slug']]
    new = mints[~mints['slug'].isin(top['slug'])]
    all = pd.concat([top, new])

    #gather stats from opensea
    opensea = [get_opensea(x) for x in all['slug']]
    opensea = pd.DataFrame(opensea)
    all_data = pd.merge(left=all, right=opensea, on='slug', how='left')

    twitter = [get_twitter(x) for x in all_data['twitter_username']]
    twitter = pd.DataFrame(twitter, columns=['twitter_username', 'followers', 'following', 'tweets', 'description'])
    all_data = pd.merge(left=all_data, right=twitter, on='twitter_username', how='left')
    all_data['time'] = now
    return all_data

#initiate function
now = dt.datetime.now().strftime('%Y%m%d')
nft_clean, raw1 = get_mint_data()
nft_clean.to_csv('top_mints')
raw1.to_csv('raw_mints_{}'.format(now))
top_clean = gather_top_rank()
top_clean.to_csv('top_rank_{}'.format(now))
time.sleep(60*60)

#run in loop
while True:
    #get mint data
    nft, raw = get_mint_data()
    print(nft)
    nft_clean = pd.concat([nft_clean, nft])
    nft_clean.to_csv('top_mints')
    raw.to_csv('raw_mints_{}'.format(now))

    #get top rank data once a day
    if dt.datetime.now().strftime('%Y%m%d') != now:
        top_new = gather_top_rank()
        top_clean = pd.concat([top_clean, top_new])
        top_clean.to_csv('top_rank')
        top_clean.to_csb('top_archive_{}'.format(now))
        print(top_clean.head())
        now = dt.datetime.now().strftime('%Y%m%d')
    time.sleep(60*60)



