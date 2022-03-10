import requests
import json
import numpy as np
import pandas as pd
import sqlalchemy as sql
import time
from scripts.config import *
from sqlalchemy import create_engine
from utils.logger import logger
from typing import *

SEARCH = "https://api.twitter.com/2/tweets/search/all"
SEARCH_RECENT = "https://api.twitter.com/2/tweets/search/recent"


class DB:

    @property
    def location(self) -> str:
        return 'sqlite:///{path}.db'

    @classmethod
    def delete(cls, table_name: str, path: str) -> None:
        query = sql.text("DROP TABLE IF EXISTS {table_name}".format(table_name=table_name))
        engine = create_engine(DB().location.format(path=path), echo=False)
        logger.info('Deleting table {table_name}'.format(table_name=table_name))
        engine.execute(query)

    @classmethod
    def fetch(cls, table_name: str, path: str) -> pd.DataFrame:
        engine = create_engine(DB().location.format(path=path), echo=False)
        df = pd.DataFrame(engine.execute("SELECT * FROM {table_name}".format(table_name=table_name)).fetchall())
        
        if table_name=='users':
            header_dict_user = {0:"created_at",1:"name",2:"screen_name",3:"description",
            4:"id",5:"location",
                   6:"followers_count",7:"following_count",8:"listed_count",9:"tweet_count"}

            df = df.rename(header_dict_user,axis = 1)

        elif table_name=='user_tweets':
            header_dict_user_tweets = {0:"created_at",1:"screen_name",2:'text',3:'lang',
               4:'retweet_count',5:'reply_count',6:'like_count', 7:"quote_count",
               8:"id",9:"author_id",10:"conversation_id",11:"in_reply_to_user_id",12:"geo"}
            df = df.rename(header_dict_user_tweets,axis = 1)

        elif table_name == 'keyword_tweets':
            
            header_dict_keyword_tweets =  {0:"created_at",1:"screen_name",2:'text',3:'lang',
               4:'retweet_count',5:'reply_count',6:'like_count', 7:"quote_count",
               8:"id",9:"author_id",10:"conversation_id",11:"in_reply_to_user_id",12:"geo",
               13:'entities'}

            df = df.rename(header_dict_keyword_tweets,axis = 1)

        return df 

    @classmethod
    def write(cls, table_name: str, data: pd.DataFrame, path: str) -> None:
        engine = create_engine(DB().location.format(path=path), echo=False)
        logger.info('Writing {rows} rows to table {table}'.format(rows=len(data), table=table_name))
        if data.index[0] != 0:
            data = data.reset_index()
        data.to_sql(table_name, con=engine, if_exists='append', index=False)


class Follow:

    def __init__(self):
        self.sleep_time = 15
        self.url = 'https://api.twitter.com/2/users/{user_id}/{kind}'

    @classmethod
    def create_headers(cls, bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token), 'User-Agent': 'v2FollowingLookupPython'}
        return headers

    @classmethod
    def custom_params(cls):
        return {"user.fields": "created_at", 'max_results': 5}

    @classmethod
    def _fetch(cls, user_id: str, kind: str = 'following', target_total: int = 100,
               token_number: int = 0) -> pd.DataFrame:
        """
        Helper function
        """
        url = Follow().url.format(user_id=user_id, kind=kind)
        headers = Follow.create_headers(bearer_token=eval('BEARER{token}'.format(token=token_number)))
        params = Follow.custom_params()
        counter, results = 0, []
        while counter < target_total:
            response = requests.request("GET", url, headers=headers, params=params)
            if response.status_code == 429:
                logger.info(f'Status code 429: sleeping for {Follow().sleep_time} minutes')
                time.sleep(int(60 * Follow().sleep_time))
                continue
            if response.status_code != 200:
                continue
            data = json.loads(response.text)
            if not 'data' in data.keys():
                break
            data, meta = pd.DataFrame(data['data']), data['meta']
            counter += len(data)
            if isinstance(data, pd.DataFrame):
                results.append(data)
            if not 'next_token' in list(meta.keys()):
                break
            else:
                params['next_token'] = meta['next_token']
        if len(results) > 0:
            return pd.concat(results, axis=0)
        return pd.DataFrame({'username': ''}, index=[0])

    @classmethod
    def fetch(cls, users: Optional[List[str]] = None, user_ids: Optional[List[str]] = None, kind: str = 'following',
              target_total: int = 100, token_number: int = 0) -> pd.Series:
        """
        Demo
        users = ['barackobama', 'justinbieber', 'katyperry', 'rihanna', 'cristiano', 'taylorswift13', 'arianagrande']
        test = Follow.fetch(users=users, target_total=5, token_number=2)
        """
        if isinstance(users, list):
            info = User.user_info(users=users).set_index('username').id.to_dict()
            users, ids = list(info.keys()), list(info.values())
        elif isinstance(user_ids, list):
            users, ids = user_ids, user_ids
        else:
            raise('Must have either users or user ids specified')
        data_list = [Follow._fetch(user_id=k, kind=kind, target_total=target_total,
                                   token_number=token_number) for k in ids]
        return pd.Series(dict(zip(users, [','.join(j.username) for j in data_list])))


class History:

    @classmethod
    def create_headers(cls, bearer_token):
        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        return headers

    @classmethod
    def custom_params(cls, arg: Dict[str, str], start_date: str, end_date: str, max_results: int = 10):
        """
        Example of arg:
        {"value": "#DOGE"}
        {"value": "#AAVE -is:retweet"}
        {"value": "#BTC from:elonmusk"}
        Parameters
        ----------
        arg
        start_date
        end_date
        max_results

        Returns
        -------

        """
        return {
            'start_time': '{}T00:00:00Z'.format(start_date),
            'end_time': '{}T00:00:00Z'.format(end_date),
            'tweet.fields': 'id,author_id,created_at,in_reply_to_user_id,possibly_sensitive,public_metrics,lang,source,entities,geo,conversation_id',
            'max_results': max_results,
            'expansions': 'attachments.media_keys,author_id,geo.place_id',
            'media.fields': 'duration_ms,media_key,url,type,public_metrics',
            'user.fields': 'username',
            'query': arg['value']
        }
    @classmethod
    def custom_params_recent(cls, arg: Dict[str, str], max_results: int = 10):
        """
        Example of arg:
        {"value": "#DOGE"}
        {"value": "#AAVE -is:retweet"}
        {"value": "#BTC from:elonmusk"}
        Parameters
        ----------
        arg
        max_results

        Returns
        -------

        """
        return {
             'tweet.fields': 'id,author_id,created_at,in_reply_to_user_id,possibly_sensitive,public_metrics,lang,source,entities,geo,conversation_id',
            'max_results': max_results,
            'expansions': 'attachments.media_keys,author_id,geo.place_id',
            'media.fields': 'duration_ms,media_key,url,type,public_metrics',
            'user.fields': 'username',
            'query': arg['value']
        }
    @classmethod
    def fetch(cls, keyword: str, start_date: str, end_date: str, max_results: int = 100, target_total: int = 1000,
                   token_number: int = 0, sleep_time: int = 15, tag: str = '', language: str = 'en', retweets: bool = False) -> pd.DataFrame:
        """
        Usage:
        data = History.fetch(keyword='HEX', start_date='2021-08-23', end_date='2021-09-23')

        Parameters
        ----------
        keyword
        start_date
        end_date
        max_results
        target_total
        token_number
        sleep_time

        Returns
        -------

        """
        counter = 0
        results = []
        headers = History.create_headers(bearer_token=eval('BEARER{}'.format(str(token_number))))
        retweet_status = '-is:retweet' if not retweets else retweets
        if len(tag) == 1:
            keyword = tag + keyword
        params = History.custom_params(arg={"value": "{keyword} lang:{language} {retweet_status}".format(keyword=keyword, language=language, retweet_status=retweet_status)},
                                       start_date=start_date, end_date=end_date, max_results=max_results)
        while counter < target_total:
            time.sleep(1)
            response = requests.request("GET", SEARCH, headers=headers, params=params)
            if response.status_code == 429:
                logger.info('Sleeping')
                time.sleep(int(60 * sleep_time))
                continue
            if response.status_code != 200:
                continue
            data = json.loads(response.text)
            if not 'data' in data.keys():
                break
            data, meta = pd.DataFrame(data['data']), data['meta']
            logger.info('Fetched {} tweets'.format(len(data)))
            counter += len(data)
            data = pd.concat([data, data['public_metrics'].apply(pd.Series)], axis=1, sort=False).drop('public_metrics', axis=1)
            results.append(data)
            if not 'next_token' in list(meta.keys()):
                break
            else:
                params['next_token'] = meta['next_token']
        if len(results) > 0:
            df = pd.concat(results, axis=0, sort=False)
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].apply(lambda x: np.nan if x == np.nan
                    else str(x).encode('utf-8', 'replace').decode('utf-8'))
            if 'geo' not in df.columns:
                df['geo'] = 'None'
            if 'screen_name' not in df.columns:
                df['screen_name'] = 'None'
            if 'entities' not in df.columns:
                df['entities'] = 'None'
            column_list = ['created_at','screen_name','text','lang',
                                'retweet_count','reply_count','like_count','quote_count',
                                'id','author_id','conversation_id','in_reply_to_user_id','geo',
                                'entities']
            return df[column_list]

    @classmethod
    def fetch_recent(cls, keyword: str, max_results: int = 100, 
        target_total: int = 1000,token_number: int = 0, sleep_time: int = 15, tag: str = '', language: str = 'en', retweets: bool = False) -> pd.DataFrame:
            """
            Usage:
            data = History.fetch_recent(keyword='HEX')

            Parameters
            ----------
            keyword
            max_results
            target_total
            token_number
            sleep_time

            Returns
            -------

            """
            counter = 0
            results = []
            headers = History.create_headers(bearer_token=eval('BEARER{}'.format(str(token_number))))
            retweet_status = '-is:retweet' if not retweets else retweets
            if len(tag) == 1:
                keyword = tag + keyword
            params = History.custom_params_recent(arg={"value": "{keyword} lang:{language} {retweet_status}".format(keyword=keyword, language=language, retweet_status=retweet_status)},
                                           max_results=max_results)
            while counter < target_total:
                time.sleep(1)
                response = requests.request("GET", SEARCH_RECENT, headers=headers, params=params)
                if response.status_code == 429:
                    logger.info('Sleeping')
                    time.sleep(int(60 * sleep_time))
                    continue
                if response.status_code != 200:
                    continue
                data = json.loads(response.text)
                if not 'data' in data.keys():
                    break
                data, meta = pd.DataFrame(data['data']), data['meta']
                logger.info('Fetched {} tweets'.format(len(data)))
                counter += len(data)
                data = pd.concat([data, data['public_metrics'].apply(pd.Series)], axis=1, sort=False).drop('public_metrics', axis=1)
                results.append(data)
                if not 'next_token' in list(meta.keys()):
                    break
                else:
                    params['next_token'] = meta['next_token']
            if len(results) > 0:
                df = pd.concat(results, axis=0, sort=False)
                for col in df.columns:
                    if df[col].dtype == object:
                        df[col] = df[col].apply(lambda x: np.nan if x == np.nan
                        else str(x).encode('utf-8', 'replace').decode('utf-8'))
                if 'geo' not in df.columns:
                    df['geo'] = 'None'
                if 'screen_name' not in df.columns:
                    df['screen_name'] = 'None'
                if 'entities' not in df.columns:
                    df['entities'] = 'None'
                column_list = ['created_at','screen_name','text','lang',
                                    'retweet_count','reply_count','like_count','quote_count',
                                    'id','author_id','conversation_id','in_reply_to_user_id','geo',
                                    'entities']
                return df[column_list]
class Tweet:

    def __init__(self):
        self.MAX_RESULTS=100

    @classmethod
    def create_headers(cls, bearer_token: str) -> Dict[str, str]:
        return {"Authorization": "Bearer {}".format(bearer_token)}

    @classmethod
    def create_url(cls, user_id: str) -> str:
        return "https://api.twitter.com/2/users/{}/tweets".format(user_id)

    @classmethod
    def custom_params(cls, start_time: str, end_time: str) -> Dict[str, str]:
        return {"tweet.fields": "id,created_at,author_id,text,conversation_id,in_reply_to_user_id,lang,public_metrics",
                "start_time": '{}T01:00:00Z'.format(start_time),
                "end_time": '{}T01:00:00Z'.format(end_time),
                'max_results': Tweet().MAX_RESULTS}

    @classmethod
    def fetch_user_tweets(cls, user_id: str, start: str, end: str, max_results: int = 100, token: int=0) -> None:
        results = []
        results_length = 0
        url = Tweet.create_url(user_id)
        params = Tweet.custom_params(start_time=start, end_time=end)
        headers = Tweet.create_headers(eval('BEARER{}'.format(token)))
        try:
            while True:
                response = requests.request("GET", url, headers=headers, params=params)
                if response.status_code == 429:
                    logger.info('Sleeping')
                    time.sleep(15 * 60)
                    continue
                if response.status_code != 200:
                    raise Exception(response.status_code, response.text)
                data = json.loads(response.text)
                if not 'data' in data.keys():
                    break
                data, meta = pd.DataFrame(data['data']), data['meta']
                logger.info('Fetched {} tweets'.format(len(data)))
                data = pd.concat([data, data['public_metrics'].apply(pd.Series)], axis=1, sort=False).drop('public_metrics', axis=1)
                results.append(data)
                results_length += len(data)

                if results_length >= max_results: # Tweet().MAX_RESULTS:
                    break
                if not 'next_token' in list(meta.keys()):
                    break
                else:
                    params['pagination_token'] = meta['next_token']
            if len(results) > 0:
                df = pd.concat(results, axis=0)
                for col in df.columns:
                    if df[col].dtype == object:
                        df[col] = df[col].apply(
                            lambda x: np.nan if x == np.nan else str(x).encode('utf-8', 'replace').decode('utf-8'))
                
                if 'geo' not in df.columns:
                    df['geo'] = 'None'
                if 'screen_name' not in df.columns:
                    df['screen_name'] = 'None'
                column_list = ['created_at','screen_name','text','lang',
                                'retweet_count','reply_count','like_count','quote_count',
                                'id','author_id','conversation_id','in_reply_to_user_id','geo']
                
                return df[column_list]

        except Exception as e:
            logger.error(e)
            pass


class User:


    @classmethod
    def create_headers(cls, bearer_token: str) -> Dict[str, str]:
        return {"Authorization": "Bearer {}".format(bearer_token)}

    @classmethod
    def create_url(cls, usernames: List[str]) -> pd.DataFrame:
        usernames = "usernames=" + ",".join(usernames)
        user_fields = "user.fields=description,created_at,id,location,name,public_metrics"
        return "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)

    @classmethod
    def userid_url(cls, userid: str) -> pd.DataFrame:
        user_fields = "user.fields=description,created_at,id,location,name,public_metrics"
        return "https://api.twitter.com/2/users/{}".format(userid)

    @classmethod
    def connect_to_endpoint(cls, url, headers, params=None) -> Dict[str, Any]:
        response = requests.request("GET", url, headers=headers)
        if response.status_code == 429:
            logger.info('Sleeping')
            time.sleep(15 * 60)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()
    
    @classmethod
    def user_info(cls, users: List[str], token: int = 0) -> pd.DataFrame:
        url = User.create_url(usernames=users)
        headers = User.create_headers(eval('BEARER{}'.format(token)))
        json_response = User.connect_to_endpoint(url, headers)
        data = json.loads(json.dumps(json_response, indent=4, sort_keys=True))
        try:
            user_info = pd.DataFrame(data['data'])
            features = user_info['public_metrics'].apply(pd.Series)
            data = pd.concat([user_info, features], axis=1).drop('public_metrics', axis=1)
            if 'location' not in data.columns:
                data['location'] = None
            column_list = ['created_at','name','username',
            'description','id','location',
            'followers_count','following_count','listed_count','tweet_count']
            return data[column_list]
        except Exception as e:
            print(e)
            pass

    @classmethod
    def id_to_username(cls, user_id: str, token: int=0) -> str:
        try:
            headers = User.create_headers(bearer_token=eval('BEARER{number}'.format(number=token)))
            url = User.userid_url(userid=user_id)
            return User.connect_to_endpoint(url, headers)['data']['username']
        except Exception as e:
            logger.error(e)
            pass