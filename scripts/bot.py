from requests_oauthlib import OAuth1Session
import os
import json
import io
import urllib.request
from typing import *
from scripts.config_bot import credentials, user_id, urls


__author__ = 'kqureshi'


class Bot:

    @classmethod
    def fetch_auth(cls) -> None:
        """

        """
        consumer_key, consumer_secret = credentials['consumer_key'], credentials['consumer_secret']
        oauth = OAuth1Session(consumer_key, client_secret=consumer_secret)
        request_token_url = urls['request_token']
        fetch_response = oauth.fetch_request_token(request_token_url)
        resource_owner_key = fetch_response.get("oauth_token")
        resource_owner_secret = fetch_response.get("oauth_token_secret")
        base_authorization_url = urls['authorize']
        authorization_url = oauth.authorization_url(base_authorization_url)
        verifier = input("Please paste the pin from \n {} \n here: ".format(authorization_url))
        oauth = OAuth1Session(
            consumer_key,
            client_secret=consumer_secret,
            resource_owner_key=resource_owner_key,
            resource_owner_secret=resource_owner_secret,
            verifier=verifier,
        )
        access_token_url = urls['access_token']
        oauth_tokens = oauth.fetch_access_token(access_token_url)
        access_token = oauth_tokens["oauth_token"]
        access_token_secret = oauth_tokens["oauth_token_secret"]
        return OAuth1Session(consumer_key,
                             client_secret=consumer_secret,
                             resource_owner_key=access_token,
                             resource_owner_secret=access_token_secret,
                             )

    @classmethod
    def tweet(cls, text: str, reply_to: Optional[str] = None, media_url: Optional[str] = None,
              oauth: OAuth1Session = None) -> None:
        """

        """
        params = {"text": '{}'.format(str(text))}
        if isinstance(media_url, str):
            buffer = io.BytesIO(urllib.request.urlopen(media_url).read())
            media_response = oauth.post(urls['media'], files={"media": ('image.png', buffer)})
            media_string = media_response.json()['media_id_string']
            params['media'] = {'media_ids': [media_string]}
        if isinstance(reply_to, str):
            params['reply'] = {"in_reply_to_tweet_id": reply_to}
        response = oauth.post(urls['tweets'], json=params)
        return

    @classmethod
    def delete_tweet(cls, tweet_id: str, oauth: OAuth1Session = None) -> None:
        """

        """
        response = oauth.delete("{endpoint}/{tweet_id}".format(tweet_id=tweet_id, endpoint=urls['tweets']))
        if response.status_code != 200:
            raise Exception("Failed to delete tweet")
        return

    @classmethod
    def retweet(cls, tweet_id: str, oauth: Optional[OAuth1Session] = None) -> None:
        """

        """
        response = oauth.post(urls['retweets'].format(user_id=user_id),
                              json={"tweet_id": "{tweet_id}".format(tweet_id=tweet_id)})
        if not response.json()['data']['retweeted']:
            print('Failed to retweet')
        return

    @classmethod
    def like(cls, tweet_id: str, oauth: Optional[OAuth1Session] = None) -> None:
        """

        """
        response = oauth.post(urls['like'].format(user_id=user_id),
                                                  json={"tweet_id": "{tweet_id}".format(tweet_id=tweet_id)})
        if not response.json()['data']['liked']:
            raise Exception('Failed to like tweet')
        return

    @classmethod
    def follow(cls, follow_id: str, oauth: Optional[OAuth1Session] = None) -> None:
        """

        """
        response = oauth.post(urls['follow'].format(user_id),
                              json={"list_id": "{follow_id}".format(follow_id=follow_id)})
        return

    @classmethod
    def update_description(cls, description: str, oauth: OAuth1Session = None) -> None:
        """

        """
        response = oauth.post(urls['bio'], data={'description': '{description}'.format(description=description)})
        if not response.status_code == 200:
            raise Exception('Failed to update profile description')
        return
