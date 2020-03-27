#!/usr/bin/env python

#https://github.com/sebgoa/ksources/tree/master/python

import os
import json
import requests
import time

from flask import Flask, jsonify, redirect, render_template, request, Response

from google.cloud import secretmanager

import tweepy

from tweepy.streaming import StreamListener

import tweepy
#from tweepy import OAuthHandler
#from tweepy import Stream

# https://kafka-python.readthedocs.io/en/master/usage.html
from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

sink_url = os.environ['SINK']

#PROJECT_ID = os.environ.get('yokicloud')
PROJECT_ID = 'yokicloud'

secrets = secretmanager.SecretManagerServiceClient()
TWITTER_KEY = secrets.access_secret_version("projects/"+PROJECT_ID+"/secrets/twitter-key/versions/2").payload.data.decode("utf-8")
TWITTER_SECRET = secrets.access_secret_version("projects/"+PROJECT_ID+"/secrets/twitter-secret/versions/1").payload.data.decode("utf-8")
ACCESS_TOKEN = secrets.access_secret_version("projects/"+PROJECT_ID+"/secrets/twitter-access-key/versions/2").payload.data.decode("utf-8")
ACCESS_SECRET = secrets.access_secret_version("projects/"+PROJECT_ID+"/secrets/twitter-access-secret/versions/2").payload.data.decode("utf-8")



auth = tweepy.OAuthHandler(TWITTER_KEY, TWITTER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
#api = tweepy.API(auth,wait_on_rate_limit=True)
api = tweepy.API(auth)


def produce(self, topic, message):
        client = self.p
        msg = client.produce(topic, value=message, callback=self.acked)
        return msg


def make_msg(message):
    msg = '{"msg": "%s"}' % (message)
    return msg

def get_tweet():
    tweets = []
    for tweet in api.search(q='news',count=100,
                        lang="en",
                        since="2019-06-01"):
        newmsg = make_msg(tweet.text)
    
        tweets.append(newmsg)

    #return Response(json.dumps(tweets),  mimetype='application/json')
    return tweets


body = {"Hello":"World"}
headers = {'Content-Type': 'application/cloudevents+json'}

while True:
    body = get_tweet()
    requests.post(sink_url, data=json.dumps(body), headers=headers)
    time.sleep(15)
