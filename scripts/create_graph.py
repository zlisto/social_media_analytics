# -*- coding: utf-8 -*-
"""
Created on Sat Jun 27 01:02:44 2020

@author: Zlisto
"""
import numpy as np
import re
import pandas as pd
import networkx as nx
from collections import Counter


#functions to pull out source tweet text and source tweeter from a retweet string
def extract_source(text):
    x = re.match(".*RT\W@(\w+):", text)
    if x:
        return x.groups()[0]
    else:
        return None

def interaction_network_from_tweets(keyword_tweets: pd.DataFrame):
    G = nx.DiGraph()

    for index,row in keyword_tweets.iterrows():
        try:
            e = eval(row.entities)
            tweeter_id = row.author_id
            mentions = e['mentions']
            G.add_node(tweeter_id, username = "None")
            for mention in mentions:
                mentioned_id = mention['id']
                mentioned_username = mention['username']
                G.add_node(mentioned_id, username = mentioned_username)
                G.add_edge(mentioned_id,tweeter_id)               
        except:
            pass  
    nv = G.number_of_nodes()
    ne = G.number_of_edges()
    print(f"G has {nv} nodes and {ne} edges")

    return G     
    
def retweet_network_from_tweets(df,cmax=1e9):
   c=0
   e=0
   EdgeList = []
   G = nx.DiGraph()

   for key,tweet in df.iterrows():
       c+=1
       if c> cmax:break
       if c%100000==0:print("\tTweet %s of %s, %s retweets"%(c,len(df),e))
       if 'RT @' in tweet.text:
           try:
               source = extract_source(tweet.text)
               if source:
                   retweeter = tweet.screen_name
                   EdgeList.append((source,retweeter))
                   e +=1
           except:
               pass
               #print('Error with tweet %s: %s'%(tweet['screen_name'],tweet['text']))
   W = Counter(EdgeList)
   ne = len(W)
   print("went through %s tweets, found %s retweets, %s retweet edges"%(c,e,ne))
   
   e = 0
   for edge in W.keys():  
       source = edge[0]
       retweeter = edge[1]
       if not(G.has_node(source)):
           G.add_node(source)
       if not(G.has_node(retweeter)):
           G.add_node(retweeter)
       G.add_edge(source, retweeter, weight = W[edge])
   
   nv = G.number_of_nodes()
   ne = G.number_of_edges()
   print('Retweet network has %s nodes and %s edges'%(nv,ne))
   return G    


def follower_network_from_dataframe(df: pd.DataFrame):
    Users = df.screen_name.tolist()   
    c = 0  #counter for all edges in df
    G = nx.DiGraph() 
    for index,row in df.iterrows():
        user = row.screen_name
        Following = row.following.replace("[","").replace("]","").replace("'","").split(",")
        for following in Following:
            c+=1
            if following in Users:
                G.add_edge(following,user)
                print(f"({following},{user})")
    ne = G.number_of_edges()
    nv = G.number_of_nodes()
    print(f"df has {c} following edges.\nFollowing network has {nv} nodes, {ne} edges")
    return G
  
def following_networkx_from_following_list(filename):
    Following = []
    Screen_name = []
    ne = 0
    with open(filename) as fp:
        for cnt, line in enumerate(fp): 
            line = line.strip('\n')
            users =line.split(",")
            follower = users[0]
            following = users[1:]
            Following.append(following)
            Screen_name.append(follower)
    
    df =  pd.DataFrame({'screen_name':Screen_name,'following':Following})
    G = nx.DiGraph()
    V = [node for node in df.screen_name]
    for index,row in df.iterrows():
        node = row.screen_name
        for following in row.following:
            if following in V:
                G.add_edge(following,node)
    
    ne = G.number_of_edges()
    nv = G.number_of_nodes()
    print(f"{nv} nodes, {ne} edges")
    return(G)
            
   
def retweet_similarity_network(G):
    V = list(G.nodes())
    print(f'{len(V)} nodes in retweet network')

    ebunch = []
    for counter,u in enumerate(V):
        for v in V[counter+1:]:
            if (G.has_node(v)) and (G.has_node(u)):
                ebunch.append((u,v))
    preds = nx.jaccard_coefficient(G.to_undirected(),ebunch)
    print(len(ebunch), " node pairs to check Jaccard index")

    print("Create similarity graph between nodes using Jacard coefficient based on retweets")
    counter = 0
    Gsim = nx.Graph()
    ne = 0
    for u, v, s in preds:
        counter+=1
        if s >0:
            Gsim.add_edge(u, v, weight=s)
            ne+=1
        if counter%1e6==0:print(counter,ne, " positive weights")
    nv = Gsim.number_of_nodes()
    ne = Gsim.number_of_edges()
    print("Gsim has %s nodes, %s edges"%(nv,ne))
    return Gsim               
                
                
                
