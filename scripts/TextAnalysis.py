import re
import numpy as np
from typing import *
import gensim
import pandas as pd
import textwrap as tr

#clean tweet text for doing TF, TF-IDF embeddings
def clean_tweet(elem):
    s1 =re.sub(r"([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?|u2026|u2019|u2019s|u2014|amp", "", elem.lower())
    return s1.strip()

def word2vec_embedding(list_of_docs:List[str], 
                       model:gensim.models.keyedvectors.Word2VecKeyedVectors)-> np.ndarray:
    features = []

    for tokens in list_of_docs:
        zero_vector = np.zeros(model.vector_size)
        vectors = []
        for token in tokens:
            if token in model.wv:
                try:
                    vectors.append(model.wv[token])
                except KeyError:
                    continue
        if vectors:
            vectors = np.asarray(vectors)
            avg_vec = vectors.mean(axis=0)
            features.append(avg_vec)
        else:
            features.append(zero_vector)
    return np.array(features)
    
#measure sentiment of text from a pre-trained transformer model+tokenizer
def sentiment_classifier(text,model,tokenizer):
    inputs = tokenizer.encode_plus(text, return_tensors='pt', add_special_tokens=True)

    token_type_ids = inputs['token_type_ids']
    input_ids = inputs['input_ids']

    output = model(input_ids, token_type_ids=token_type_ids,return_dict=True,output_hidden_states=True)
    logits = np.array(output.logits.tolist()[0])
    prob = np.exp(logits)/np.sum(np.exp(logits))
    sentiment = np.sum([(x+1)*prob[x] for x in range(len(prob))])  #use this line if you want the mean score
    #sentiment = logits.argmax()  #use this line if you just wanted the most likely score
    embedding = output.hidden_states[12].detach().numpy().squeeze()[0]
    return sentiment,embedding

#this is is used for multicore computation
def _sentiment_classifier(data):
    text,model, tokenizer = data
    sentiment,_ =  sentiment_classifier(text,model,tokenizer)
    #print(f"Sentiment:{sentiment:.2f}\nText: {text}\n")
    return sentiment

#display output text from generator
def display_text(outputs):
    for count,output in enumerate(outputs):
        text = output['generated_text']
        text = text.replace("\n"," ").replace("\r"," ")
        print(f"{count}: {tr.fill(text,width = 50)}")
    return None

#sample words from a generator given an input text
def sample_words(input_text,generator,nsamples):
    max_length = len(input_text.split(" "))+1
    outputs = generator(input_text, 
                             max_length=max_length,
                            pad_token_id = 50256,                         
                            num_return_sequences=nsamples,
                             do_sample=True, 
                            top_k=0
                            )

    W = []
    for count,output in enumerate(outputs):
        text = output['generated_text']
        text = text.replace("\n"," ").replace("\r"," ").replace(input_text,"")
        W.append(text)
    counter = Counter(W)

    words = [x for x in counter.keys()]
    freqs = [x for x in counter.values()]

    df_freq = pd.DataFrame({'word':words,'freq':freqs})
    df_freq.sort_values(by = 'freq', ascending = False, inplace = True)
    df_freq.head()
    return df_freq