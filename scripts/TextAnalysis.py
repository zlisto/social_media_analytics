import re
import numpy as np


#clean tweet text for doing TF, TF-IDF embeddings
def clean_tweet(elem):
    s1 =re.sub(r"([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?|u2026|u2019|u2019s|u2014|amp", "", elem.lower())
    return s1.strip()


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

