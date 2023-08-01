import numpy as np
from scipy.spatial import distance
import pandas as pd 
class VectorDatabase:
    def __init__(self,nlp,model):
        self.vectors = {}
        self.nlp = nlp
        self.model = model
        self.very_similar = 0.5
        self.similar = 0.5
        

    def split_sentences(self, text):
        text = text.replace(',','.').replace('and','.').replace('but','.')
        doc = self.nlp(text, disable=["ner"])
        roots = [token  for token in doc if token.dep_ == "ROOT" ]
    
        texts = []
        for root in roots:
            token_list = [e.i for e in root.subtree]
            token_list = list(dict.fromkeys(token_list))
            token_list.sort()
            text = ' '.join([doc[i].text for i in token_list ])
            texts.append(text.lower().strip())
            
        return texts#text.replace(',','.').replace('but','.').split('.')


    def insert(self, sentence: str, type: str) -> None:
        model = self.model
        #embeddings = model.encode(sentence)
        embeddings = list(model.encode([sentence])[0])
        key = len(self.vectors) + 1
        self.vectors[key] = {'text': sentence,
                             'type': type,
                             'vector': embeddings}

    def search(self, query: str):
        model = self.model
        query_vector = list(model.encode([query])[0])
        
        similarities = [(key, value['text'],distance.cosine(query_vector, value['vector']),value['type']) for key, value in self.vectors.items()]
        

        aux = pd.DataFrame(similarities)
        
        aux.columns = ['index_db','text','similarity','topic']
                
        return  aux

    def long_search(self, query: str):
        topics = []
        for str in self.split_sentences(query):
            topics_this = self.search(str)
            topics.append(topics_this)
            

        topics = pd.concat(topics)[['similarity','topic']].groupby(['topic']).min().reset_index()

        
        aux = pd.DataFrame(list(topics.similarity)).transpose()
        aux.columns = list(topics.topic)
        

        return  aux