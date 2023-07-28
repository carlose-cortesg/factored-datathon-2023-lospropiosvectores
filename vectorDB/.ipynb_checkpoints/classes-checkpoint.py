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
        doc = self.nlp(text, disable=["ner"])
        roots = [token  for token in doc if token.dep_ == "ROOT" ]
    
        texts = []
        for root in roots:
            token_list = [e.i for e in root.subtree]
            token_list = list(dict.fromkeys(token_list))
            token_list.sort()
            text = ' '.join([doc[i].text for i in token_list ])
            texts.append(text.lower().strip())
            
        return texts


    def insert(self, sentence: str, polarity: int, type: str) -> None:
        model = self.model
        embeddings = model.encode(sentence)
        key = len(self.vectors) + 1
        self.vectors[key] = {'text': sentence,
                             'polarity': polarity,
                             'type': type,
                             'vector': embeddings}

    def search(self, query: str):
        model = self.model
        query_vector = model.encode(query)
        
        similarities = [(key, value['text'],distance.cosine(query_vector, value['vector']),value['polarity'],value['type']) for key, value in self.vectors.items()]
        

        aux = pd.DataFrame(similarities)
        aux.columns = ['index_db','text','similarity','polarity','topic']
        aux = aux.sort_values(by=['similarity']).reset_index(drop=True).reset_index()

        #aux = aux.reset_index().query('index<20 or similarity<0.7').query('similarity<1')[['index','topic']].groupby(['topic']).count()
        
        aux = aux.query('index<=10')
        #aux = aux.query('similarity <={}'.format(self.very_similar))

        aux = aux.query('similarity <={}'.format(self.similar))
        
        aux = aux[['index','topic']].groupby(['topic']).count()
        
        
        #aux['index2'] = aux['index']/aux['index'].sum()

        
        

        aux = aux.sort_values(by='index', ascending=False).head(1)
                
        return  list(aux.index.unique())

    def long_search(self, query: str):
        topics = []
        for str in self.split_sentences(query):
            topics_this = self.search(str)
            if len(topics_this)>0:
                mini_df = pd.DataFrame(topics_this)
                mini_df.columns = ['topic']
                mini_df['review'] = query
                mini_df['sub_review'] = str
                topics.append(mini_df)
        if len(topics)>0:
            
            aux = pd.concat(topics)
            #aux ['stars'] = [int(self.sentiment_pipe(str)[0]['label'][0]) for str in aux.sub_review]
        else:
            aux = None
            
        return  aux

    def set_th(self):
        data = pd.DataFrame(self.vectors).transpose()

        same_type_similarity = []
        
        same_type_top_similarity = []
        
        for i in range(len(data.vector)):
        
            vectors = data.vector
            vector = vectors.values[i]
            aux = pd.DataFrame(
                [distance.cosine(vector, vectors[i]) for i in vectors.keys()]
            )
            
            aux.columns = ['similarity']
            
            aux['topic'] = data.type.values
            
            topic_review = data.type.values[i]
             
            same_type_similarity.append(np.percentile(aux.query(f'topic=="{topic_review}"').similarity,75))
        
            same_type_top_similarity.append(np.percentile(aux.query(f'topic=="{topic_review}"').similarity,25))
        
        self.very_similar = np.mean(same_type_top_similarity)
        self.similar = np.percentile(same_type_similarity,75)


