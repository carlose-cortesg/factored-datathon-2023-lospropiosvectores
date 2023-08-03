from typing import List
from typing_extensions import Self
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tqdm import tqdm
from google.cloud import bigquery
from spacy.lang.en import English
from scipy.spatial import distance
from fast_sentence_transformers.FastSentenceTransformer import FastSentenceTransformer

class VectorDatabase:
    '''
    Class to set a vector database to answer questions over collections of texts
    '''
    def __init__(self, nlp:English, model:FastSentenceTransformer) -> None:
        '''
        Initializes vector database.
  
        Args:
            nlp (spacy corpus): Model used to tokenize by root all texts.
            model (hugging face model): Model used to encode into latent space all texts.
    
        Returns:
            None
        '''
        self.vectors = {}
        self.nlp = nlp
        self.model = model
        self.very_similar = 0.5
        self.similar = 0.5
        
        
    def __split_sentences__(self, text: str) -> List[str]:
        '''
        Split sentences by roots
  
        Args:
            text (str): Text to tokenize.
    
        Returns:
            List of strings
        '''
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
            
        return texts


    def insert(self, sentence: str, type: str) -> Self:
        '''
        Insert questions into database latent space
  
        Args:
            sentence (str): Question to be encoded.
            type (str): Feature related to the question.
            
        Returns:
            Vector database
        '''
        model = self.model
        embeddings = list(model.encode([sentence])[0])

        if type in [values['type'] for values in self.vectors.values()]:
            raise Exception('A question for this feature have already been inserted!')

        key = len(self.vectors) + 1
        self.vectors[key] = {'text': sentence,
                             'type': type,
                             'vector': embeddings}
        
        return self
    
    
    def delete(self, type: str) -> Self:
        '''
        Delete questions into database latent space
  
        Args:
            type (str): Feature related to the question to be deleted.
            
        Returns:
            Vector database
        '''
        key = 1
        new_vectors = {}
        for value in self.vectors.values():
            if value['type'] != type:
                new_vectors[key] = value
                key += 1
        
        self.vectors = new_vectors
        
        return self
    
    
    def modify(self, sentence: str, type: str) -> Self:
        '''
        Modify questions into database latent space
  
        Args:
            sentence (str): New question.
            type (str): Feature related to the question to be modified.
            
        Returns:
            Vector database
        '''
        model = self.model
        embeddings = list(model.encode([sentence])[0])
        
        for key, value in self.vectors.items():
            if value['type'] == type:
                self.vectors[key] = {'text': sentence,
                                     'type': type,
                                     'vector': embeddings}
                
        return self
    
    
    def ls(self) -> Self:
        '''
        List questions on database latent space
  
        Args:
            None.
            
        Returns:
            Vector database
        '''
        for key, question in self.vectors.items():
            print(f'''
            ----------------------------------------------------------------------------
                                                {question['type']}
            ----------------------------------------------------------------------------
            {key}. {question['text']}
            ''')
        print('''
        ======================================END===================================
        ''')
        
        return self


    def search(self, query: str) -> pd.DataFrame:
        '''
        Search the most similar question on database latent space for the text passed within query
  
        Args:
            query (str): Text searched.
            
        Returns:
            Pandas dataframe
        '''
        model = self.model
        query_vector = list(model.encode([query])[0])
        
        similarities = [(key, value['text'],distance.cosine(query_vector, value['vector']),value['type']) for key, value in self.vectors.items()]
        

        aux = pd.DataFrame(similarities)
        
        aux.columns = ['index_db','text','similarity','topic']
                
        return  aux

    
    def long_search(self, query: str, return_query: bool = False) -> pd.DataFrame:
        '''
        Tokenize by root and then search the most similar question on database latent space for each subtext passed
  
        Args:
            query (str): Text searched.
            
        Returns:
            Pandas dataframe
        '''
        topics = []
        for str in self.__split_sentences__(query):
            topics_this = self.search(str)
            topics.append(topics_this)
            

        topics = pd.concat(topics)[['similarity','topic']].groupby(['topic']).min().reset_index()

        
        aux = pd.DataFrame(list(topics.similarity)).transpose()
        aux.columns = list(topics.topic)
        
        if return_query:
            aux['text'] = query
        
        return  aux
    

    def __query_reviews__(self, product:str, limit: str = 'LIMIT 5000') -> pd.DataFrame:
        '''
        Query an specific product by ASIN and return outcome as dataframe
  
        Args:
            product (str): Item code.
            
        Returns:
            Pandas dataframe
        '''
        sql = f'''
        SELECT DISTINCT reviewText
        FROM `plenary-stacker-393921.factored.raw_reviews` 
        WHERE asin = '{product}' AND reviewText IS NOT NULL
        {limit}
        '''

        client = bigquery.Client()
        df = client.query(sql).result().to_dataframe()
    
        return df

    
    def ask_reviews(self, product: str, limit: str = 'LIMIT 5000') -> pd.DataFrame:
        '''
        Ask all queried reviews with the questions save on database
  
        Args:
            product (str): Item code.
            
        Returns:
            Pandas dataframe
        '''
        df = self.__query_reviews__(product, limit)
        
        with ThreadPoolExecutor() as executor:
            results = executor.map(lambda x: self.long_search(x, return_query=True), 
                                   df['reviewText'])

        all_reviews = pd.concat(results)
    
        return all_reviews
    