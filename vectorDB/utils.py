import spacy
import pandas as pd
from fast_sentence_transformers import FastSentenceTransformer as SentenceTransformer
from vectorDB.classes import VectorDatabase

def create_database() -> VectorDatabase:
    
    model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu', quantize=True)
    nlp = spacy.load('en_core_web_lg')
    
    return VectorDatabase(nlp, model)


def save_as_dict(answers: pd.DataFrame, k: int = 10) -> dict:
    
    result = {key:row 
              for key, row 
              in enumerate(answers.sort_values(answers.columns[0]).iloc[:k,1])}
    
    return result