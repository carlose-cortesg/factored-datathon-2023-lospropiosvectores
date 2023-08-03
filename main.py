import os
from typing import Union
from fastapi import FastAPI

from vectorDB.classes import VectorDatabase
from vectorDB.utils import create_database, save_as_dict



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sa.json'

app = FastAPI()
vector_db = create_database()

@app.get('/ready')
def ready():    
    return {'status':'ok'}
    
@app.get('/help')
async def any_question(question: str, product: str, k: int = 10, limit: Union[int, None] = None):

    query_lim = '' if limit is None else f'LIMIT {limit}'
    answers = vector_db.insert(question, question).ask_reviews(product, query_lim)
    vector_db.delete(question)

    saved_answers = save_as_dict(answers, k)
    
    return saved_answers