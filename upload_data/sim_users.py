import time
import re
import os
from typing import List

from hugchat import hugchat
from hugchat.login import Login


class reviewGenerator:
    """
    Review generator using Hugging Face chatbot
    """
    def __init__(self, email: str, pwd: str):
        """
        Initializes chatbot with email and password required by Hugging Face
        """
        self.email = email
        self.pwd = pwd
        
    def __initialize_chat__(self) -> hugchat.ChatBot:
        """
        Initializes chatbot with required cookies
        """
        sign = Login(self.email, self.pwd)
        cookies = sign.login()
        sign.saveCookiesToDir()
        chatbot = hugchat.ChatBot(cookies=cookies.get_dict())
        
        return chatbot
    
    def __delete_chat__(self, chatbot: hugchat.ChatBot) -> None:
        """
        Delete last conversation with chatbot
        """
        chat_id = chatbot.get_conversation_list()
        chatbot.delete_conversation(conversation_id = chat_id[0])

    def __generate__(self, product: str, features: str) -> str:
        """
        Generate answers from chatbot. Returns string.
        """
        chatbot = self.__initialize_chat__()
        prompt = f"""
        Print a numbered list with 10 short user reviews about {product} being sold on an e-commerce, simulating different styles, and taking into account the following features: {features}. 
        """
        result = chatbot.chat(prompt)
        self.__delete_chat__(chatbot)
    
        return result
    
    def generate(self, num_reviews: int, product: str, features: str) -> List[str]:
        """
        Generate at least num_reviews answers from chatbot. Returns list of reviews.
        """
        reviews = []
        while len(reviews) < num_reviews:
            try:
                result = self.__generate__(product, features)
                texts = re.findall(r'"([^"]*)"', result)
                texts = [element.replace('"', '').replace("'", "") for element in texts if len(element.split(" ")) > 8]
                if len(texts) > 0:
                    reviews.extend(texts)
                    print(f"{len(reviews)} reviews generated...")
            except:
                time.sleep(5)
                print("Something went wrong. Retrying...")
            
        return reviews