#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 25 21:17:43 2019

@author: sizhenhan
"""

from langdetect import detect
from nltk.stem.porter import PorterStemmer
import nltk
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from gensim.summarization.summarizer import summarize as gensim_summarize 
import string
from nltk.corpus import stopwords
from newspaper import Article

def get_text(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        text = article.text
    
        return text
    except:
        print(f'fail to download news from {url}')
        return None

class ProcessPipeline:
	def __init__(self,texts=None,steps=["langdetection","summarization",'tokenization']):
	    self.stemmer = PorterStemmer()
	    self.lemmatizer = nltk.WordNetLemmatizer()
	    self.texts = texts
	    self.steps = steps

	def process(self,text,return_str=False):
		if "langdetection" in self.steps:
			lang = self.detect_lang(text)
			if lang == "en":
				text =  text
			else:
				text = ""
		if "summarization"	in self.steps:
			text = self.summarize(text)
		if "tokenization" in self.steps:
			processed = self.pre_process(text,return_str=return_str)
			return processed
		else:
			return text

	def run(self,return_str=False,workers=6):
	    with ProcessPoolExecutor(max_workers=workers) as executor:
	    	if return_str:
    	        	res = executor.map(self.process, self.texts,[True]*len(self.texts))        		
	    	else:
    	        	res = executor.map(self.process, self.texts)
	    return list(res)    

	def detect_lang(self,text):
	    try:
	        language = detect(text)
	        print(f"language is {language}")
	    except:
	        print("Not able to detect language")
	        language = "other"
	    return language

	def summarize(self,text,**kwargs):
	    try:
	        summarized = gensim_summarize(text,**kwargs)
	        return summarized
	    except:
	        return text

	def pre_process(self,text,return_str=False,steps=['remove_digits','remove_punctuation',"remove_stopwords",'lemmatization','stemmization'],whiteList=["n't", "not", "no"]):
	    if "remove_digits" in steps:
	    	text = text.translate(str.maketrans('', '',string.digits))
	    if "remove_punctuation"  in steps:
	    	text = text.translate(str.maketrans('', '', string.punctuation))
	    if "remove_stopwords" in steps:
	    	text = [word for word in text.split() if (word.lower() not in stopwords.words('english') and not word.startswith("http")) or (word.lower() in whiteList)]
	    if "lemmatization" in steps:
	    	lemmatizer = nltk.WordNetLemmatizer()
	    	text = list(map(lambda x:lemmatizer.lemmatize(x),text))
	    if "stemmization" in steps:
	    	stemmer = PorterStemmer()
	    	text = list(map(lambda x:stemmer.stem(x),text))
	    if return_str:
	    	return (' ').join(text)
	    else:
	    	return text