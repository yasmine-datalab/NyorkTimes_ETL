"""Construction de modèle de NLP"""
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob

# modèle

class SentimentModel:
    """
        
    
    """
    @classmethod
    def get_sentiment(cls,text):
        """ Fonction retournant le score et caractère du texte"""
        nlp = spacy.load('en_core_web_sm')
        
        #spacy_text_blob = SpacyTextBlob()
        
        nlp.add_pipe("spacytextblob")
        doc = nlp(text)
        # scores du modèle
        polarity = doc._.polarity
        subjectivity = doc._.subjectivity
        assessments = doc._.assessments
        if polarity > 0:
            message = "positif"
        else:
            message = "negatif"
        return message