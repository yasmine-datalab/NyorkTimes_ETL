"""Construction de modèle de NLP"""
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob


class SentimentModel:
    """ """
    @classmethod
    def get_sentiment(cls, text: str):
        """
            get sentiment about a text
        Args:
            text([string]): text to analyse
        Returns:
            sentiment ([string]) : positif or negatif
        """
        nlp = spacy.load('en_core_web_sm')
        nlp.add_pipe("spacytextblob")
        doc = nlp(text)
        # model performance
        polarity = doc._.polarity
        subjectivity = doc._.subjectivity
        assessments = doc._.assessments
        # decision
        if polarity > 0:
            sentiment = "positif"
        else:
            sentiment = "negatif"
        return sentiment