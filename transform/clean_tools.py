import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


def cleaning(sent):
    """
        text cleaning function
    Args:
        sent([str]) : text to clean
    Returns:
        sent([list]) : list of words
    """
    punc = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    for word in sent:
        if word in punc:
            sent = sent.replace(word,"")
    # removing emoji
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE)
    sent = regrex_pattern.sub(r'', sent)
    # removing stopwords
    """ this function permit to remove all stop words.
    it take a text as arg and return a list """
    stop_words = set(stopwords.words("english"))
    word_tokens = word_tokenize(sent)
    filtered_sentence = [w for w in word_tokens if not w.lower() in stop_words]
    return filtered_sentence


