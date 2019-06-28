import spacy
from vaderSentiment import vaderSentiment
nlp = spacy.load("en_core_web_lg")
"Spacy is loaded"


class S

query = nlp("I love this :)")
sentences = [str(s) for s in query.sents]
print(sentences)

analyzer = vaderSentiment.SentimentIntensityAnalyzer()
sentiment = [analyzer.polarity_scores(str(s)) for s in sentences]

print(sentiment)