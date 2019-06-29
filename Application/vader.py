import spacy
from vaderSentiment import vaderSentiment
nlp = spacy.load("en_core_web_lg")



query = nlp("Just watched grownish and it feels like Aaron broke my heart and not Anna am so sad")
sentences = [str(s) for s in query.sents]
print(sentences)

analyzer = vaderSentiment.SentimentIntensityAnalyzer()
sentiment = [analyzer.polarity_scores(str(s)) for s in sentences]

print(sentiment)