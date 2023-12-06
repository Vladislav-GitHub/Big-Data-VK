
from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
from nltk import word_tokenize
from nltk.corpus import stopwords
import string
import nltk

class MRBigram(MRJob):
    
    def mapper_init(self):
        nltk.download('punkt')
        nltk.download('stopwords')

    def mapper(self, _, line):
        words = word_tokenize(line.lower())
        stop_words = set(stopwords.words('english'))
        words = [word for word in words if word.isalnum() and word not in stop_words]
        bigrams = [(words[i], words[i + 1]) for i in range(len(words) - 1)]

        for bigram in bigrams:
            yield bigram, 1

    def reducer(self, bigram, counts):
        yield None, (bigram, sum(counts))

    def reducer_top_bigrams(self, _, bigram_counts):
        top_bigrams = sorted(bigram_counts, key=lambda x: x[1], reverse=True)[:20]
        for bigram, count in top_bigrams:
            yield bigram, count

if __name__ == '__main__':
    MRBigram.run()
