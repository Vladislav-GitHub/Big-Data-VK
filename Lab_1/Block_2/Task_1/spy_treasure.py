
from mrjob.job import MRJob
import matplotlib.pyplot as plt

class MRPhrasesCount(MRJob):
    
    def mapper(self, _, line):
        words = line.replace("\"", "").replace("\\", "").split()
        person = words[1]
        yield person, 1

    def reducer(self, person, counts):
        yield person, sum(counts)

    def reducer_top_persons(self, person_counts):
        top_persons = sorted(person_counts, key=lambda x: x[1], reverse=True)[:20]
        for person, count in top_persons:
            yield person, count

if __name__ == '__main__':
    MRPhrasesCount.run()
