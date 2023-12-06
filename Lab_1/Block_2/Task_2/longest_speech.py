
from mrjob.job import MRJob
import re

class MRLongestPhrases(MRJob):

    def mapper(self, _, line):
        match = re.match(r'"(\d+)" "(.*?)" "(.*?)"', line)
        if match:
            character_id, character, dialog = match.groups()
            yield character, (len(dialog.split()), dialog)

    def reducer(self, character, pairs):
        longest_phrase = max(pairs, key=lambda x: x[0])
        yield None, (character, longest_phrase)

    def mapper_sort(self, _, long_char):
        yield long_char[1][0], long_char[1][1]

    def reducer_sort(self, character, longest_phrases):
        yield character, max(longest_phrases, key=lambda x: len(x[1]))

if __name__ == '__main__':
    MRLongestPhrases.run()
