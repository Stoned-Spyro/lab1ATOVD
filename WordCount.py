from mrjob.job import MRJob
from mrjob.step import MRStep
class MRWordCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,reducer=self.reducer_count_words)
        ]
    def mapper_get_words(self, _, line):
        for word in line.split():
            yield word, 1
    def reducer_count_words(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    MRWordCount.run()