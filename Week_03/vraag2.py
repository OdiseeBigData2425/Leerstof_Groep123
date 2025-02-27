# vraag 2: het aantal keer dat elk karakter voorkomt
from mrjob.job import MRJob 

class Vraag2(MRJob):
    def mapper(self, _, line): 
        for character in line:
             yield (character, 1)
         
    def reducer(self, character, counts):
        yield (character, sum(counts))

if __name__ == '__main__':
    Vraag2.run()
