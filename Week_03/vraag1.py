# vraag 1: gemiddelde woordlengte
from mrjob.job import MRJob

class Vraag1(MRJob): 
    def mapper(self, _, line): 
        #lijn per lijn
        for word in line.split():
            # woord per woord
            yield ("lengte", len(word)) # emit hoe lang elk woord is

    def reducer(self, word, counts):
        aantal = 0
        totaal = 0
        for c in counts:
            aantal += 1
            totaal += c

        if aantal == 0:
            yield ('mean length', 0)
        else:
            yield ('mean length', totaal/aantal)
        

if __name__ == '__main__':
    Vraag1.run()
