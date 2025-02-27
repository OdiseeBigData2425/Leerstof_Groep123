from mrjob.job import MRJob # importeer de mr job package

class MRWordCount(MRJob): # maak een mapreduce applicatie aan
    #words = {}
    def mapper(self, _, line): 
        # hier schrijf je de code voor te mappen -> dit wordt lijn per lijn opgeroepen
        # dit kan op verschillende nodes uitgevoerd worden voor verschillende blokken -> dus je gaat niet zomaar alles kunnen optellen hier
        for word in line.split():
            # onderstaande kan je niet doen
            #words[word] += 1
            yield (word, 1) # key-value paar om te emitten
            # yield is een return die de code niet stopt

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    MRWordCount.run()
