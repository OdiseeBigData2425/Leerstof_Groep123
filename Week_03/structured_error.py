
from mrjob.job import MRJob
import csv
from io import StringIO

col_survived = 1
col_sex = 4
col_age = 5

class MR_Structured(MRJob):
    
    def mapper(self, _, line):
        if line == "PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked":
            return
        
        # line of text to list of columns
        csv_file = StringIO(line)
        cols = next(csv.reader(csv_file))    

        yield ("age", float(cols[col_age]))
        yield ("overleefd", int(cols[col_survived]))
        yield ("geslacht" , cols[col_sex])
        yield ("vrouw_overleefd", int(cols[col_survived]))

    def reducer(self, word, counts):

        if word == "age":
            counts = list(counts)
            total = sum(counts)
            size=len(counts)
            yield (word, total/size)
        elif word == "overleefd" or word == "vrouw_overleefd":
            counts = list(counts)
            total = sum(counts)
            size=len(counts)
            yield (word, total/size*100.0)
        elif word == "geslacht": 
            aantal_mannen = 0
            aantal_vrouwen = 0
            for gender in counts:
                if gender == "male":
                    aantal_mannen += 1
                else:
                    aantal_vrouwen += 1
                    
            yield ("percentage mannen", aantal_mannen/(aantal_mannen + aantal_vrouwen) * 100.0)
        else:
            yield("dummy", next(counts))

if __name__ == '__main__':
    MR_Structured.run()
