from mrjob.job import MRJob 
import csv # om csv-based text om te zetten
from io import StringIO

col_age = 5
col_survived = 1
col_gender = 4

class MR_structured(MRJob): 
    def mapper(self, _, line): 
        # col1, col2, col3, col4, ...
        
        csv_file = StringIO(line)
        cols = next(csv.reader(csv_file)) # zet de csv-text om naar een lijst met kolommen
        # op deze manier kunnen wordt het volgende ook correct omgezet test,"hallo world, met komma", test3
        # 1 zaak om mee op te letten -> elke lijn van de csv 1 entry bevat -> sommige datasets bevatten tekst met newline characters
            # -> die newline characters verwijder je best voor je het bestand oplaad naar het hdfs

        # gemiddelde leeftijd
        if cols[col_age] != '': # vermijd null-waarden problemen
            yield ('age', float(cols[col_age])) # zet de string ook om naar een getal

        if cols[col_survived] != '':
            yield('overleefd', int(cols[col_survived]))
            
        if cols[col_gender] != '':
            yield('geslacht', (cols[col_gender]))
            
        if cols[col_gender] != 'female' and cols[col_survived] != '':
            yield('vrouw_overleefd', int(cols[col_survived])) # wat we hier emitten is de volledige survived kolom voor vrouwen

    def reducer(self, key, counts):
        if key == 'age':
            counts = list(counts)
            aantal = len(counts)
            totaal = sum(counts) # als je het niat omzet naar een list gaat deze lijna een fout geven -> counts is een iterator -> kan niet herstarten

            yield ('mean age', totaal/aantal)

        elif key == 'overleefd' or key == 'vrouw_overleefd':
            counts = list(counts)
            aantal = len(counts)
            totaal = sum(counts)

            yield (key, totaal/aantal*100.0)

        elif key == 'geslacht':
            aantal = 0
            for gender in counts:
                if gender == 'male':
                    aantal_mannen += 1
                aantal += 1
            yield ('percentage mannen', aantal_mannen/aantal*100.0)

if __name__ == '__main__':
    MR_structured.run()
