# je begint altijd met een from (base image waar je van start)
# hier beginnen we van een python image, versie 3.8 (slim = zo weinig mogelijk package)
FROM python:3.8-slim 

# zet de working directory op de /app folder
# analoog aan het uitvoeren van cd /app in de container
WORKDIR /app

# kopieer de app.py naar de container image
COPY . /app

# maak poort 5000 en 8888 beschikbaar
EXPOSE 5000
EXPOSE 8888

# voer een willekeurig cli commando uit (in dit voorbeeld installeer numpy)
RUN pip install numpy

# voer het app.py scriptje uit
# let op de structuur - lijst van tokens die het argument bepalen
# python app.py -> ['python', 'app.py']
CMD ["python", "app.py"]