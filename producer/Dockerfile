FROM python:3.13.2

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les fichiers nécessaires
COPY producer.py /app/
COPY requirements.txt /app/
COPY my_functions.py /app/

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Définir la commande de lancement
CMD ["python", "producer.py"]