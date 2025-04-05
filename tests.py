from hdfs import InsecureClient

# Connexion au Namenode (modifie l'IP et le port si nécessaire)
client = InsecureClient('http://hadoop-namenode:9870', user='root')

# Lister les fichiers dans un dossier HDFS
print(client.list('/data'))

# Supprimer le fichier
try:
    client.delete("/data/pollution.json")
    print(f"Fichier supprimé avec succès.")
except Exception as e:
    print(f"Erreur lors de la suppression du fichier : {e}")

try:
    hdfs_path = '/data/pollution.json'  # Chemin HDFS de destination
    local_path = 'pollution.json'  # Chemin local du fichier
    client.upload(hdfs_path, local_path)
    print(f"Fichier {local_path} uploadé avec succès.")
except Exception as e:
    print(f"Erreur lors de l'upload du fichier : {e}")

# # Lire un fichier depuis HDFS
# try:
#     with client.read('/data/pollution.json', encoding='utf-8') as reader:
#         df = reader.read()
#         print(df)
# except Exception as e:
#     print(f"Erreur : {e}")
