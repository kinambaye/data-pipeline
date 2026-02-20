#projet pipeline ETL + data visualisation
# @author: Rokhaya MBAYE
#Importation de la librairie pandas
import pandas as pd;

#Lecture et stcokage du fichier csv dans une variable
data = pd.read_csv('/home/rokina/Documents/Projet Perso/Pipeline-donées/Dataset/swi.0000-0249/swi.0-249.csv', sep =";");

# Affcihages des premiers lignes pour s'assurer quele fichier est bien lu 
print(data.head());  

# Suppression des lignes vides
data = data.dropna();

# Suppressions les doublons
data = data.drop_duplicates();

# Affichage des colonnes du dataset
print(data.columns.tolist());

# Filtrage des données pour ne conserver que les enregistrements à partir de l'année 2000
data = data[data['DATE'] >= 200001]  

#Transformation de l'indice d'humidité de String vers flottant ^pour éviter des soucis lors de la création du tableau de bord
data['SWI_UNIF_MENS'] = data['SWI_UNIF_MENS'].str.replace(',', '.').astype(float);

#Création d'une nouvelle colonne 'annee' à partir de la colonne 'DATE' pour faciliter les analyses temporelles
data['annee'] = (data['DATE'] // 100).astype(int)

#Renomage des colonnes pour une meilleure compréhension car les noms d'origine ne sont pas très explicites
data.columns = ['numero', 'lambx', 'lamby', 'date', 'humidite', 'annee'];

# Affichage du dataframe après nettoyage
print(data.info());

# Affcihage des statistiques de base
print(data.describe());

# Affichage du nombre de valeurs manquantes par colonne
print(data.isnull().sum());

# Sauvegarde fu fichier version nettoyé qui sera utilisé pour la création du tableau de bord
#une légere modification j'ai enlevé le sep = ';' car looker studio ne les reconnais pas 
data.to_csv('meteo_propre.csv', index=False);

