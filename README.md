# anfr-integration-python-pg
Intégration des données ANFR (installations radioélectriques) avec python / postgreSQL

## Description

Ce script automatise la création d'une table postgreSQL / postGIS qui permet l'affichage des positions des supports d'antennes, jeu de données téléchargeable ici : https://www.data.gouv.fr/fr/datasets/donnees-sur-les-installations-radioelectriques-de-plus-de-5-watts-1/

Les étapes : 

* récupération des fichiers zippés et dézip
* intégration des CSV dans postgreSQL
* création de la table finale

## Pré-requis

* Installer postgreSQL et postGIS
* Installer python, pip et virtualenv
* Créer une BDD
* Créer une extension postGIS
* Modifier le fichier conf/conf.json (infos BDD)
* Cloner le projet

```shell
git clone https://github.com/eric-pommereau/anfr-integration-python-pg
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

Lancer le script 

```shell
python etl_anfr.py --download
```


