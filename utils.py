#!/usr/bin/python2.7
#-*- coding: utf-8 -*-
import csv
import os
import urllib2
import zipfile
import json
import logging
from logging.handlers import RotatingFileHandler
import psycopg2

# Configuration du logger
import sys


def getLogger():
    # Créer le logger, le niveau de log et le format
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG) 
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # Définir une sortie (handler) console
    steam_handler = logging.StreamHandler()
    steam_handler.setLevel(logging.INFO)
    steam_handler.setFormatter(formatter)
    logger.addHandler(steam_handler)

    # Définir une sortie (handler) fichier
    file_handler = RotatingFileHandler('activity.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info('------------------------')
    logger.info('Initialisation du logger')
    logger.info('------------------------')
    
    return logger;

# Découvir les colonnes
# Prend un CSV en entrée et retourne les colonnes dispos dans un tableau
def discoverCols(fileName):
    csvFile = open(fileName, "rb")
    try :
        #dialect = csv.Sniffer().sniff(csvFile.read(1024))
        #csvFile.seek(0)
        reader = csv.DictReader(csvFile, delimiter=';')
        colNames = reader.fieldnames

    except ValueError:
        print "Erreur : " + sys.exc_info()[0]
    finally:
        csvFile.close()
    
    return colNames

def generatePgDDLDropTable (schema="public", tableName='', colNames=[]):
    
    sql = """
        DROP TABLE IF EXISTS {schema}.{tblName} CASCADE;
    """
    
    return sql.format(tblName=tableName, schema=schema)

def generatePgDDLCreateTable (schema="public", tableName='', colNames=[]):
    
    sql = """
         CREATE TABLE {schema}.{tblName} (
             {fields}
         ); 
    """
   
    fields = ""
    fields += " varchar(255),\n".join(str(col) for col in colNames)
    fields += " varchar(255)"        
    
    return sql.format(tblName=tableName, schema=schema, fields=fields)

def generatePgDDLcreateDatabase(dbName):
    """
        DROP DATABASE IF EXISTS {dbName};
        CREATE DATABASE {dbName}
    """

def clearDatasDir(datasDirectory):
    try:
        # os.remove(datasDirectory + "SUP_ANTENNE.txt")
        logger.info("- Suppression des fichiers du répertoire {dir}".format(dir=datasDirectory))
        filelist = [ f for f in os.listdir(datasDirectory) ]
        for f in filelist:
            os.remove(datasDirectory + f)
            
    except Exception:
        logger.info('Erreur effacement de répertoire ' + Exception)
        
def dlFile(fileUrl="", datasDir="."):
    logger.info("- Telechargement du fichier : " + fileUrl)
    file_name = fileUrl.split('/')[-1]
    f = urllib2.urlopen(fileUrl)
    with open(datasDir + file_name, "wb") as code:
        code.write(f.read())

    return file_name
        
def unzipFile(fileName="", datasDir="."):
    logger.info("- Dezip du fichier : " + fileName)
    filesCount = 0
    zf = zipfile.ZipFile(datasDir + fileName)
    for fname in zf.namelist():
        filesCount+=1
        outpath = datasDir
        zf.extract(fname, outpath)
    return filesCount

def getConf(jsonFile):
    with open(jsonFile) as json_data_file:
        data = json.load(json_data_file)
    return data

def cleanCsv(csvFile):
    logger.info ('Nettoyage du fichier -> {file}'.format(file=csvFile))
    
    try :
        #Fichier d'entrée
        fileSrc = open(csvFile, 'rb')
        fileSrcReader = csv.reader(fileSrc, delimiter=';')
       
        # En-tête
        row = fileSrcReader.next()
        # nombre de colonnes
        fileSrcColsCount = len(row)
        # nombre de lignes
        fileSrcRowCount = len(list(fileSrcReader))
        logger.info ('  Nombre de lignes -> {lines}'.format(lines=fileSrcRowCount))
        
        fileSrc.seek(0)
        
        # Fichier de sortie
        fileDest = open(csvFile + '.new', "wb")
        fileDestWriter = csv.writer(fileDest, delimiter=';', quoting=csv.QUOTE_NONE)
        
        iLines = 0
        iLinesRejected = 0
        for row in fileSrcReader:
            
            if len(row) == fileSrcColsCount:
                fileDestWriter.writerow(row)
                iLines+=1
            else:
                iLinesRejected += 1
                logger.debug("ligne rejetée")
                
        logger.info("  Total : lignes ok [{ok}], nok [{nok}]".format(ok=iLines, nok=iLinesRejected))
        fileSrc.close()
        fileDest.close()

        if iLinesRejected>0:
            #renomer 
            logger.debug("  Suppression du fichier " + csvFile)
            os.remove(csvFile)
            
            logger.debug("  Renomage de {src} en {dest}".format(src=(csvFile + '.new'), dest=csvFile))
            os.rename(csvFile + '.new', csvFile)
        else:
            # effacer csvFile + '.new'
            logger.debug("  Effacement du fichier vide")
            os.remove(csvFile + '.new')

    except ValueError, e:
        logger.error("  Exception dans la fonction " + __file__)
    finally:
        fileSrc.close()
        fileDest.close()

def copy2pg(datasDir, file, conf, conn, cur):
    try:
        cleanCsv(datasDir+file)
        
        # Récupérer les noms de colonne
        colNames = discoverCols(datasDir + file)
        
        # Déduire le nom de la table PG (virer extension + lowercase)
        tableName=os.path.splitext(file.lower())[0]
        
        # Générer les DDL de suppression et création
        sqlDropTable = generatePgDDLDropTable(tableName=tableName, colNames=colNames)
        sqlCreateTable = generatePgDDLCreateTable(tableName=tableName, colNames=colNames)
        
        logger.debug(" - Effacement de la table {tbl}".format(tbl=tableName))
        logger.debug(sqlDropTable.strip())
        cur.execute(sqlDropTable)
        conn.commit()
        
        logger.debug(" - Création de la table {tbl}".format(tbl=tableName))
        logger.debug(sqlCreateTable.strip())
        cur.execute(sqlCreateTable)
        conn.commit()
        
        logger.info(' - Copie dans postgreSQL (copy from)')
        f = open(datasDir+file, 'r')
        f.readline()
        logger.debug(' - ouverture du ficher')
        cur.execute("SET CLIENT_ENCODING TO 'LATIN1';")
        cur.copy_from(f, tableName, sep=';')
        logger.debug(' - copie')
        conn.commit()
        
    except:
        logger.error("Unexpected error: %s, dans la function %s" % (sys.exc_info()[0], __name__))
    finally:        
        f.close()
        
def execSql(conn, cur, sql):
    try:
        logger.debug(sql)
        cur.execute(sql);
        conn.commit()

    except psycopg2.DatabaseError, e:
        logger.error('Error: %s' % e)

def vacuum(conn):
    try:
        conn.set_isolation_level(0)
        cur = conn.cursor()
        query = "VACUUM ANALYSE"
        cur.execute(query)
    except psycopg2.DatabaseError, e:
        logger.error('Error: %s' % e)



logger = getLogger()