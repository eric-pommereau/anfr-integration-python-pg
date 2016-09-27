#!/usr/bin/python2.7
#-*- coding: utf-8 -*-

import argparse
from utils import *
from sql import *

parser = argparse.ArgumentParser(prog='anfr_process', usage='%(prog)s [options]')
parser.add_argument("--download", "-d", nargs='?', help="download remote files", default=False)

args = parser.parse_args()

# Lecture du fichier de configuration
conf = getConf('./conf/conf.json')

datasDir = conf['files']['directory']
fileUrlDatas = conf['dl']['datas']
fileUrlRef = conf['dl']['ref']

# télécharger le fichier zippé
if (args.download == True):
    dlFile(fileUrl=fileUrlDatas, datasDir=datasDir)
    dlFile(fileUrl=fileUrlRef, datasDir=datasDir)

fileZipDatas = fileUrlDatas.split('/')[-1]
fileZipRef = fileUrlRef.split('/')[-1]

#dézipper les fichiers
filesCount = unzipFile(fileName=fileZipDatas, datasDir=datasDir)
logger.info("Nombre de fichiers extraits : {ctr}" . format(ctr=str(filesCount)))

filesCount = unzipFile(fileName=fileZipRef, datasDir=datasDir)
logger.info("Nombre de fichiers extraits : {ctr}" . format(ctr=str(filesCount)))

try:
    conn = psycopg2.connect("dbname={db}".format(db=conf['postgres']['db']))
    cur = conn.cursor()

    logger.info('Traitement des CSV tables de ref.')
    
    # Traitement des fichiers de référence 
    for file in conf['files']['ref']:
        logger.info(' * Fichier {file}'.format(file=file))
        copy2pg(datasDir=datasDir, file=file, conf=conf, conn=conn, cur=cur)

    logger.info('Traitement des CSV tables de datas')
    # Traitement des fichiers de données
    for file in conf['files']['datas']:
        logger.info(' * Fichier {file}'.format(file=file))
        copy2pg(datasDir=datasDir, file=file, conf=conf, conn=conn, cur=cur)

    logger.info('Traitement des stations')
    execSql(conn=conn, cur=cur, sql=sql_sup_station())

    logger.info('Traitement des antennes')
    execSql(conn=conn, cur=cur, sql=sql_sup_antenne())

    logger.info('Traitement des emetteurs')
    execSql(conn=conn, cur=cur, sql=sql_sup_emetteur())

    logger.info('Traitement des exploitants')
    execSql(conn=conn, cur=cur, sql=sql_sup_exploitant())

    logger.info('Traitement des natures')
    execSql(conn=conn, cur=cur, sql=sql_sup_nature())

    logger.info('Traitement des propriétaires')
    execSql(conn=conn, cur=cur, sql=sql_sup_proprietaire())

    logger.info('Traitement des supports')
    execSql(conn=conn, cur=cur, sql=sql_sup_support())

    logger.info('Traitement des types antenne')
    execSql(conn=conn, cur=cur, sql=sql_sup_type_antenne())

    logger.info('Création de la table finale')
    conn = psycopg2.connect("dbname={db}".format(db=conf['postgres']['db']))
    cur = conn.cursor()
    execSql(conn=conn, cur=cur, sql=sql_create_final_table().format(tbl_name='supports_anfr_092016'))

    logger.info(' - VACUUM')
    vacuum(conn=conn)

except:
    #logger.error("Unexpected error:", sys.exc_info()[0])
    logger.error("Unexpected error...[%s]" % (sys.exc_info()[0]))
finally:
    logger.info('------------ Fin du programme')
    cur.close()
    conn.close()

exit()