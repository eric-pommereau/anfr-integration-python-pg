#!/usr/bin/python2.7
#-*- coding: utf-8 -*-

def sql_sup_station():
    sql = """
        -- modifier la valeur en entier -> regexp_matches retourne un text[], d'où le unnest
        UPDATE sup_station SET adm_id = unnest(regexp_matches(adm_id, '(\d{1,10}),'));
        -- mofifier le type de la colonne
        ALTER TABLE sup_station ALTER COLUMN adm_id TYPE integer USING adm_id::integer;
    """
    return sql
def sql_sup_antenne():
    sql = """
        UPDATE sup_antenne SET aer_nb_alt_bas = 0 where  aer_nb_alt_bas = '';
        UPDATE sup_antenne SET aer_nb_alt_bas = unnest(regexp_matches(aer_nb_alt_bas, '(\d{1,10}),'));
        ALTER TABLE sup_antenne ALTER COLUMN aer_nb_alt_bas TYPE integer USING aer_nb_alt_bas::integer;
        ALTER TABLE sup_antenne ALTER COLUMN sta_nm_anfr TYPE varchar(20) USING sta_nm_anfr::varchar(20);
        ALTER TABLE sup_antenne ALTER COLUMN aer_id TYPE bigint USING aer_id::bigint;
        ALTER TABLE sup_antenne ALTER COLUMN tae_id TYPE bigint USING tae_id::bigint;
        ALTER TABLE sup_antenne ADD CONSTRAINT pk_sup_antenne PRIMARY KEY (aer_id);
    """
    return sql

def sql_sup_emetteur():
    sql = """
        ALTER TABLE sup_emetteur ALTER COLUMN sta_nm_anfr TYPE varchar(20) USING sta_nm_anfr::varchar(20);
        ALTER TABLE sup_emetteur ALTER COLUMN emr_id TYPE bigint USING emr_id::bigint;
        ALTER TABLE sup_emetteur ALTER COLUMN aer_id TYPE bigint USING aer_id::bigint;
        ALTER TABLE sup_emetteur ALTER COLUMN emr_lb_systeme TYPE varchar(30) USING emr_lb_systeme::varchar(30);
        ALTER TABLE sup_emetteur ADD CONSTRAINT pk_sup_emetteur PRIMARY KEY (emr_id);
    """
    return sql

def sql_sup_exploitant():
    sql = """
        ALTER TABLE sup_exploitant ALTER COLUMN adm_id TYPE bigint USING adm_id::bigint;
        ALTER TABLE sup_exploitant ALTER COLUMN adm_lb_nom TYPE varchar(100) USING adm_lb_nom::varchar(100);
        ALTER TABLE sup_exploitant ADD CONSTRAINT pk_sup_exploitant PRIMARY KEY (adm_id);
    """
    return sql

def sql_sup_nature():
    sql = """
        ALTER TABLE sup_nature ALTER COLUMN nat_id TYPE bigint USING nat_id::bigint;
        ALTER TABLE sup_nature ADD CONSTRAINT pk_sup_nature PRIMARY KEY (nat_id);
    """
    return sql

def sql_sup_proprietaire():
    sql = """
        ALTER TABLE sup_proprietaire ALTER COLUMN tpo_id TYPE bigint USING tpo_id::bigint;
    """
    return sql

def sql_sup_support():
    sql = """
        ALTER TABLE sup_support ALTER COLUMN sup_id TYPE bigint USING sup_id::bigint;
        ALTER TABLE sup_support ALTER COLUMN nat_id TYPE bigint USING nat_id::bigint;
        UPDATE sup_support SET tpo_id = 0 where  tpo_id = '';
        ALTER TABLE sup_support ALTER COLUMN tpo_id TYPE bigint USING tpo_id::bigint;

        UPDATE sup_support SET sup_nm_haut = 0 where  sup_nm_haut = '';
        ALTER TABLE sup_support ALTER COLUMN sup_nm_haut TYPE real USING cast(replace(sup_nm_haut, ',', '.') as real);

        UPDATE sup_support SET cor_nb_dg_lat = 0 where  cor_nb_dg_lat = '';
        ALTER TABLE sup_support ALTER COLUMN cor_nb_dg_lat TYPE bigint USING cor_nb_dg_lat::bigint;
        UPDATE sup_support SET cor_nb_mn_lat = 0 where cor_nb_mn_lat = '';
        ALTER TABLE sup_support ALTER COLUMN cor_nb_mn_lat TYPE bigint USING cor_nb_mn_lat::bigint;
        UPDATE sup_support SET cor_nb_sc_lat = 0 where cor_nb_sc_lat = '';
        ALTER TABLE sup_support ALTER COLUMN cor_nb_sc_lat TYPE bigint USING cor_nb_sc_lat::bigint;
        -- UPDATE sup_support SET cor_cd_ns_lat = 0 where cor_cd_ns_lat = '';
        ALTER TABLE sup_support ALTER COLUMN cor_cd_ns_lat TYPE varchar(1) USING cor_cd_ns_lat::varchar(1);

        -- select cor_cd_ns_lat, count(*) as ctr from sup_support group by cor_cd_ns_lat order by ctr DESC;

        UPDATE sup_support SET cor_nb_dg_lon = 0 where  cor_nb_dg_lon = '';
        ALTER TABLE sup_support ALTER COLUMN cor_nb_dg_lon TYPE bigint USING cor_nb_dg_lon::bigint;
        UPDATE sup_support SET cor_nb_mn_lon = 0 where cor_nb_mn_lon = '';
        ALTER TABLE sup_support ALTER COLUMN cor_nb_mn_lon TYPE bigint USING cor_nb_mn_lon::bigint;
        UPDATE sup_support SET cor_nb_sc_lon = 0 where cor_nb_sc_lon = '';
        ALTER TABLE sup_support ALTER COLUMN cor_nb_sc_lon TYPE bigint USING cor_nb_sc_lon::bigint;
        -- UPDATE sup_support SET cor_cd_ew_lon = 0 where cor_cd_ew_lon = '';
        ALTER TABLE sup_support ALTER COLUMN cor_cd_ew_lon TYPE varchar(1) USING cor_cd_ew_lon::varchar(1);

        ALTER TABLE sup_support ALTER COLUMN nat_id TYPE bigint USING nat_id::bigint;

    """
    return sql

def sql_sup_type_antenne():
    sql = """
        ALTER TABLE sup_type_antenne ALTER COLUMN tae_id TYPE bigint USING tae_id::bigint;
    """
    return sql

def sql_create_final_table():
    sql= """
    set work_mem='1500MB';


    DROP TABLE IF EXISTS {tbl_name} CASCADE;

    CREATE TABLE {tbl_name} (
        sup_id serial,
        nature varchar(100),
        proprietaire varchar(30),
        nb_emetteurs int,
        hauteur_support real,
        adresse varchar(255),
        cd_postal varchar(10),
        insee varchar(10),
        lieu varchar(100),
        exploitants text,
        sys_emeteurs text,
        geom geometry(POINT, 4326)
    );

    WITH t as (
       SELECT
            nat.nat_lb_nom as nature,
            prop.tpo_lb as proprietaire,
            count(sta.sta_nm_anfr) as nb_stations,
            count(eme.emr_id) as nb_emetteurs,
            sup.SUP_NM_HAUT as hauteur_support,
            sup.adr_lb_add1 || ' ' || sup.adr_lb_add2 || ' ' || sup.adr_lb_add3 as adresse,
            sup.adr_nm_cp as cd_postal,
            sup.adr_lb_lieu as lieu,
            com_cd_insee as insee,
            json_agg(DISTINCT exp.adm_lb_nom) as exploitants,
            json_agg(DISTINCT eme.emr_lb_systeme) as sys_emeteurs,
            ST_GeomFromText('POINT(' ||
                round(dms2dd(cor_nb_dg_lon || '° '|| cor_nb_mn_lon || '''' || cor_nb_sc_lon || '"' || cor_cd_ew_lon), 6) || ' ' ||
                round(dms2dd(cor_nb_dg_lat || '° '|| cor_nb_mn_lat || '''' || cor_nb_sc_lat || '"' || cor_cd_ns_lat), 6) ||
            ')', 4326) as geom
        FROM sup_support sup
            INNER JOIN sup_station sta ON (sup.sta_nm_anfr = sta.sta_nm_anfr)
            INNER JOIN sup_proprietaire prop ON (prop.tpo_id = sup.tpo_id)
            INNER JOIN sup_exploitant exp ON (exp.adm_id = sta.adm_id)
            INNER JOIN sup_nature nat ON (nat.nat_id = sup.nat_id)
            INNER JOIN sup_emetteur eme ON (eme.sta_nm_anfr = sta.sta_nm_anfr)
            -- INNER JOIN sup_type_antenne tae ON (ant.tae_id = tae.tae_id)
        group by nature, proprietaire, hauteur_support, adresse, cd_postal, lieu, insee, geom
    )

    INSERT INTO {tbl_name} (nature, proprietaire, hauteur_support, nb_emetteurs, exploitants, sys_emeteurs, adresse, cd_postal, lieu, insee, geom)
    select nature, proprietaire, hauteur_support, nb_emetteurs, exploitants, sys_emeteurs, adresse, cd_postal, lieu, insee, geom from t;

    ALTER TABLE {tbl_name} ADD CONSTRAINT pk_supports_anfr PRIMARY KEY(sup_id);

    CREATE INDEX idx_geom ON {tbl_name} USING gist (geom);

    """

    return sql