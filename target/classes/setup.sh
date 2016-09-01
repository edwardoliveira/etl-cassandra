#!/usr/bin/env bash

bin/cqlsh --keyspace bolsafamilia --request-timeout=120


create keyspace bolsafamilia WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1} ;

use bolsafamilia ;

DROP TABLE BF;

CREATE TABLE BF (ID TIMEUUID,
                 UF TEXT,
                 CODIGO_MUNICIPIO TEXT,
                 NOME_MUNICIPIO TEXT,
                 NOME_BENEFICIARIO TEXT,
                 VALOR_PAGO FLOAT,
                 MES_ANO TEXT,
                 PRIMARY KEY (ID, VALOR_PAGO));

INSERT INTO BF (ID,
                UF,
                CODIGO_MUNICIPIO,
                NOME_MUNICIPIO,
                NOME_BENEFICIARIO,
                VALOR_PAGO,
                MES_ANO)
VALUES (?, ?, ?, ?, ?, ?, ?)

truncate bf;

bin/nodetool cfstats bolsafamilia