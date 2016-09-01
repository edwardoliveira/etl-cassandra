package br.unb.cassandra.etl;

public class CassandraBulkLoader {

    public static final String SCHEMA = "CREATE TABLE %s.%s (ID TIMEUUID,\n" +
                                        "                    UF TEXT,\n" +
                                        "                    CODIGO_MUNICIPIO TEXT,\n" +
                                        "                    NOME_MUNICIPIO TEXT,\n" +
                                        "                    NOME_BENEFICIARIO TEXT,\n" +
                                        "                    VALOR_PAGO FLOAT,\n" +
                                        "                    MES_ANO TEXT,\n" +
                                        "                    PRIMARY KEY (ID, VALOR_PAGO))";

    public static final String INSERT = "INSERT INTO %s.%s (ID, UF, CODIGO_MUNICIPIO, NOME_MUNICIPIO, NOME_BENEFICIARIO, VALOR_PAGO, MES_ANO) " +
            " VALUES (?, ?, ?, ?, ?, ?, ?)";


    public static void main(String[] args) {

    }


}
