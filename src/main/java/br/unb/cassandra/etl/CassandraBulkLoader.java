package br.unb.cassandra.etl;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.File;
import java.io.IOException;

public class CassandraBulkLoader {

    public static final String SCHEMA = "CREATE TABLE %s.%s (ID TIMEUUID,\n" +
                                        "                    UF TEXT,\n" +
                                        "                    CODIGO_MUNICIPIO TEXT,\n" +
                                        "                    NOME_MUNICIPIO TEXT,\n" +
                                        "                    NOME_BENEFICIARIO TEXT,\n" +
                                        "                    VALOR_PAGO FLOAT,\n" +
                                        "                    MES_ANO TEXT,\n" +
                                        "                    PRIMARY KEY (ID, VALOR_PAGO))";

    public static final String INSERT_STMT = "INSERT INTO %s.%s (ID, UF, CODIGO_MUNICIPIO, NOME_MUNICIPIO," +
                                             " NOME_BENEFICIARIO, VALOR_PAGO, MES_ANO) " +
                                             " VALUES (?, ?, ?, ?, ?, ?, ?)";

    public static final String KEYSPACE = "bolsafamilia";
    public static final String TABLE = "bf";

    public static final String DEFAULT_OUTPUT_DIR = "./bulk";

    private CQLSSTableWriter writer;


    public boolean connect() {

        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
        if (!outputDir.exists() && !outputDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
                // set target schema
                .forTable(String.format(SCHEMA, KEYSPACE, TABLE))
                        // set CQL statement to put data
                .using(String.format(INSERT_STMT, KEYSPACE, TABLE))
                        // set partitioner if needed
                        // default is Murmur3Partitioner so set if you use different one.
                .withPartitioner(new Murmur3Partitioner());
        writer = builder.build();
        return true;
    }

    public void insert(Registro registro) {
        try {
            writer.addRow(UUIDs.timeBased(),
                    registro.getUf(),
                    registro.getCodigoMunicipio(),
                    registro.getNomeMunicipio(),
                    registro.getNomeBeneficiario(),
                    Float.parseFloat(registro.getValorPago().replace(",", "")),
                    registro.getMesAno());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
