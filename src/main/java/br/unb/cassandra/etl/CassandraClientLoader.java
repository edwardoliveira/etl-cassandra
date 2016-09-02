package br.unb.cassandra.etl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;

public class CassandraClientLoader {

    private Cluster cluster;
    private Session session;
    private String host = "127.0.0.1";
    public static final String KEYSPACE = "bolsafamilia";
    public static final String TABLE = "bf";
    public static final String INSERT = "INSERT INTO %s.%s (ID, UF, CODIGO_MUNICIPIO, NOME_MUNICIPIO, NOME_BENEFICIARIO, VALOR_PAGO, MES_ANO) " +
                                        " VALUES (?, ?, ?, ?, ?, ?, ?)";

    public boolean connect() {
        cluster = Cluster.builder().addContactPoint(host).build();
        session = cluster.connect(KEYSPACE);
        return !session.isClosed();
    }

    public void close() {
        if (session != null)
            session.close();
        if (cluster != null) cluster.close();
    }

    // TODO: use BATCH insert to speedup?
    public void insert(Registro registro) {

       PreparedStatement pstmt = session.prepare(String.format(INSERT, KEYSPACE, TABLE));

       BoundStatement bstmt = pstmt.bind(UUIDs.timeBased(),
                                         registro.getUf(),
                                         registro.getCodigoMunicipio(),
                                         registro.getNomeMunicipio(),
                                         registro.getNomeBeneficiario(),
                                         Float.parseFloat(registro.getValorPago().replace(",", "")),
                                         registro.getMesAno());
       session.execute(bstmt);
    }
}
