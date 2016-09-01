package br.unb.cassandra.etl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;

public class CassandraClientLoader {

    private Cluster cluster;
    private Session session;
    private String host = "127.0.0.1";
    private String keyspace = "bolsafamilia";
    private String insertCmd = "INSERT INTO BF (ID, UF, CODIGO_MUNICIPIO, NOME_MUNICIPIO, NOME_BENEFICIARIO, VALOR_PAGO, MES_ANO) " +
                               " VALUES (?, ?, ?, ?, ?, ?, ?)";

    public boolean connect() {
        cluster = Cluster.builder().addContactPoint(host).build();
        session = cluster.connect(keyspace);
        return !session.isClosed();
    }

    public void close() {
        if (session != null)
            session.close();
        if (cluster != null) cluster.close();
    }

    // TODO: use BATCH insert to speedup?
    public void insert(Registro registro) {

       PreparedStatement pstmt = session.prepare(insertCmd);

       BoundStatement bstmt = pstmt.bind(UUIDs.timeBased(),
                                         registro.getUf(),
                                         registro.getCodigoMunicipio(),
                                         registro.getNomeMunicipio(),
                                         registro.getNomeBeneficiario(),
                                         Float.parseFloat(registro.getValorPago()),
                                         registro.getMesAno());
       session.execute(bstmt);
    }
}
