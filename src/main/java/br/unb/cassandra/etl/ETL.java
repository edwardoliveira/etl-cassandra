package br.unb.cassandra.etl;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public class ETL {


    public ETL() {
        String filename = "/home/eribeiro/Downloads/201606_BolsaFamiliaFolhaPagamento.csv";
        doLoad(filename);
    }

    private void doLoad(String filename) {

        CassandraClientLoader cassandraLoader = new CassandraClientLoader();
        cassandraLoader.connect();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), Charset.forName("ISO-8859-1")))) {

            reader.readLine(); // IGNORA O HEADER
            String line;
            long ini = System.currentTimeMillis();
            while ((line = reader.readLine()) != null) {
                List<String> colunas = Arrays.asList(line.split("\t"));

                Registro registro = new Registro(colunas.get(0),
                                                 colunas.get(1),
                                                 colunas.get(2),
                                                 colunas.get(8),
                                                 colunas.get(10),
                                                 colunas.get(11)
                                                 );

                cassandraLoader.insert(registro);
            }

            long end = System.currentTimeMillis() - ini;
            System.out.println("Tempo de carga: " + (end / (60 * 1000)) + " min");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            cassandraLoader.close();
        }
    }

    public static void main(String[] args) {
        new ETL();
    }
}
