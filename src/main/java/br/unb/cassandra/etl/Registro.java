package br.unb.cassandra.etl;


public class Registro {

    private final String uf;
    private final String codigoMunicipio;
    private final String nomeMunicipio;
    private final String nomeBeneficiario;
    private final String valorPago;
    private final String mesAno;

    public Registro(String uf,
                    String codigoMunicipio,
                    String nomeMunicipio,
                    String nomeBeneficiario,
                    String valorPago,
                    String mesAno) {
        this.uf = uf;
        this.codigoMunicipio = codigoMunicipio;
        this.nomeMunicipio = nomeMunicipio;
        this.nomeBeneficiario = nomeBeneficiario;
        this.valorPago = valorPago;
        this.mesAno = mesAno;
    }

    public String getUf() {
        return uf;
    }

    public String getCodigoMunicipio() {
        return codigoMunicipio;
    }

    public String getNomeMunicipio() {
        return nomeMunicipio;
    }

    public String getNomeBeneficiario() {
        return nomeBeneficiario;
    }

    public String getValorPago() {
        return valorPago;
    }
    public String getMesAno() {
        return mesAno;
    }

    @Override
    public String toString() {
        return String.join(",", uf, codigoMunicipio, nomeMunicipio, nomeBeneficiario, valorPago, mesAno);
    }
}
