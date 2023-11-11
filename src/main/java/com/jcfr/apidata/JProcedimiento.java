package com.jcfr.apidata;

import java.io.Serializable;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import com.jcfr.apidata.interfaces.IProcedimiento;
import com.jcfr.apidata.utiles.FillerDT;
import com.jcfr.apidata.utiles.InOutParamEnum;
import com.jcfr.utiles.DateTime;
import com.jcfr.utiles.JConvertidor;
import com.jcfr.utiles.order.OC;
import com.jcfr.utiles.repositorios.DataSet;
import com.jcfr.utiles.repositorios.DataTable;

class JProcedimiento implements IProcedimiento {

    private Connection cnx;
    private CallableStatement cmd;
    private ArrayList<Parametro> params;
    private String nombrePRO;

    JProcedimiento(Connection cnx) { // constructor con acceso a paquete
        this.cnx = cnx;
        params = new ArrayList<Parametro>(8);
    }

    private String contruirSentenciaPRO(int numParametros, String nombrePRO) {
        if (numParametros <= 0) return "{call " + nombrePRO + "()}";

        StringBuilder s = new StringBuilder(2 * numParametros + nombrePRO.length() + 8);
        s.append("{call ").append(nombrePRO).append("(");
        for (int i = 0; i < numParametros - 1; i++) {
            s.append("?,");
        }
        s.append("?)}");

        return s.toString();
    }

    private void setearParametros() throws SQLException {
        // si tiene cero parametros no hay problema
        for (Parametro p : params) {
            if (p.escala != 0) { // si tiene escala, registrarlo con escala
                if (p.esDeEntrada()) {
                    cmd.setObject(p.nombreParametro, p.valor, p.tipoJava, p.escala);
                } else if (p.esDeSalida()) {
                    cmd.registerOutParameter(p.nombreParametro, p.tipoJava, p.escala);
                } else { // entrada y salida
                    cmd.setObject(p.nombreParametro, p.valor, p.tipoJava, p.escala);
                    cmd.registerOutParameter(p.nombreParametro, p.tipoJava, p.escala);
                }
            } else { // registrarlo sin escala
                if (p.esDeEntrada()) {
                    cmd.setObject(p.nombreParametro, p.valor, p.tipoJava);
                } else if (p.esDeSalida()) {
                    cmd.registerOutParameter(p.nombreParametro, p.tipoJava);
                } else { // entrada y salida
                    cmd.setObject(p.nombreParametro, p.valor, p.tipoJava);
                    cmd.registerOutParameter(p.nombreParametro, p.tipoJava);
                }
            }

        }

    }

    private boolean esPosibleCerrar() {
        for (Parametro p : params) {
            if (p.esDeSalida() || p.esDeEntradaSalida()) return false;
        }
        return true;
    }

    @Override
    public void crear(String nombrePRO) {
        this.nombrePRO = nombrePRO;
        params.clear();
    }

    @Override
    public void addParam(String nombre, Object valor, InOutParamEnum tipoDIR, int tipoJSQL) {
        params.add(new Parametro(nombre.trim(), valor, tipoDIR, tipoJSQL));
    }

    @Override
    public void addParam(String nombre, Object valor, InOutParamEnum tipoDIR, int tipoJSQL, int escala) {
        params.add(new Parametro(nombre.trim(), valor, tipoDIR, tipoJSQL, escala));
    }

    @Override
    public void changeValorParam(String nombre, Object nuevoValor) {
        // el parametros solo puede ser de entrada o de entrada/salida
        for (Parametro p : params) {
            if (p.nombreParametro.equals(nombre.trim())) {
                p.valor = nuevoValor;
                break;
            }
        }
    }

    @Override
    public void exe() throws SQLException {
        // recien al ejecutar se crea la consulta segun el numero de parametros
        String sentenciaPRO = contruirSentenciaPRO(params.size(), nombrePRO.trim());
        cmd = cnx.prepareCall(sentenciaPRO);
        setearParametros(); // setear parametros en la sentencia recien inicializada
        cmd.executeUpdate();
        if (esPosibleCerrar()) cerrar();

    }

    @Override
    public void exe(DataTable dt) throws SQLException {
        // recien al ejecutar se crea la consulta segun el numero de parametros
        String sentenciaPRO = contruirSentenciaPRO(params.size(), nombrePRO.trim());
        cmd = cnx.prepareCall(sentenciaPRO);
        setearParametros(); // setear parametros en la sentencia recien inicializada
        ResultSet rs = cmd.executeQuery();
        FillerDT.fill(rs, dt);
        rs.close();
        if (esPosibleCerrar()) cerrar();
    }

    @Override
    public void exe(DataSet ds) throws SQLException {
        // recien al ejecutar se crea la consulta segun el numero de parametros
        String sentenciaPRO = contruirSentenciaPRO(params.size(), nombrePRO.trim());
        cmd = cnx.prepareCall(sentenciaPRO);
        setearParametros(); // setear parametros en la sentencia recien inicializada
        FillerDT.fillDataSet(cmd, ds); // internamente cierra los datasets
        if (esPosibleCerrar()) cerrar();

    }

    @Override
    public DataSet exe(String[] nombreTablasSalida) throws SQLException {
        DataSet ds = new DataSet();
        exe(ds);
        int min = Math.min(ds.getTablas().size(), nombreTablasSalida.length);
        for (int i = 0; i < min; i++) {
            ds.getTablaAt(i).setNombreTabla(nombreTablasSalida[i]);
        }
        return ds;
    }

    @Override
    public Object getParamObject(String nombreParametro) throws SQLException {
        return cmd.getObject(nombreParametro.trim());
    }

    @Override
    public float getParamFloat(String nombreParametro) throws SQLException {
        return cmd.getFloat(nombreParametro.trim());
    }

    @Override
    public double getParamDouble(String nombreParametro) throws SQLException {
        return cmd.getDouble(nombreParametro.trim());
    }

    @Override
    public short getParamShort(String nombreParametro) throws SQLException {
        return cmd.getShort(nombreParametro.trim());
    }

    @Override
    public int getParamInt(String nombreParametro) throws SQLException {
        return cmd.getInt(nombreParametro.trim());
    }

    @Override
    public long getParamLong(String nombreParametro) throws SQLException {
        return cmd.getLong(nombreParametro.trim());
    }

    @Override
    public char getParamChar(String nombreParametro) throws SQLException {
        return cmd.getString(nombreParametro.trim()).charAt(0);
    }

    @Override
    public DateTime getParamDateTime(String nombreParametro) throws SQLException {
        return JConvertidor.JConvertidor.toDateTime(cmd.getTimestamp(nombreParametro.trim()));
    }

    @Override
    public Blob getParamBlob(String nombreParametro) throws SQLException {
        return cmd.getBlob(nombreParametro.trim());
    }

    @Override
    public String getParamString(String nombreParametro) throws SQLException {
        return cmd.getString(nombreParametro.trim());
    }

    @Override
    public Date getParamDate(String nombreParametro) throws SQLException {
        return cmd.getDate(nombreParametro.trim());
    }

    @Override
    public void cerrar() throws SQLException {
        if (cmd != null && !cmd.isClosed()) {
            cmd.close();
        }
    }

    private static class Parametro implements Serializable {

        private static final long serialVersionUID = 2386572394751752746L;

        String nombreParametro;
        Object valor;
        int tipoJava;
        int escala;
        private InOutParamEnum tipoDIR;

        Parametro(String nombreParametro, Object valor, InOutParamEnum tipoDIR, int tipoJSQL) {
            this(nombreParametro, valor, tipoDIR, tipoJSQL, 0);
        }

        Parametro(String nombreParametro, Object valor, InOutParamEnum tipoDIR, int tipoJSQL, int escala) {
            this.nombreParametro = nombreParametro;
            this.valor = valor;
            this.tipoJava = tipoJSQL;
            this.escala = escala;
            this.tipoDIR = tipoDIR;
        }

        boolean esDeEntrada() {
            return tipoDIR == InOutParamEnum.ENTRADA;
        }

        boolean esDeSalida() {
            return tipoDIR == InOutParamEnum.SALIDA;
        }

        boolean esDeEntradaSalida() {
            return tipoDIR == InOutParamEnum.ENTRADA_SALIDA;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 97 * hash + (this.nombreParametro != null ? this.nombreParametro.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj instanceof Parametro) {
                Parametro objCast = (Parametro) obj;
                return OC.compare(nombreParametro, objCast.nombreParametro) == 0;
            }
            return false;
        }
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
