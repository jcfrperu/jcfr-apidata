package com.jcfr.apidata;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;

import com.jcfr.apidata.interfaces.IConsultaPreparada;
import com.jcfr.apidata.utiles.FillerDT;
import com.jcfr.apidata.utiles.MsgConstantes;
import com.jcfr.utiles.DateTime;
import com.jcfr.utiles.repositorios.DataSet;
import com.jcfr.utiles.repositorios.DataTable;

class JConsultaPreparada implements IConsultaPreparada {

    private final static int TIPO_SQL_NO_SETEADO = -45966695; // esperemos que nunca pongan en el java.sql.Types este valor :D
    private final static int ESCALA_SQL_NO_SETEADA = 0;
    private Connection cnx;
    private PreparedStatement sentencia;
    private HashMap<Integer, Parametro> params;

    JConsultaPreparada(Connection conexion) {
        this.cnx = conexion;
        params = new HashMap<Integer, Parametro>(6);
    }

    @Override
    public void crear(String sentenciaPSQL) throws SQLException {
        if (sentenciaPSQL == null) throw new SQLException(MsgConstantes.MSG_PCONSULTA_001);
        sentencia = cnx.prepareStatement(sentenciaPSQL);
        if (params.size() > 0) params.clear();
    }

    @Override
    public void addParam(int posIndex, Object valor, int typeSQL) throws SQLException {
        if (sentencia == null) throw new SQLException(MsgConstantes.MSG_PCONSULTA_002);
        params.put(posIndex, new Parametro(posIndex, valor, typeSQL));
        sentencia.setObject(posIndex, valor, typeSQL);
    }

    @Override
    public void addParam(int posIndex, Object valor, int typeSQL, int escala) throws SQLException {
        if (sentencia == null) throw new SQLException(MsgConstantes.MSG_PCONSULTA_002);
        params.put(posIndex, new Parametro(posIndex, valor, typeSQL, escala));
        sentencia.setObject(posIndex, valor, typeSQL, escala);
    }

    @Override
    public void addParam(int posIndex, Object valor) throws SQLException {
        if (sentencia == null) throw new SQLException(MsgConstantes.MSG_PCONSULTA_002);

        if (valor instanceof String) addParam(posIndex, valor, Types.VARCHAR);
        else if (valor instanceof java.sql.Date) addParam(posIndex, (java.sql.Date) valor, Types.DATE);
        else if (valor instanceof Double) addParam(posIndex, valor, Types.DOUBLE);
        else if (valor instanceof Integer) addParam(posIndex, valor, Types.INTEGER);
        else if (valor instanceof Timestamp) addParam(posIndex, valor, Types.TIMESTAMP);
        else if (valor instanceof Long) addParam(posIndex, valor, Types.BIGINT);
        else if (valor instanceof Date) addParam(posIndex, toDateSQL(DateTime.getInstancia((Date) valor)), Types.DATE);
        else if (valor instanceof DateTime) addParam(posIndex, toDateSQL((DateTime) valor), Types.DATE);
        else if (valor instanceof BigInteger) addParam(posIndex, valor, Types.BIGINT);
        else {
            params.put(posIndex, new Parametro(posIndex, valor));
            sentencia.setObject(posIndex, valor); // DEJAR EL MAPEO AL MISMO DRIVER.
        }
    }

    private java.sql.Date toDateSQL(DateTime fecha) {
        return fecha == null ? null : fecha.toDateSQL();
    }

    @Override
    public void changeValorParam(int posIndex, Object nuevoValor) throws SQLException {
        Parametro p = params.get(posIndex);
        if (p.isEscalaSeteado()) sentencia.setObject(p.posIndex, nuevoValor, p.typeSQL, p.escala);
        else if (p.isTipoSQLSeteado()) sentencia.setObject(p.posIndex, nuevoValor, p.typeSQL);
        else sentencia.setObject(p.posIndex, nuevoValor);
    }

    @Override
    public int exe() throws SQLException {
        if (sentencia == null) throw new SQLException(MsgConstantes.MSG_PCONSULTA_002);
        sentencia.executeUpdate();
        int count = sentencia.getUpdateCount();
        sentencia.close();
        return count;
    }

    @Override
    public void exe(DataTable dt) throws SQLException {
        if (sentencia == null) throw new SQLException(MsgConstantes.MSG_PCONSULTA_002);
        ResultSet rs = sentencia.executeQuery();
        FillerDT.fill(rs, dt);
        rs.close();
        sentencia.close();
    }

    @Override
    public void exe(DataSet ds) throws SQLException {
        if (sentencia == null) throw new SQLException(MsgConstantes.MSG_PCONSULTA_002);
        DataTable dt = new DataTable();
        exe(dt);
        ds.getTablas().add(dt);
    }

    @Override
    public DataSet exe(String... nombreTablasSalida) throws SQLException {
        if (nombreTablasSalida == null || nombreTablasSalida.length <= 0) throw new SQLException(MsgConstantes.MSG_PCONSULTA_003);
        DataTable dt = new DataTable();
        exe(dt);
        DataSet ds = new DataSet();
        ds.getTablas().add(dt);
        ds.getTablaAt(0).setNombreTabla(nombreTablasSalida[0]);
        return ds;
    }

    @SuppressWarnings("unused")
    private static class Parametro implements Serializable {

        private static final long serialVersionUID = -5052291187783194733L;

        int posIndex;
        Object valor;
        int typeSQL; // por lo general se setea, pero es opcional si se desea que el driver haga el mapeo segun el JDBC
        int escala; // opcional

        Parametro(int posIndex, Object valor, int typeSQL) {
            this(posIndex, valor, typeSQL, ESCALA_SQL_NO_SETEADA);
        }

        Parametro(int posIndex, Object valor) {
            this(posIndex, valor, TIPO_SQL_NO_SETEADO, ESCALA_SQL_NO_SETEADA);
        }

        Parametro(int posIndex, Object valor, int typeSQL, int escala) {
            this.posIndex = posIndex;
            this.valor = valor;
            this.typeSQL = typeSQL;
            this.escala = escala;
        }

        boolean isEscalaSeteado() {
            return escala != TIPO_SQL_NO_SETEADO;
        }

        boolean isTipoSQLSeteado() {
            return typeSQL != TIPO_SQL_NO_SETEADO;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 67 * hash + this.posIndex;
            hash = 67 * hash + this.typeSQL;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj instanceof Parametro) {
                Parametro objCast = (Parametro) obj;
                return objCast.posIndex == posIndex && objCast.typeSQL == typeSQL;
            }
            return false;
        }
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
