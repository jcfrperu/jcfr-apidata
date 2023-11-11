package com.jcfr.apidata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.jcfr.apidata.interfaces.IConsulta;
import com.jcfr.apidata.utiles.FillerDT;
import com.jcfr.apidata.utiles.MsgConstantes;
import com.jcfr.utiles.DateTime;
import com.jcfr.utiles.repositorios.DataSet;
import com.jcfr.utiles.repositorios.DataTable;

class JConsulta implements IConsulta {

    private Connection cnx;

    JConsulta(Connection conexion) { // constructor con acceso a paquete
        this.cnx = conexion;
    }

    @Override
    public Object callEscalar(String sentenciaSQL) throws SQLException {
        return call(sentenciaSQL).getValorAt(0, 0);
    }

    @Override
    public int callUpdate(String sentenciaSQL) throws SQLException {
        Statement sentencia = cnx.createStatement();
        sentencia.executeUpdate(sentenciaSQL);
        int count = sentencia.getUpdateCount();
        sentencia.close();
        return count;
    }

    @Override
    public void call(String sentenciaSQL, DataTable dt) throws SQLException {
        Statement sentencia = cnx.createStatement();
        ResultSet rs = sentencia.executeQuery(sentenciaSQL);
        FillerDT.fill(rs, dt);
        rs.close(); // cierra el resultset, pero las referencias a los datos ya estan en el datatable
        sentencia.close();
        // NOTA: La conexion se cierra en capas superiores
    }

    // solo recoge una tabla. En microsoft si se puede recoger varias tablas, pero en mysql no
    // ver si se puede hacer esto: para microsoft que si jale las tablas, y para mysql que solo guarde la primera sin error.
    @Override
    public void call(String sentenciaSQL, DataSet ds) throws SQLException {
        // NOTA. En MySQL no se permite el retorno de varias tablas en una sentencia SQL escriba en codigo cliente.
        ds.getTablas().add(call(sentenciaSQL));
    }

    // solo recoge una tabla. En microsoft si se puede recoger varias tablas, pero en mysql no
    @Override
    public DataSet call(String sentenciaSQL, String[] nombreTablasSalida) throws SQLException {
        // NOTA. En MySQL no se permite el retorno de varias tablas en una sentencia SQL escriba en codigo cliente.
        DataSet ds = new DataSet();
        ds.getTablas().add(call(sentenciaSQL));
        ds.getTablaAt(0).setNombreTabla(nombreTablasSalida[0]);
        return ds;
    }

    @Override
    public DataTable call(String sentenciaSQL) throws SQLException {
        DataTable dt = new DataTable();
        call(sentenciaSQL, dt);
        return dt;
    }

    @Override
    public DataTable callParseDate(String sentenciaSQL) throws SQLException {
        DataTable dt = new DataTable();
        callParseDate(sentenciaSQL, dt);
        return dt;
    }

    @Override
    public void callParseDate(String sentenciaSQL, DataTable dt) throws SQLException {
        PreparedStatement ps = prepareStatementParse(cnx, sentenciaSQL);
        ResultSet rs = ps.executeQuery();
        FillerDT.fill(rs, dt);
        rs.close(); // cierra el resultset, pero las referencias a los datos ya estan en el datatable
        ps.close();
        // NOTA: La conexion se cierra en capas superiores
    }

    private PreparedStatement prepareStatementParse(Connection cnx, String sql) throws SQLException {
        String sqlParse = Pattern.compile(MsgConstantes.PATRON_PARSE_FECHA_COMILLAS).matcher(sql).replaceAll("?");
        PreparedStatement ps = cnx.prepareStatement(sqlParse);
        int pos = 1;
        Matcher matcher = Pattern.compile(MsgConstantes.PATRON_PARSE_FECHA).matcher(sql);
        while (matcher.find()) {
            ps.setDate(pos++, toDateSQL(DateTime.getInstancia(matcher.group())));
        }
        return ps;
    }

    private java.sql.Date toDateSQL(DateTime fecha) {
        return fecha == null ? null : fecha.toDateSQL();
    }

    @Override
    public DataTable call(String sentenciaSQL, int tamAprox) throws SQLException {
        DataTable dt = new DataTable(tamAprox);
        call(sentenciaSQL, dt);
        return dt;
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
