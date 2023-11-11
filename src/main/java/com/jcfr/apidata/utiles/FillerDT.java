package com.jcfr.apidata.utiles;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import com.jcfr.utiles.repositorios.DataSet;
import com.jcfr.utiles.repositorios.DataTable;

public class FillerDT {

    private FillerDT() {
    }

    public static void fill(ResultSet rs, DataTable dt) throws SQLException {

        ResultSetMetaData meta = rs.getMetaData();
        int nCols = meta.getColumnCount();

        // nombre columnas
        for (int j = 1; j <= nCols; j++) {
            dt.addNombreColumnaFaster(meta.getColumnName(j));
        }

        Object[] fila = null;
        ArrayList<Object[]> listaRef = dt.getListaFilas();
        while (rs.next()) {
            fila = new Object[nCols];
            for (int j = nCols; --j >= 0;) {
                fila[j] = rs.getObject(j + 1);
            }
            listaRef.add(fila);
        }
    }

    public static void fillDataSet(CallableStatement sentencia, DataSet ds) throws SQLException {
        // asume que se tiene un objeto sentencia valido y con todos sus parametros seteados
        DataTable dt;
        ResultSet rs;

        sentencia.execute();

        // bucle infinito, del cual saldremos con break;
        for (;;) {

            int numFilas = sentencia.getUpdateCount();

            if (numFilas >= 0) {
                // > 0 --> devuelve un update count, = 0 --> sentencia DDL o cero actualizaciones
                sentencia.getMoreResults();
                continue;
            }

            // si se ha llegado hasta aquí, se trata de un ResultSet o no hay más resultados
            rs = sentencia.getResultSet();

            if (rs != null) {
                // si hay mas resultados, procesar y buscar mas.
                dt = new DataTable();
                fill(rs, dt);
                ds.getTablas().add(dt);
                rs.close(); // cerrar todos los resultsets despues de haberlos guardados en los datatables
                sentencia.getMoreResults();
                continue;
            }

            // si no hay más resultados, cerramos la sentencia y salimos del bucle infinito

            // sentencia.close();
            // // no la cerramos aqui, porque la deberia cerrar arriba, en niveles superiores
            // por el asunto delos valores retornardos por los procedimientos
            break;
        }

    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
