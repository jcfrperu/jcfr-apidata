package com.jcfr.apidata.interfaces;

import java.sql.SQLException;

import com.jcfr.utiles.repositorios.DataSet;
import com.jcfr.utiles.repositorios.DataTable;

public interface IConsulta {

    Object callEscalar(String sentenciaSQL) throws SQLException;

    int callUpdate(String sentenciaSQL) throws SQLException;

    void call(String sentenciaSQL, DataTable dt) throws SQLException;

    void call(String sentenciaSQL, DataSet ds) throws SQLException;

    DataSet call(String sentenciaSQL, String[] nombreTablasSalida) throws SQLException;

    DataTable call(String sentenciaSQL) throws SQLException;

    DataTable call(String sentenciaSQL, int tamAprox) throws SQLException;

    DataTable callParseDate(String sentenciaSQL) throws SQLException;

    void callParseDate(String sentenciaSQL, DataTable dt) throws SQLException;
}
