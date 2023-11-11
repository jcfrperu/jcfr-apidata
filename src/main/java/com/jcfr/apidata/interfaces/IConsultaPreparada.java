package com.jcfr.apidata.interfaces;

import java.sql.SQLException;

import com.jcfr.utiles.repositorios.DataSet;
import com.jcfr.utiles.repositorios.DataTable;

public interface IConsultaPreparada {

    void crear(String sentenciaSQL) throws SQLException;

    void addParam(int posIndex, Object valor) throws SQLException;

    void addParam(int posIndex, Object valor, int typeSQL) throws SQLException;

    void addParam(int posIndex, Object valor, int typeSQL, int escala) throws SQLException;

    void changeValorParam(int posIndex, Object nuevoValor) throws SQLException;

    int exe() throws SQLException;

    void exe(DataTable dt) throws SQLException;

    void exe(DataSet ds) throws SQLException;

    DataSet exe(String... nombreTablasSalida) throws SQLException;

}
