package com.jcfr.apidata.interfaces;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.Date;

import com.jcfr.apidata.utiles.InOutParamEnum;
import com.jcfr.utiles.DateTime;
import com.jcfr.utiles.repositorios.DataSet;
import com.jcfr.utiles.repositorios.DataTable;

public interface IProcedimiento {

    void crear(String nombrePRO);

    void addParam(String nombre, Object valor, InOutParamEnum tipoDIR, int tipoJSQL);

    void addParam(String nombre, Object valor, InOutParamEnum tipoDIR, int tipoJSQL, int escala);

    void changeValorParam(String nombre, Object nuevoValor);

    void exe() throws SQLException;

    void exe(DataTable dt) throws SQLException;

    void exe(DataSet ds) throws SQLException;

    DataSet exe(String[] nombreTablasSalida) throws SQLException;

    void cerrar() throws SQLException;

    Object getParamObject(String nombreParametro) throws SQLException;

    float getParamFloat(String nombreParametro) throws SQLException;

    double getParamDouble(String nombreParametro) throws SQLException;

    short getParamShort(String nombreParametro) throws SQLException;

    int getParamInt(String nombreParametro) throws SQLException;

    long getParamLong(String nombreParametro) throws SQLException;

    char getParamChar(String nombreParametro) throws SQLException;

    DateTime getParamDateTime(String nombreParametro) throws SQLException;

    Date getParamDate(String nombreParametro) throws SQLException;

    Blob getParamBlob(String nombreParametro) throws SQLException;

    String getParamString(String nombreParametro) throws SQLException;

}
