package com.jcfr.apidata.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

public interface IConexion {

    Connection crearConexionJDBC() throws SQLException;

    String getURL();

    String getBD();

    String getUser();

    String getServer();

    String getPort();

    String getNombreDriver();

}
