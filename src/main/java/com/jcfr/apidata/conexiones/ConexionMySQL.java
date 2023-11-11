package com.jcfr.apidata.conexiones;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.jcfr.apidata.interfaces.IConexion;
import com.jcfr.apidata.utiles.MsgConstantes;
import com.jcfr.utiles.exceptions.JcFRException;

public class ConexionMySQL implements IConexion {

    static {
        try {
            Class.forName(MsgConstantes.DRIVER_MYSQL);
        } catch (ClassNotFoundException sos) {
            throw new JcFRException(sos.getMessage());
        }
    }

    private String urlBaseDatos; // La cadena de conexión
    private String password;
    private String bd = "";
    private String server = "";
    private String user = "";
    private String port = "";

    public ConexionMySQL(String urlBaseDatos) {
        this.urlBaseDatos = urlBaseDatos;
    }

    public ConexionMySQL(String nombreServidor, String nombreBD, String usuario, String password) {
        this(nombreServidor, nombreBD, usuario, password, MsgConstantes.PUERTO_MYSQL, true);
    }

    public ConexionMySQL(String nombreServidor, String nombreBD, String usuario, String password, String puerto) {
        this(nombreServidor, nombreBD, usuario, password, puerto, true);
    }

    public ConexionMySQL(String nombreServidor, String nombreBD, String usuario, String password, String puerto, boolean autoReconnect) {
        server = nombreServidor;
        port = puerto;
        bd = nombreBD;
        user = usuario;

        urlBaseDatos = "jdbc:mysql://" + nombreServidor + ":" + puerto + "/" + nombreBD + "?autoReconnect=" + autoReconnect;
        this.password = password;
    }

    @Override
    public Connection crearConexionJDBC() throws SQLException {
        return DriverManager.getConnection(urlBaseDatos, user, password);
    }

    @Override
    public String getNombreDriver() {
        return MsgConstantes.DRIVER_MYSQL;
    }

    @Override
    public String getURL() {
        return urlBaseDatos;
    }

    @Override
    public String getBD() {
        return bd;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getServer() {
        return server;
    }

    @Override
    public String getPort() {
        return port;
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
