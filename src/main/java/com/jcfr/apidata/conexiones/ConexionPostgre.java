package com.jcfr.apidata.conexiones;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.jcfr.apidata.interfaces.IConexion;
import com.jcfr.apidata.utiles.MsgConstantes;
import com.jcfr.utiles.exceptions.JcFRException;

public class ConexionPostgre implements IConexion {

    static {
        try {
            Class.forName(MsgConstantes.DRIVER_POSTGRE);
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

    public ConexionPostgre(String urlBaseDatos) {
        this.urlBaseDatos = urlBaseDatos;
    }

    public ConexionPostgre(String nombreServidor, String nombreBD, String usuario, String password) {
        this(nombreServidor, nombreBD, usuario, password, MsgConstantes.PUERTO_POSTGRE);
    }

    public ConexionPostgre(String nombreServidor, String nombreBD, String usuario, String password, String puerto) {
        this.urlBaseDatos = "jdbc:postgresql://" + nombreServidor + ":" + puerto + "/" + nombreBD + "/";
        this.server = nombreServidor;
        this.port = puerto;
        this.bd = nombreBD;
        this.user = usuario;
        this.password = password;
    }

    @Override
    public String getNombreDriver() {
        return MsgConstantes.DRIVER_POSTGRE;
    }

    @Override
    public Connection crearConexionJDBC() throws SQLException {
        return DriverManager.getConnection(urlBaseDatos, user, password);
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
