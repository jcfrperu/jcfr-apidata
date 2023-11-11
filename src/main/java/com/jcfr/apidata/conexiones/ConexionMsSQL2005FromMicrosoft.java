package com.jcfr.apidata.conexiones;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.jcfr.apidata.interfaces.IConexion;
import com.jcfr.apidata.utiles.MsgConstantes;
import com.jcfr.utiles.exceptions.JcFRException;

public class ConexionMsSQL2005FromMicrosoft implements IConexion {

    static {
        try {
            Class.forName(MsgConstantes.DRIVER_MSSQL2005_MICROSOFT);
        } catch (ClassNotFoundException sos) {
            throw new JcFRException(sos.getMessage());
        }
    }

    private String urlBaseDatos; // La cadena de conexión
    private String password;
    private boolean integratedSecurity = false; // adicional
    private String bd = "";
    private String server = "";
    private String user = "";
    private String port = "";

    public ConexionMsSQL2005FromMicrosoft(String urlBaseDatos) {
        this.urlBaseDatos = urlBaseDatos;
        integratedSecurity = true;
    }

    public ConexionMsSQL2005FromMicrosoft(String nombreServidor, String nombreBD, String usuario, String password) {
        this(nombreServidor, nombreBD, usuario, password, MsgConstantes.PUERTO_MSSQL2005);
    }

    public ConexionMsSQL2005FromMicrosoft(String nombreServidor, String nombreBD, String usuario, String password, String puerto) {

        String nombreServidorFinal = nombreServidor.trim();

        if (nombreServidor.trim().toLowerCase().equals("(local)") || nombreServidor.trim().toLowerCase().equals("local")) {
            nombreServidorFinal = "localhost";
        }

        server = nombreServidorFinal;
        port = puerto;
        bd = nombreBD;
        user = usuario;

        urlBaseDatos = "jdbc:sqlserver://" + nombreServidorFinal + ":" + puerto.trim() + ";database=" + nombreBD.trim() + ";";

        this.user = usuario;
        this.password = password;
    }

    @Override
    public Connection crearConexionJDBC() throws SQLException {
        if (integratedSecurity) {
            return DriverManager.getConnection(urlBaseDatos);
        }
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

    @Override
    public String getNombreDriver() {
        return MsgConstantes.DRIVER_MSSQL2005_MICROSOFT;
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
