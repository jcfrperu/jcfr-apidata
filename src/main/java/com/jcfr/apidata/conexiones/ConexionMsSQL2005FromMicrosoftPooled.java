package com.jcfr.apidata.conexiones;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.jcfr.apidata.interfaces.IConexion;
import com.jcfr.apidata.utiles.MsgConstantes;
import com.jcfr.utiles.exceptions.JcFRException;
import com.jcfr.utiles.math.Alea;
import com.jcfr.utiles.string.JSUtil;

public class ConexionMsSQL2005FromMicrosoftPooled implements IConexion {

    static {
        try {
            Class.forName(MsgConstantes.DRIVER_MSSQL2005_MICROSOFT);
        } catch (ClassNotFoundException sos) {
            throw new JcFRException(sos.getMessage());
        }
    }

    private String urlBaseDatos; // La cadena de conexión
    // private boolean integratedSecurity = false; // adicional
    private String bd = "";
    private String server = "";
    private String user = "";
    private String port = "";
    private String nombrePool;
    private String urlCnxPool; // adicional
    private GenericObjectPool gop = null;

    // PENDING: falta el pool para integrated
    // public ConexionMsSQL2005FromMicrosoftPooled( String urlBaseDatos ) {
    // this.urlBaseDatos = urlBaseDatos;
    // this.integratedSecurity = true;
    // }
    public ConexionMsSQL2005FromMicrosoftPooled(String nombreServidor, String nombreBD, String usuario, String password) {
        this(nombreServidor, nombreBD, usuario, password, MsgConstantes.PUERTO_MSSQL2005, 64, 64);
    }

    @SuppressWarnings("unused")
    public ConexionMsSQL2005FromMicrosoftPooled(String nombreServidor, String nombreBD, String usuario, String password, String puerto, int maxActive, int maxIdle) {

        String nombreServidorFinal = nombreServidor.trim();

        if (nombreServidor.trim().toLowerCase().equals("(local)") || nombreServidor.trim().toLowerCase().equals("local")) {
            nombreServidorFinal = "localhost";
        }

        server = nombreServidorFinal;
        port = puerto;
        bd = nombreBD;
        user = usuario;

        urlBaseDatos = "jdbc:sqlserver://" + nombreServidorFinal + ":" + puerto.trim() + ";database=" + nombreBD.trim() + ";";

        gop = new GenericObjectPool(null);

        gop.setMaxActive(Math.max(8, maxActive));
        gop.setMaxIdle(Math.max(8, maxIdle));

        DriverManagerConnectionFactory dmf = new DriverManagerConnectionFactory(urlBaseDatos, usuario, password);

        try {
            PoolableConnectionFactory refNoUsed = new PoolableConnectionFactory(dmf, gop, null, null, false, true);
        } catch (Exception sos) {
        }

        this.nombrePool = Alea.newStringID("sq" + JSUtil.JSUtil.SUBSTR(nombreBD, 1, 5), 3);
        this.urlCnxPool = MsgConstantes.PREFIJO_POOL + nombrePool;

        (new PoolingDriver()).registerPool(nombrePool, gop);

    }

    @Override
    public Connection crearConexionJDBC() throws SQLException {
        // if ( integratedSecurity ) {
        // // TODO: no se esta probando conectarse con pool y seguridad integrada
        // return DriverManager.getConnection( urlCnxPool );
        //// return DriverManager.getConnection( urlBaseDatos );
        // }
        return DriverManager.getConnection(urlCnxPool);
        // return DriverManager.getConnection( urlBaseDatos, user, password );
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
