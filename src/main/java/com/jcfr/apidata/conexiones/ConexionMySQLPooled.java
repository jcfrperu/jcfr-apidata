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

public class ConexionMySQLPooled implements IConexion {

    private String nombrePool;
    private String urlBaseDatos;
    private String urlCnxPool; // adicional
    private String bd = "";
    private String server = "";
    private String user = "";
    private String port = "";
    private GenericObjectPool gop = null;

    public ConexionMySQLPooled(String urlBaseDatos) {
        this.urlBaseDatos = urlBaseDatos;
    }

    public ConexionMySQLPooled(String nombreServidor, String nombreBD, String usuario, String password) {
        this(nombreServidor, nombreBD, usuario, password, MsgConstantes.PUERTO_MYSQL, true, 64, 64);
    }

    public ConexionMySQLPooled(String nombreServidor, String nombreBD, String usuario, String password, String puerto) {
        this(nombreServidor, nombreBD, usuario, password, puerto, true, 64, 64);
    }

    public ConexionMySQLPooled(String nombreServidor, String nombreBD, String usuario, String password, String puerto, boolean autoReconnect) {

        this(nombreServidor, nombreBD, usuario, password, puerto, autoReconnect, 64, 64);
    }

    @SuppressWarnings("unused")
    public ConexionMySQLPooled(String nombreServidor, String nombreBD, String usuario, String password, String puerto, boolean autoReconnect, int maxActive, int maxIdle) {

        try {
            Class.forName(MsgConstantes.DRIVER_MYSQL);
        } catch (ClassNotFoundException sos) {
            throw new JcFRException(sos.getMessage());
        }

        this.server = nombreServidor;
        this.port = puerto;
        this.bd = nombreBD;
        this.user = usuario;

        this.urlBaseDatos = "jdbc:mysql://" + nombreServidor + ":" + puerto + "/" + nombreBD + "?autoReconnect=" + autoReconnect;

        gop = new GenericObjectPool(null);

        gop.setMaxActive(Math.max(8, maxActive));
        gop.setMaxIdle(Math.max(8, maxIdle));

        DriverManagerConnectionFactory dmf = new DriverManagerConnectionFactory(urlBaseDatos, usuario, password);

        try {
            PoolableConnectionFactory refNoUsed = new PoolableConnectionFactory(dmf, gop, null, null, false, true);
        } catch (Exception sos) {
        }

        this.nombrePool = Alea.newStringID("my" + JSUtil.JSUtil.SUBSTR(nombreBD, 1, 5), 3);
        this.urlCnxPool = MsgConstantes.PREFIJO_POOL + nombrePool;

        (new PoolingDriver()).registerPool(nombrePool, gop);
    }

    @Override
    public String getNombreDriver() {
        return MsgConstantes.DRIVER_MYSQL;
    }

    @Override
    public Connection crearConexionJDBC() throws SQLException {
        return DriverManager.getConnection(urlCnxPool);
    }

    public GenericObjectPool getGenericObjectPool() {
        return gop;
    }

    public String getURLPool() {
        return urlCnxPool;
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
