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

public class ConexionPostgrePooled implements IConexion {

	private String nombrePool;
	private String urlBaseDatos;
	private String urlCnxPool; // adicional
	private String bd = "";
	private String server = "";
	private String user = "";
	private String port = "";
	private GenericObjectPool gop = null;

	public ConexionPostgrePooled(String nombreServidor, String nombreBD, String usuario, String password) {
		this(nombreServidor, nombreBD, usuario, password, MsgConstantes.PUERTO_POSTGRE, 64, 64);
	}

	public ConexionPostgrePooled(String nombreServidor, String nombreBD, String usuario, String password,
			String puerto) {
		this(nombreServidor, nombreBD, usuario, password, puerto, 64, 64);
	}

	@SuppressWarnings("unused")
	public ConexionPostgrePooled(String nombreServidor, String nombreBD, String usuario, String password, String puerto,
			int maxActive, int maxIdle) {

		try {
			Class.forName(MsgConstantes.DRIVER_POSTGRE);
		} catch (ClassNotFoundException sos) {
			throw new JcFRException(sos.getMessage());
		}

		this.server = nombreServidor;
		this.port = puerto;
		this.bd = nombreBD;
		this.user = usuario;

		this.urlBaseDatos = "jdbc:postgresql://" + nombreServidor + ":" + puerto + "/" + nombreBD;

		gop = new GenericObjectPool(null);

		gop.setMaxActive(Math.max(8, maxActive));
		gop.setMaxIdle(Math.max(8, maxIdle));

		DriverManagerConnectionFactory dmf = new DriverManagerConnectionFactory(urlBaseDatos, usuario, password);

		try {
			PoolableConnectionFactory refNoUsed = new PoolableConnectionFactory(dmf, gop, null, null, false, true);
		} catch (Exception sos) {
		}
		
		this.nombrePool = Alea.newStringID("pg" + JSUtil.JSUtil.SUBSTR(nombreBD, 1, 5), 3);
		this.urlCnxPool = MsgConstantes.PREFIJO_POOL + nombrePool;

		(new PoolingDriver()).registerPool(nombrePool, gop);
	}

	@Override
	public String getNombreDriver() {
		return MsgConstantes.DRIVER_POSTGRE;
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
