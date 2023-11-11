package com.jcfr.apidata.utiles;

import java.io.Serializable;

public class MsgConstantes implements Serializable {

    private static final long serialVersionUID = 4116474184457221815L;

    // DRIVERS DE CONEXION: com.microsoft.sqlserver.jdbc.SQLServerDriver"
    public static final String DRIVER_MSSQL2000_SUN = "com.sun.sql.jdbc.sqlserver.SQLServerDriver";
    public static final String DRIVER_MSSQL2005_MICROSOFT = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String DRIVER_MYSQL = "org.gjt.mm.mysql.Driver";
    public static final String DRIVER_POSTGRE = "org.postgresql.Driver";
    // PUERTOS POR DEFAULT PARA CONEXION
    public static final String PUERTO_MSSQL2000 = "1433";
    public static final String PUERTO_MSSQL2005 = "1433";
    public static final String PUERTO_MYSQL = "3306";
    public static final String PUERTO_POSTGRE = "5432";
    // ATRIBUTOS DEL POOL
    public static final String PREFIJO_POOL = "jdbc:apache:commons:dbcp:";
    // MENSAJES DE PCONSULTA
    public static final String MSG_PCONSULTA_001 = "Argumento del método crear() no puede ser nulo!";
    public static final String MSG_PCONSULTA_002 = "Debe invocar al método crear(...) antes de invocar a addParam(...)";
    public static final String MSG_PCONSULTA_003 = "Debe invocar ingresar al menos un nombre de tabla!";
    // MENSAJES DE JBASE
    public static final String MSG_JBASE_001 = "JSQLParams params no soportado";
    public static final String MSG_JBASE_002 = "Campos no pueden estar vacíos";

    // PATRON CONSULTA
    public static final String PATRON_PARSE_FECHA = "\\d{1,2}/\\d{1,2}/\\d{1,4}";
    public static final String PATRON_PARSE_FECHA_COMILLAS = "'\\d{1,2}/\\d{1,2}/\\d{1,4}'";
}
