package com.jcfr.apidata;

import java.sql.Connection;
import java.sql.SQLException;

import com.jcfr.apidata.interfaces.IConsulta;
import com.jcfr.apidata.interfaces.IConsultaPreparada;
import com.jcfr.apidata.interfaces.IProcedimiento;

public class JApiDataWeb {

    private JConsulta SQL;
    private JProcedimiento PRO;
    private JConsultaPreparada PSQL;
    private Connection cnx;

    public JApiDataWeb(Connection cnx) throws SQLException {
        if (cnx == null) throw new SQLException("La conexión no puede ser nula");
        this.cnx = cnx;
        PSQL = new JConsultaPreparada(this.cnx);
    }

    public Connection getConnection() {
        return cnx;
    }

    public void cerrar() throws SQLException {
        if (cnx != null && !cnx.isClosed()) {
            cnx.close();
        }
    }

    public IConsulta getSQL() {
        if (SQL == null) SQL = new JConsulta(cnx);
        return SQL;
    }

    public IProcedimiento getPRO() {
        if (PRO == null) PRO = new JProcedimiento(cnx);
        return PRO;
    }

    public IConsultaPreparada getPSQL() {
        return PSQL;
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
