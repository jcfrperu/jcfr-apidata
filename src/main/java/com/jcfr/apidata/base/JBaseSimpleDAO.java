package com.jcfr.apidata.base;

import java.sql.Connection;

import com.jcfr.utiles.base.JBaseUtil;

public abstract class JBaseSimpleDAO extends JBaseUtil {

    protected Connection cnx;

    public JBaseSimpleDAO(Connection cnx) {
        this.cnx = cnx;
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }

    @Override
    public String toString() {
        return "cnx = " + cnx;
    }
}
