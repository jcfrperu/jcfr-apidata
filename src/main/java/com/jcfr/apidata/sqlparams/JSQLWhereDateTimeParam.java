package com.jcfr.apidata.sqlparams;

import com.jcfr.utiles.DateTime;

public class JSQLWhereDateTimeParam extends JSQLParams {

    public String[] camposSelect;
    public String tablaFrom;
    public String campoWhere;
    public DateTime valorWhere;
    public String[] camposOrderBy;

    public JSQLWhereDateTimeParam() {
    }

    public JSQLWhereDateTimeParam(String tablaFrom) {
        this.tablaFrom = tablaFrom;
    }

    public JSQLWhereDateTimeParam select(String... campos) {
        camposSelect = campos;
        return this;
    }

    public JSQLWhereDateTimeParam from(String tabla) {
        tablaFrom = tabla;
        return this;
    }

    public JSQLWhereDateTimeParam where(String campo, DateTime valor) {
        this.campoWhere = campo;
        this.valorWhere = valor;
        return this;
    }

    public JSQLWhereDateTimeParam orderBy(String... campos) {
        camposOrderBy = campos;
        return this;
    }

    @Override
    public String toString() {
        return "tablaFrom=" + tablaFrom + ", campoWhere=" + campoWhere + ", valorWhere=" + valorWhere;
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
