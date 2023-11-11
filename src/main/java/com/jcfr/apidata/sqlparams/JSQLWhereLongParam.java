package com.jcfr.apidata.sqlparams;

public class JSQLWhereLongParam extends JSQLParams {

    public String[] camposSelect;
    public String tablaFrom;
    public String campoWhere;
    public long valorWhere;
    public String[] camposOrderBy;

    public JSQLWhereLongParam() {
    }

    public JSQLWhereLongParam(String tablaFrom) {
        this.tablaFrom = tablaFrom;
    }

    public JSQLWhereLongParam select(String... campos) {
        camposSelect = campos;
        return this;
    }

    public JSQLWhereLongParam from(String tabla) {
        tablaFrom = tabla;
        return this;
    }

    public JSQLWhereLongParam where(String campo, long valor) {
        this.campoWhere = campo;
        this.valorWhere = valor;
        return this;
    }

    public JSQLWhereLongParam orderBy(String... campos) {
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
