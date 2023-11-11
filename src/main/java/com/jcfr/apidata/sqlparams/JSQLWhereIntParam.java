package com.jcfr.apidata.sqlparams;

public class JSQLWhereIntParam extends JSQLParams {

    public String[] camposSelect;
    public String tablaFrom;
    public String campoWhere;
    public int valorWhere;
    public String[] camposOrderBy;

    public JSQLWhereIntParam() {
    }

    public JSQLWhereIntParam(String tablaFrom) {
        this.tablaFrom = tablaFrom;
    }

    public JSQLWhereIntParam select(String... campos) {
        camposSelect = campos;
        return this;
    }

    public JSQLWhereIntParam from(String tabla) {
        tablaFrom = tabla;
        return this;
    }

    public JSQLWhereIntParam where(String campo, int valor) {
        this.campoWhere = campo;
        this.valorWhere = valor;
        return this;
    }

    public JSQLWhereIntParam orderBy(String... campos) {
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
