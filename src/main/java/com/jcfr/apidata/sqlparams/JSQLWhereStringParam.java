package com.jcfr.apidata.sqlparams;

public class JSQLWhereStringParam extends JSQLParams {

    public String[] camposSelect;
    public String tablaFrom;
    public String campoWhere;
    public String valorWhere;
    public String[] camposOrderBy;

    public JSQLWhereStringParam() {
    }

    public JSQLWhereStringParam(String tablaFrom) {
        this.tablaFrom = tablaFrom;
    }

    public JSQLWhereStringParam select(String... campos) {
        camposSelect = campos;
        return this;
    }

    public JSQLWhereStringParam from(String tabla) {
        tablaFrom = tabla;
        return this;
    }

    public JSQLWhereStringParam where(String campo, String valor) {
        this.campoWhere = campo;
        this.valorWhere = valor;
        return this;
    }

    public JSQLWhereStringParam orderBy(String... campos) {
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
