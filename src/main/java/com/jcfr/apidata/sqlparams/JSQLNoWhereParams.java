package com.jcfr.apidata.sqlparams;

public class JSQLNoWhereParams extends JSQLParams {

    public String[] camposSelect;
    public String tablaFrom;
    public String[] camposOrderBy;

    public JSQLNoWhereParams() {
    }

    public JSQLNoWhereParams(String tablaFrom) {
        this.tablaFrom = tablaFrom;
    }

    public JSQLNoWhereParams select(String... campos) {
        camposSelect = campos;
        return this;
    }

    public JSQLNoWhereParams from(String tabla) {
        tablaFrom = tabla;
        return this;
    }

    public JSQLNoWhereParams orderBy(String... campos) {
        camposOrderBy = campos;
        return this;
    }

    @Override
    public String toString() {
        return "tablaFrom=" + tablaFrom;
    }

    public static String getCreditos() {
        return com.jcfr.utiles.Constantes.MSG_CREDITOS;
    }
}
