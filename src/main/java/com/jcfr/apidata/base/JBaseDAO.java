package com.jcfr.apidata.base;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.jcfr.apidata.JApiDataWeb;
import com.jcfr.apidata.sqlparams.JSQLNoWhereParams;
import com.jcfr.apidata.sqlparams.JSQLParams;
import com.jcfr.apidata.sqlparams.JSQLWhereDateTimeParam;
import com.jcfr.apidata.sqlparams.JSQLWhereIntParam;
import com.jcfr.apidata.sqlparams.JSQLWhereLongParam;
import com.jcfr.apidata.sqlparams.JSQLWhereStringParam;
import com.jcfr.apidata.utiles.MsgConstantes;
import com.jcfr.utiles.DateTime;
import com.jcfr.utiles.exceptions.JcFRException;
import com.jcfr.utiles.repositorios.DataTable;

public class JBaseDAO extends JBaseSimpleDAO {

    public JBaseDAO(Connection cnx) {
        super(cnx);
    }

    // EXISTE: revisar limit, tiene un "limit" que en microsoft sql no existe
    public boolean existe(JSQLParams params) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(paramCast.campoWhere, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, true));
            ps.setString(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            boolean res = rs.next();
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(paramCast.campoWhere, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, true));
            ps.setInt(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            boolean res = rs.next();
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(paramCast.campoWhere, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, true));
            ps.setLong(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            boolean res = rs.next();
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(paramCast.campoWhere, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, true));
            ps.setDate(1, JS.toDateSQL(paramCast.valorWhere));
            ResultSet rs = ps.executeQuery();
            boolean res = rs.next();
            rs.close();
            ps.close();
            return res;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public boolean existe(String tablaFrom, String campoWhere, String valorWhere) throws SQLException {
        return existe(new JSQLWhereStringParam(tablaFrom).where(campoWhere, valorWhere));
    }

    public boolean existe(String tablaFrom, String campoWhere, int valorWhere) throws SQLException {
        return existe(new JSQLWhereIntParam(tablaFrom).where(campoWhere, valorWhere));
    }

    public boolean existe(String tablaFrom, String campoWhere, long valorWhere) throws SQLException {
        return existe(new JSQLWhereLongParam(tablaFrom).where(campoWhere, valorWhere));
    }

    public boolean existe(String tablaFrom, String campoWhere, DateTime valorWhere) throws SQLException {
        return existe(new JSQLWhereDateTimeParam(tablaFrom).where(campoWhere, valorWhere));
    }

    public DataTable getTabla(String tablaFrom) throws SQLException {
        return select(new JSQLNoWhereParams(tablaFrom).select("*"), 128);
    }

    public DataTable getTabla(String tablaFrom, int tamAprox) throws SQLException {
        return select(new JSQLNoWhereParams(tablaFrom).select("*"), tamAprox);
    }

    public DataTable select(JSQLParams params) throws SQLException {
        return select(params, 128);
    }

    public DataTable select(JSQLParams params, int tamAprox) throws SQLException {

        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            JApiDataWeb cmd = new JApiDataWeb(cnx);
            DataTable dt = new DataTable(tamAprox);
            cmd.getPSQL().crear(makeSelectFromWhere(paramCast.camposSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy));
            cmd.getPSQL().addParam(1, paramCast.valorWhere, Types.VARCHAR);
            cmd.getPSQL().exe(dt);
            return dt;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            JApiDataWeb cmd = new JApiDataWeb(cnx);
            DataTable dt = new DataTable(tamAprox);
            cmd.getPSQL().crear(makeSelectFromWhere(paramCast.camposSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy));
            cmd.getPSQL().addParam(1, paramCast.valorWhere, Types.INTEGER);
            cmd.getPSQL().exe(dt);
            return dt;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            JApiDataWeb cmd = new JApiDataWeb(cnx);
            DataTable dt = new DataTable(tamAprox);
            cmd.getPSQL().crear(makeSelectFromWhere(paramCast.camposSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy));
            cmd.getPSQL().addParam(1, paramCast.valorWhere, Types.BIGINT);
            cmd.getPSQL().exe(dt);
            return dt;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            JApiDataWeb cmd = new JApiDataWeb(cnx);
            DataTable dt = new DataTable(tamAprox);
            cmd.getPSQL().crear(makeSelectFromWhere(paramCast.camposSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy));
            cmd.getPSQL().addParam(1, JS.toDateSQL(paramCast.valorWhere), Types.DATE);
            cmd.getPSQL().exe(dt);
            return dt;
        }

        if (params instanceof JSQLNoWhereParams) {
            JSQLNoWhereParams paramCast = (JSQLNoWhereParams) params;
            JApiDataWeb cmd = new JApiDataWeb(cnx);
            DataTable dt = new DataTable(tamAprox);
            cmd.getPSQL().crear(makeSelectFromWhere(paramCast.camposSelect, paramCast.tablaFrom, null, paramCast.camposOrderBy));
            cmd.getPSQL().exe(dt);
            return dt;
        }

        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    // UPDATE CAMPOS: CON EL OBJETO PARAMS
    public void updateCampoString(JSQLParams params, String campoSet, String valorSet) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setString(1, valorSet);
            ps.setString(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setString(1, valorSet);
            ps.setInt(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setString(1, valorSet);
            ps.setLong(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setString(1, valorSet);
            ps.setDate(2, JS.toDateSQL(paramCast.valorWhere));
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLNoWhereParams) {
            JSQLNoWhereParams paramCast = (JSQLNoWhereParams) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, null));
            ps.setString(1, valorSet);
            ps.executeUpdate();
            ps.close();
            return;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public void updateCampoInt(JSQLParams params, String campoSet, int valorSet) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setInt(1, valorSet);
            ps.setString(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setInt(1, valorSet);
            ps.setInt(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setInt(1, valorSet);
            ps.setLong(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setInt(1, valorSet);
            ps.setDate(2, JS.toDateSQL(paramCast.valorWhere));
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLNoWhereParams) {
            JSQLNoWhereParams paramCast = (JSQLNoWhereParams) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, null));
            ps.setInt(1, valorSet);
            ps.executeUpdate();
            ps.close();
            return;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public void updateCampoLong(JSQLParams params, String campoSet, long valorSet) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setLong(1, valorSet);
            ps.setString(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setLong(1, valorSet);
            ps.setInt(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setLong(1, valorSet);
            ps.setLong(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setLong(1, valorSet);
            ps.setDate(2, JS.toDateSQL(paramCast.valorWhere));
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLNoWhereParams) {
            JSQLNoWhereParams paramCast = (JSQLNoWhereParams) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, null));
            ps.setLong(1, valorSet);
            ps.executeUpdate();
            ps.close();
            return;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public void updateCampoDouble(JSQLParams params, String campoSet, double valorSet) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setDouble(1, valorSet);
            ps.setString(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setDouble(1, valorSet);
            ps.setInt(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setDouble(1, valorSet);
            ps.setLong(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setDouble(1, valorSet);
            ps.setDate(2, JS.toDateSQL(paramCast.valorWhere));
            ps.executeUpdate();
            ps.close();
            return;
        }

        if (params instanceof JSQLNoWhereParams) {
            JSQLNoWhereParams paramCast = (JSQLNoWhereParams) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, null));
            ps.setDouble(1, valorSet);
            ps.executeUpdate();
            ps.close();
            return;
        }

        throw new JcFRException(MsgConstantes.MSG_JBASE_001);

    }

    public void updateCampoDate(JSQLParams params, String campoSet, DateTime valorSet) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setDate(1, JS.toDateSQL(valorSet));
            ps.setString(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setDate(1, JS.toDateSQL(valorSet));
            ps.setInt(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setDate(1, JS.toDateSQL(valorSet));
            ps.setLong(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setDate(1, JS.toDateSQL(valorSet));
            ps.setDate(2, JS.toDateSQL(paramCast.valorWhere));
            ps.executeUpdate();
            ps.close();
            return;
        }

        if (params instanceof JSQLNoWhereParams) {
            JSQLNoWhereParams paramCast = (JSQLNoWhereParams) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, null));
            ps.setDate(1, JS.toDateSQL(valorSet));
            ps.executeUpdate();
            ps.close();
            return;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);

    }

    public void updateCampoTimestamp(JSQLParams params, String campoSet, DateTime valorSet) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setTimestamp(1, JS.toTimeStamp(valorSet));
            ps.setString(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setTimestamp(1, JS.toTimeStamp(valorSet));
            ps.setInt(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setTimestamp(1, JS.toTimeStamp(valorSet));
            ps.setLong(2, paramCast.valorWhere);
            ps.executeUpdate();
            ps.close();
            return;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, paramCast.campoWhere));
            ps.setTimestamp(1, JS.toTimeStamp(valorSet));
            ps.setDate(2, JS.toDateSQL(paramCast.valorWhere));
            ps.executeUpdate();
            ps.close();
            return;
        }

        if (params instanceof JSQLNoWhereParams) {
            JSQLNoWhereParams paramCast = (JSQLNoWhereParams) params;
            PreparedStatement ps = cnx.prepareStatement(makeUpdateFromWhere(paramCast.tablaFrom, campoSet, null));
            ps.setTimestamp(1, JS.toTimeStamp(valorSet));
            ps.executeUpdate();
            ps.close();
            return;
        }

        throw new JcFRException(MsgConstantes.MSG_JBASE_001);

    }

    // UPDATE CAMPOS: SIN EL OBJETO PARAMS
    public void updateCampoString(String tablaFrom, String campoSet, String valorSet, String campoWhere, String valorWhere) throws SQLException {
        updateCampoString(new JSQLWhereStringParam(tablaFrom).where(campoWhere, valorWhere), campoSet, valorSet);
    }

    public void updateCampoInt(String tablaFrom, String campoSet, int valorSet, String campoWhere, String valorWhere) throws SQLException {
        updateCampoInt(new JSQLWhereStringParam(tablaFrom).where(campoWhere, valorWhere), campoSet, valorSet);
    }

    public void updateCampoLong(String tablaFrom, String campoSet, long valorSet, String campoWhere, String valorWhere) throws SQLException {
        updateCampoLong(new JSQLWhereStringParam(tablaFrom).where(campoWhere, valorWhere), campoSet, valorSet);
    }

    public void updateCampoDouble(String tablaFrom, String campoSet, double valorSet, String campoWhere, String valorWhere) throws SQLException {
        updateCampoDouble(new JSQLWhereStringParam(tablaFrom).where(campoWhere, valorWhere), campoSet, valorSet);
    }

    public void updateCampoDate(String tablaFrom, String campoSet, DateTime valorSet, String campoWhere, String valorWhere) throws SQLException {
        updateCampoDate(new JSQLWhereStringParam(tablaFrom).where(campoWhere, valorWhere), campoSet, valorSet);
    }

    public void updateCampoTimestamp(String tablaFrom, String campoSet, DateTime valorSet, String campoWhere, String valorWhere) throws SQLException {
        updateCampoTimestamp(new JSQLWhereStringParam(tablaFrom).where(campoWhere, valorWhere), campoSet, valorSet);
    }

    // OBTENER UN SOLO CAMPO: CON EL OBJETO PARAMS
    public int getCampoInt(JSQLParams params, String campoSelect, int valorDefault) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setString(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            int res = rs.next() ? rs.getInt(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setInt(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            int res = rs.next() ? rs.getInt(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setLong(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            int res = rs.next() ? rs.getInt(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setDate(1, JS.toDateSQL(paramCast.valorWhere));
            ResultSet rs = ps.executeQuery();
            int res = rs.next() ? rs.getInt(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public long getCampoLong(JSQLParams params, String campoSelect, long valorDefault) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setString(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            long res = rs.next() ? rs.getLong(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setInt(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            long res = rs.next() ? rs.getLong(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setLong(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            long res = rs.next() ? rs.getLong(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setDate(1, JS.toDateSQL(paramCast.valorWhere));
            ResultSet rs = ps.executeQuery();
            long res = rs.next() ? rs.getLong(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public double getCampoDouble(JSQLParams params, String campoSelect, double valorDefault) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setString(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            double res = rs.next() ? rs.getDouble(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setInt(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            double res = rs.next() ? rs.getDouble(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setLong(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            double res = rs.next() ? rs.getDouble(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setDate(1, JS.toDateSQL(paramCast.valorWhere));
            ResultSet rs = ps.executeQuery();
            double res = rs.next() ? rs.getDouble(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public String getCampoString(JSQLParams params, String campoSelect, String valorDefault) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setString(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            String res = rs.next() ? rs.getString(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setInt(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            String res = rs.next() ? rs.getString(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setLong(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            String res = rs.next() ? rs.getString(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setDate(1, JS.toDateSQL(paramCast.valorWhere));
            ResultSet rs = ps.executeQuery();
            String res = rs.next() ? rs.getString(campoSelect) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public DateTime getCampoDateTime(JSQLParams params, String campoSelect, DateTime valorDefault) throws SQLException {
        if (params instanceof JSQLWhereStringParam) {
            JSQLWhereStringParam paramCast = (JSQLWhereStringParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setString(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            DateTime res = rs.next() ? JS.toDateTime(rs.getDate(campoSelect)) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereIntParam) {
            JSQLWhereIntParam paramCast = (JSQLWhereIntParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setInt(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            DateTime res = rs.next() ? JS.toDateTime(rs.getDate(campoSelect)) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereLongParam) {
            JSQLWhereLongParam paramCast = (JSQLWhereLongParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setLong(1, paramCast.valorWhere);
            ResultSet rs = ps.executeQuery();
            DateTime res = rs.next() ? JS.toDateTime(rs.getDate(campoSelect)) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        if (params instanceof JSQLWhereDateTimeParam) {
            JSQLWhereDateTimeParam paramCast = (JSQLWhereDateTimeParam) params;
            PreparedStatement ps = cnx.prepareStatement(makeSelectFromWhere(campoSelect, paramCast.tablaFrom, paramCast.campoWhere, paramCast.camposOrderBy, false));
            ps.setDate(1, JS.toDateSQL(paramCast.valorWhere));
            ResultSet rs = ps.executeQuery();
            DateTime res = rs.next() ? JS.toDateTime(rs.getDate(campoSelect)) : valorDefault;
            rs.close();
            ps.close();
            return res;
        }
        throw new JcFRException(MsgConstantes.MSG_JBASE_001);
    }

    public PreparedStatement prepareStatementParse(String sql) throws SQLException {
        if (sql == null) throw new SQLException("La sentencia preparada es null.");

        String sqlParse = Pattern.compile(MsgConstantes.PATRON_PARSE_FECHA_COMILLAS).matcher(sql).replaceAll("?");

        PreparedStatement ps = cnx.prepareStatement(sqlParse);

        int pos = 1;
        Matcher matcher = Pattern.compile(MsgConstantes.PATRON_PARSE_FECHA).matcher(sql);
        while (matcher.find()) {
            ps.setDate(pos++, JS.toDateSQL(DateTime.getInstancia(matcher.group())));
        }

        return ps;
    }

    private String makeSelectFromWhere(String campoSelect, String tablaFrom, String campoWhere, String[] camposOrderBy, boolean incluyeLimit) throws SQLException {

        StringBuilder sb = new StringBuilder(70);

        sb.append("select ").append(campoSelect);
        sb.append(" from ").append(tablaFrom);
        if (campoWhere != null) sb.append(" where ").append(campoWhere).append(" = ?");

        if (camposOrderBy != null && camposOrderBy.length > 0) {
            sb.append(" order by ");
            for (int i = 0; i < camposOrderBy.length; i++) {
                if (i == 0) sb.append(camposOrderBy[i]);
                else sb.append(", ").append(camposOrderBy[i]);
            }
        }

        if (incluyeLimit) sb.append(" limit 1");

        return sb.toString();
    }

    private String makeSelectFromWhere(String[] camposSelect, String tablaFrom, String campoWhere, String[] camposOrderBy) throws SQLException {

        int size = camposSelect == null ? 0 : camposSelect.length;
        StringBuilder sb = new StringBuilder(size * 14 + 50);

        sb.append("select ");

        if (camposSelect == null || camposSelect.length == 0) {
            sb.append("*");
        } else {
            for (int i = 0; i < camposSelect.length; i++) {
                if (i == 0) sb.append(camposSelect[i]);
                else sb.append(", ").append(camposSelect[i]);
            }
        }

        sb.append(" from ").append(tablaFrom);
        if (campoWhere != null) sb.append(" where ").append(campoWhere).append(" = ?");

        if (camposOrderBy != null && camposOrderBy.length > 0) {
            sb.append(" order by ");
            for (int i = 0; i < camposOrderBy.length; i++) {
                if (i == 0) sb.append(camposOrderBy[i]);
                else sb.append(", ").append(camposOrderBy[i]);
            }
        }

        return sb.toString();
    }

    private String makeUpdateFromWhere(String tablaFrom, String campoSet, String campoWhere) throws SQLException {

        StringBuilder sb = new StringBuilder(70);

        sb.append("update ").append(tablaFrom);
        sb.append(" set ").append(campoSet).append(" = ?");

        if (campoWhere != null) sb.append(" where ").append(campoWhere).append(" = ?");

        return sb.toString();
    }
}
