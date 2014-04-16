
package com.google.refine.myDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.google.refine.model.Column;
import com.google.refine.model.Project;
import com.google.refine.model.Row;
import com.google.refine.model.Cell;

public class DatabaseOperation {

    static String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static String dbURL = "jdbc:sqlserver://localhost:50076; DatabaseName=xxl"; // Connect
                                                                                // to
                                                                                // Server
                                                                                // &
                                                                                // DB连接服务器和数据库test
    static String userName = "xxl"; // UserName 用户名
    static String userPwd = "wwpswwpsxx"; // Pwd 密码

    /*
     * Create tables when projects are created
     */
    public static void databaseTableInput(ArrayList<ArrayList<String>> rows, List<Column> columns, String name, long id)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableName = formatControl(name) + "_Pj" + id;
        String tableCreateQuery = "CREATE TABLE " + tableName + " (";
        for (int i = 0; i < columns.size(); i++) {
            if (i < columns.size() - 1)
                tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255), ";
            else
                tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255))";
        }
        ps.executeUpdate(tableCreateQuery);
        String insertQuery = "INSERT INTO " + tableName + " VALUES('";
        for (int j = 0; j < rows.size(); j++) {
            for (int k = 0; k < columns.size(); k++) {
                if (k < columns.size() - 1) {
                    insertQuery += rows.get(j).get(k) + "','";
                } else {
                    insertQuery += rows.get(j).get(k) + "')";
                }
            }
            insertQuery = insertQuery.replaceAll("\n", "");
            ps.executeUpdate(insertQuery);
            insertQuery = "INSERT INTO " + tableName + " VALUES('";
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Rename operation
    public static void databaseRenameUpdate(String projectId, String newname, String oldname)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableOldName = formatControl(oldname) + "_Pj" + projectId;
        System.out.println("******test111111*****oldname: " + tableOldName);
        String tableNewName = formatControl(newname) + "_Pj" + projectId;
        System.out.println("******test111111*****newname: " + tableNewName);
        String tableRename = "EXEC sp_rename '" + tableOldName + "', '" + tableNewName + "'";
        System.out.println(tableRename);
        ps.executeUpdate(tableRename);
        System.out.println("********");
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Cell content edit, Row & Col operations [i.e.] Col Addition / Col Removal
    // etc.
    public static void databaseRowsColsUpdate(ArrayList<ArrayList<String>> rows, List<Column> columns, String name,
            long id)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableName = formatControl(name) + "_Pj" + id;
        String tableDropQuery = "DROP TABLE " + tableName;
        ps.executeUpdate(tableDropQuery);
        if (columns.isEmpty()) {
            return;
        } else {
            String tableCreateQuery = "CREATE TABLE " + tableName + " (";
            for (int i = 0; i < columns.size(); i++) {
                if (i < columns.size() - 1)
                    tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255), ";
                else
                    tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255))";
            }
            ps.executeUpdate(tableCreateQuery);
            if (rows.isEmpty()) {
                return;
            } else {
                String insertQuery = "INSERT INTO " + tableName + " VALUES('";
                for (int j = 0; j < rows.size(); j++) {
                    for (int k = 0; k < columns.size(); k++) {
                        if (k < columns.size() - 1) {
                            insertQuery += rows.get(j).get(k) + "','";
                        } else {
                            insertQuery += rows.get(j).get(k) + "')";
                        }
                    }
                    insertQuery = insertQuery.replaceAll("\n", "");
                    ps.executeUpdate(insertQuery);
                    insertQuery = "INSERT INTO " + tableName + " VALUES('";
                }
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Format String
    public static String formatControl(String str) {
        String result = "";
        for (int i = 0; i < str.length(); i++) {
            if ((str.charAt(i) >= '0' && str.charAt(i) <= '9') || (str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                    || (str.charAt(i) >= 'A' && str.charAt(i) <= 'Z' || str.charAt(i) == '_')) result += str.charAt(i);
        }
        return result;
    }

    // Format Col's name
    public static String headlineControl(String str) {
        String result = "Column_";
        for (int i = 0; i < str.length(); i++) {
            if ((str.charAt(i) >= '0' && str.charAt(i) <= '9') || (str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                    || (str.charAt(i) >= 'A' && str.charAt(i) <= 'Z' || str.charAt(i) == '_')) result += str.charAt(i);
        }
        return result;
    }

    // Table drop
    public static void dababaseTableDrop(String name, long id)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableName = formatControl(name) + "_Pj" + id;
        String tableDropQuery = "DROP TABLE " + tableName;
        ps.executeUpdate(tableDropQuery);
    }

    // Get reordered rows in "ArrayList<ArrayList<String>>" format
    public static ArrayList<ArrayList<String>> getReorderedRows(Project project) {
        ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
        if (project.columnModel.columns.isEmpty() || project.rows.isEmpty()) {
            return rows;
        } else {
            for (int i = 0; i < project.rows.size(); i++) {
                rows.add(new ArrayList<String>());
                for (int j = 0; j < project.columnModel.columns.size(); j++) {
                    if (project.columnModel.columns.get(j).getCellIndex() > (project.rows.get(i).cells.size() - 1)) {
                        rows.get(i).add("");
                    } else {
                        if (project.rows.get(i).cells.get(project.columnModel.columns.get(j).getCellIndex()) == null) {
                            rows.get(i).add("");
                        } else {
                            rows.get(i).add(
                                    project.rows.get(i).cells.get(project.columnModel.columns.get(j).getCellIndex())
                                            .toString());
                        }
                    }
                }
            }
            return rows;
        }

    }
}
