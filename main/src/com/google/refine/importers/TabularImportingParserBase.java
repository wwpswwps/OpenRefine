/*

Copyright 2011, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package com.google.refine.importers;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.refine.ProjectMetadata;
import com.google.refine.expr.ExpressionUtils;
import com.google.refine.importing.ImportingJob;
import com.google.refine.model.Cell;
import com.google.refine.model.Column;
import com.google.refine.model.ModelException;
import com.google.refine.model.Project;
import com.google.refine.model.Row;
import com.google.refine.util.JSONUtilities;

import javax.swing.JOptionPane;
import java.sql.*;
import java.util.Date; 
import java.util.Calendar; 
import java.text.SimpleDateFormat;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.Transaction;

abstract public class TabularImportingParserBase extends ImportingParserBase {
    private final static Logger logger = LoggerFactory.getLogger("ImportingParserBase");
    static public interface TableDataReader {
        public List<Object> getNextRowOfCells() throws IOException;
    }
    
    @Override
    public JSONObject createParserUIInitializationData(ImportingJob job,
            List<JSONObject> fileRecords, String format) {
        JSONObject options = super.createParserUIInitializationData(job, fileRecords, format);
        
        JSONUtilities.safePut(options, "ignoreLines", -1); // number of blank lines at the beginning to ignore
        JSONUtilities.safePut(options, "headerLines", 1); // number of header lines
        
        JSONUtilities.safePut(options, "skipDataLines", 0); // number of initial data lines to skip
        JSONUtilities.safePut(options, "storeBlankRows", true);
        JSONUtilities.safePut(options, "storeBlankCellsAsNulls", true);
        
        return options;
    }
    
    /**
     * @param useInputStream true if parser takes an InputStream, false if it takes a Reader.
     *  
     */
    protected TabularImportingParserBase(boolean useInputStream) {
        super(useInputStream);
    }
    
    static public void readTable(
        Project project,
        ProjectMetadata metadata,
        ImportingJob job,
        TableDataReader reader,
        String fileSource,
        int limit,
        JSONObject options,
        List<Exception> exceptions
    ) {
        int ignoreLines = JSONUtilities.getInt(options, "ignoreLines", -1);
        int headerLines = JSONUtilities.getInt(options, "headerLines", 1);
        int skipDataLines = JSONUtilities.getInt(options, "skipDataLines", 0);
        int limit2 = JSONUtilities.getInt(options, "limit", -1);
        if (limit > 0) {
            if (limit2 > 0) {
                limit2 = Math.min(limit, limit2);
            } else {
                limit2 = limit;
            }
        }
        
        boolean guessCellValueTypes = JSONUtilities.getBoolean(options, "guessCellValueTypes", false);
        
        boolean storeBlankRows = JSONUtilities.getBoolean(options, "storeBlankRows", true);
        boolean storeBlankCellsAsNulls = JSONUtilities.getBoolean(options, "storeBlankCellsAsNulls", true);
        boolean includeFileSources = JSONUtilities.getBoolean(options, "includeFileSources", false);
        
        String fileNameColumnName = "File";
        if (includeFileSources) {
            if (project.columnModel.getColumnByName(fileNameColumnName) == null) {
                try {
                    project.columnModel.addColumn(
                        0, new Column(project.columnModel.allocateNewCellIndex(), fileNameColumnName), false);
                } catch (ModelException e) {
                    // Ignore: We already checked for duplicate name.
                    logger.info("ModelException",e);
                }
            }
        }
        
        List<String> columnNames = new ArrayList<String>();
        boolean hasOurOwnColumnNames = headerLines > 0;
        
        List<Object> cells = null;
        int rowsWithData = 0;
        
        try {
            while (!job.canceled && (cells = reader.getNextRowOfCells()) != null) {
                if (ignoreLines > 0) {
                    ignoreLines--;
                    continue;
                }
                
                if (headerLines > 0) { // header lines
                    for (int c = 0; c < cells.size(); c++) {
                        Object cell = cells.get(c);
                        
                        String columnName;
                        if (cell == null) {
                            // add column even if cell is blank
                            columnName = "";
                        } else if (cell instanceof Cell) {
                            columnName = ((Cell) cell).value.toString().trim();
                        } else {
                            columnName = cell.toString().trim();
                        }
                        
                        ImporterUtilities.appendColumnName(columnNames, c, columnName);
                    }
                    
                    headerLines--;
                    if (headerLines == 0) {
                        ImporterUtilities.setupColumns(project, columnNames);
                    }
                } else { // data lines
                    Row row = new Row(columnNames.size());
                    
                    if (storeBlankRows) {
                        rowsWithData++;
                    } else if (cells.size() > 0) {
                        rowsWithData++;
                    }
                    
                    if (skipDataLines <= 0 || rowsWithData > skipDataLines) {
                        boolean rowHasData = false;
                        for (int c = 0; c < cells.size(); c++) {
                            Column column = ImporterUtilities.getOrAllocateColumn(
                                project, columnNames, c, hasOurOwnColumnNames);
                            
                            Object value = cells.get(c);
                            if (value instanceof Cell) {
                                row.setCell(column.getCellIndex(), (Cell) value);
                                rowHasData = true;
                            } else if (ExpressionUtils.isNonBlankData(value)) {
                                Serializable storedValue;
                                if (value instanceof String) {
                                    storedValue = guessCellValueTypes ?
                                        ImporterUtilities.parseCellValue((String) value) : (String) value;
                                } else {
                                    storedValue = ExpressionUtils.wrapStorable(value);
                                }
                                
                                row.setCell(column.getCellIndex(), new Cell(storedValue, null));
                                rowHasData = true;
                            } else if (!storeBlankCellsAsNulls) {
                                row.setCell(column.getCellIndex(), new Cell("", null));
                            } else {
                                row.setCell(column.getCellIndex(), null);
                            }
                        }
                        
                        if (rowHasData || storeBlankRows) {
                            if (includeFileSources) {
                                row.setCell(
                                    project.columnModel.getColumnByName(fileNameColumnName).getCellIndex(),
                                    new Cell(fileSource, null));
                            }
                            project.rows.add(row);
                        }
                        
                        if (limit2 > 0 && project.rows.size() >= limit2) {
                            break;
                        }
                    }
                }
            }
            // JOptionPane.showMessageDialog(null, columnNames, "Test", JOptionPane.WARNING_MESSAGE); // Alex
            // JOptionPane.showMessageDialog(null, project.rows, "Project.Rows", JOptionPane.WARNING_MESSAGE); // Alex
            
            // test(columnNames, project.rows, formatJobId(job), project); // Alex
        } catch (IOException e) {
            exceptions.add(e);
        }
    }

    // Test: Format Job ID --- Alex
    public static String formatJobId(ImportingJob job) {
        // System.out.println("!!!*********" + (job + "") + "************!!!");
        String s = ("" + job).substring(41);
        // System.out.println("!!!*********" + s + "**********!!!!!");
        return s;
    }
    // Test: getRowElement --- Alex
    public static String getRowEle(String row, int index) {

        String[] rowEle = row.split(",");
        ArrayList<String> result = new ArrayList<String>();

        for(String s : rowEle)
            result.add(s.trim());

        return result.get(index);

    }

    // Test: format column --- Alex
    public static String formatCol(String str) {
        String result = "Col_";
        for(int i = 0; i < str.length(); i++) {
            if((str.charAt(i) >= '0' && str.charAt(i) <= '9') || (str.charAt(i) >= 'a' && str.charAt(i) <= 'z') || (str.charAt(i) >= 'A' && str.charAt(i) <= 'Z'))
                result += str.charAt(i);
        }
        return result;
    }
    // Test --- Alex
    public static void test(List columnNames, List rows, String jobId, Project project) {// List columnNames, List cells
        // JOptionPane.showMessageDialog(null, "1", "Test", JOptionPane.WARNING_MESSAGE); // Alex
        String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";  // Load JDBC Driver
        String dbURL = "jdbc:sqlserver://localhost:50076; DatabaseName=xxl";  // Connect to Server & DB连接服务器和数据库test
        String userName = "xxl";  // UserName 用户名
        String userPwd = "wwpswwpsxx";  // Pwd 密码
        Connection dbConn;

        try {
            Class.forName(driverName);
            dbConn = DriverManager.getConnection(dbURL, userName, userPwd);
            System.out.println("Connection Successful!");  //如果连接成功 控制台输出Connection Successful!
           
            // JOptionPane.showMessageDialog(null, "2", "DB Conn Successful!", JOptionPane.WARNING_MESSAGE); // Alex
           
            Statement stmt=dbConn.createStatement();// Create SQL Queries 创建SQL命令对象



            String selectJobId = "SELECT * FROM OpenRefineProject WHERE JobId = '" + jobId + "'";
            ResultSet rs=stmt.executeQuery(selectJobId);

            // boolean flag = rs.next();
// if(flag == false){
            
        // }
            // System.out.println("!!!*******" + selectJobId + "*********!!!");
// JOptionPane.showMessageDialog(null, project.id, "Project.Rows", JOptionPane.WARNING_MESSAGE); // Alex
        if(!rs.next()){
            String insertJobId = "INSERT INTO OpenRefineProject VALUES('" + jobId + "')";
            stmt.executeUpdate(insertJobId);

            // Create table 创建表
            // System.out.println("查询");
            // JOptionPane.showMessageDialog(null, "columnNames", "Test", JOptionPane.WARNING_MESSAGE); // Alex
            // JOptionPane.showMessageDialog(null, project.id, "Project.Rows", JOptionPane.WARNING_MESSAGE); // Alex
            /*
            Project's id is in main\src\com\google\refine\model
            in the preview, it will be generated once, but when the project is
            created, it will be generated another time and will not be changed
            */
            // System.out.println("开始读取数据");

            Date now = new Date(); 
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
            String tableName = "Tb_" + dateFormat.format(now);


            String createTable = "CREATE TABLE " + tableName + " (";

            for(int ii = 0; ii < columnNames.size(); ii++) {
                if(ii < columnNames.size() - 1)
                    createTable += formatCol((String)columnNames.get(ii)) + " varchar(255), ";
                else
                    createTable += formatCol((String)columnNames.get(ii)) + " varchar(255))";
            }

            // System.out.print(createTable);
            
            stmt.executeUpdate(createTable);

            // System.out.println("TB created!");

            // System.out.println(rows.get(0));
            // System.out.println(rows.get(1));


            for(int jj = 0; jj < rows.size(); jj++) {

                String insertRow = "INSERT INTO " + tableName + " VALUES(";
                
                for(int m = 0; m < columnNames.size(); m++) {
                    if(m < columnNames.size() - 1)
                        insertRow += "'" + getRowEle(rows.get(jj).toString(), m) + "', ";
                    else
                        insertRow += "'" + getRowEle(rows.get(jj).toString(), m) + "')";
                }
                stmt.executeUpdate(insertRow);
            }
            

            // stmt.executeQuery("CREATE TABLE testSQL (id int, name varchar(255))");
            // ResultSet rs=stmt.executeQuery("CREATE TABLE testSQL (id int, name varchar(255))");//返回SQL语句查询结果集(集合)
            //循环输出每一条记录
            // while(rs.next())
            // {
            // //输出每个字段
            //     System.out.println(rs.getString("id")+"\t"+rs.getString("name")+"\t"+rs.getString("phone"));
            // }
            // System.out.println("读取完毕");
            //关闭连接
            stmt.close();//关闭命令对象连接
            dbConn.close();//关闭数据库连接
        }// Alex
        } catch (Exception e) {
            // JOptionPane.showMessageDialog(null, "3", "Failed!", JOptionPane.WARNING_MESSAGE); // Alex
            e.printStackTrace();
          }
    }
}
