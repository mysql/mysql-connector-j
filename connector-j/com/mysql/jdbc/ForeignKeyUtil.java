/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package com.mysql.jdbc;

import java.sql.SQLException;
import java.sql.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


/**
 * The KeysParser knows how to parse the foreign key part of the comment field
 * of 'show table status'. It is used by DatabaseMetaData.getImportedKeys
 * and DatabaseMetaData.getExportedKeys.
 * 
 */
public class ForeignKeyUtil {

    public static final String SUPPORTS_FK = "SUPPORTS_FK";
    public static final int PKTABLE_CAT = 0;
    public static final int PKTABLE_SCHEM = 1;
    public static final int PKTABLE_NAME = 2;
    public static final int PKCOLUMN_NAME = 3;
    public static final int FKTABLE_CAT = 4;
    public static final int FKTABLE_SCHEM = 5;
    public static final int FKTABLE_NAME = 6;
    public static final int FKCOLUMN_NAME = 7;
    public static final int KEY_SEQ = 8;
    public static final int UPDATE_RULE = 9;
    public static final int DELETE_RULE = 10;
    public static final int FK_NAME = 11;
    public static final int PK_NAME = 12;
    public static final int DEFERRABILITY = 13;

    /**
     * Populates the tuples list with the imported keys of importingTable based on the
     * keysComment from the 'show table status' sql command. KeysComment is that part of
     * the comment field that follows the "InnoDB free ...;" prefix.
     */
    public static void getImportKeyResults(String catalog, 
                                           String importingTable, 
                                           String keysComment, List tuples)
                                    throws SQLException {
        getResultsImpl(catalog, importingTable, keysComment, tuples, null, 
                       false);
    }

    /**
     * Adds to the tuples list the exported keys of exportingTable based on the
     * keysComment from the 'show table status' sql command. KeysComment is that part of
     * the comment field that follows the "InnoDB free ...;" prefix.
     */
    public static void getExportKeyResults(String catalog, 
                                           String exportingTable, 
                                           String keysComment, List tuples, 
                                           String fkTableName)
                                    throws SQLException {
        getResultsImpl(catalog, exportingTable, keysComment, tuples, 
                       fkTableName, true);
    }

    private static void getResultsImpl(String catalog, String table, 
                                       String keysComment, List tuples, 
                                       String fkTableName, boolean isExport)
                                throws SQLException {

        // keys will equal something like this:
        // (parent_service_id child_service_id) REFER ds/subservices(parent_service_id child_service_id)
        // parse of the string into three phases:
        // 1: parse the opening parentheses to determine how many results there will be
        // 2: read in the schema name/table name
        // 3: parse the closing parentheses
        StringTokenizer keyTokens = new StringTokenizer(keysComment.trim(), 
                                                        "()", false);
        String localColumnNamesString = keyTokens.nextToken();
        StringTokenizer localColumnNames = new StringTokenizer(
                                                   localColumnNamesString, 
                                                   " ,");
        String referCatalogTableString = keyTokens.nextToken();
        StringTokenizer referSchemaTable = new StringTokenizer(
                                                   referCatalogTableString, 
                                                   " /");
        String referColumnNamesString = keyTokens.nextToken();
        StringTokenizer referColumnNames = new StringTokenizer(
                                                   referColumnNamesString, 
                                                   " ,");
        referSchemaTable.nextToken(); //discard the REFER token

        String referCatalog = referSchemaTable.nextToken();
        String referTable = referSchemaTable.nextToken();

        if (isExport && !referTable.equals(table)) {

            return;
        }

        if (localColumnNames.countTokens() != referColumnNames.countTokens()) {
            throw new SQLException("Error parsing foriegn keys definition", 
                                   "S1000");
        }

        while (localColumnNames.hasMoreTokens()) {

            byte[][] tuple = new byte[14][];
            String localColumnName = localColumnNames.nextToken();
            String referColumnName = referColumnNames.nextToken();
            tuple[FKTABLE_CAT] = (catalog == null ? new byte[0] : s2b(catalog));
            tuple[FKTABLE_SCHEM] = null;
            tuple[FKTABLE_NAME] = s2b((isExport) ? fkTableName : table);
            tuple[FKCOLUMN_NAME] = s2b(localColumnName);
            tuple[PKTABLE_CAT] = s2b(referCatalog);
            tuple[PKTABLE_SCHEM] = null;
            tuple[PKTABLE_NAME] = s2b((isExport) ? table : referTable);
            tuple[PKCOLUMN_NAME] = s2b(referColumnName);
            tuple[KEY_SEQ] = s2b(Integer.toString(tuples.size()));
            tuple[UPDATE_RULE] = s2b(Integer.toString(
                                             java.sql.DatabaseMetaData.importedKeySetDefault));
            tuple[DELETE_RULE] = s2b(Integer.toString(
                                             getCascadeDeleteOption(keysComment)));
            tuple[FK_NAME] = null; //not available from show table status
            tuple[PK_NAME] = null; //not available from show table status
            tuple[DEFERRABILITY] = s2b(Integer.toString(
                                               java.sql.DatabaseMetaData.importedKeyNotDeferrable));
            tuples.add(tuple);
        }
    }

    private static void getResultsImplFromShowCreate(String catalog, 
                                                     String table, 
                                                     String keysComment, 
                                                     List tuples, 
                                                     String fkTableName, 
                                                     boolean isExport)
                                              throws SQLException {

        // keys will equal something like this:
        // (parent_service_id child_service_id) REFER ds/subservices(parent_service_id child_service_id)
        // parse of the string into three phases:
        // 1: parse the opening parentheses to determine how many results there will be
        // 2: read in the schema name/table name
        // 3: parse the closing parentheses
        StringTokenizer keyTokens = new StringTokenizer(keysComment.trim(), 
                                                        "()", false);
        keyTokens.nextToken(); // eat 'FOREIGN KEY'

        String localColumnNamesString = keyTokens.nextToken();
        StringTokenizer localColumnNames = new StringTokenizer(
                                                   localColumnNamesString, ",");
        String referCatalogTableString = keyTokens.nextToken();
        StringTokenizer referSchemaTable = new StringTokenizer(referCatalogTableString.trim(), 
                                                               " .");
        String referColumnNamesString = keyTokens.nextToken();
        StringTokenizer referColumnNames = new StringTokenizer(
                                                   referColumnNamesString, ",");
        referSchemaTable.nextToken(); //discard the REFERENCES token

        String referCatalog = referSchemaTable.nextToken();
        String referTable = referSchemaTable.nextToken();
        int lastParenIndex = keysComment.lastIndexOf(")");
        String cascadeOptions = null;

        if (lastParenIndex != (keysComment.length() - 1)) {
            cascadeOptions = keysComment.substring(lastParenIndex + 1);
        }

        if (isExport && !referTable.equals(table)) {

            return;
        }

        if (localColumnNames.countTokens() != referColumnNames.countTokens()) {
            throw new SQLException("Error parsing foriegn keys definition", 
                                   "S1000");
        }

        while (localColumnNames.hasMoreTokens()) {

            byte[][] tuple = new byte[14][];
            String localColumnName = localColumnNames.nextToken();
            String referColumnName = referColumnNames.nextToken();
            tuple[FKTABLE_CAT] = (catalog == null ? new byte[0] : s2b(catalog));
            tuple[FKTABLE_SCHEM] = null;
            tuple[FKTABLE_NAME] = s2b((isExport) ? fkTableName : table);
            tuple[FKCOLUMN_NAME] = s2b(localColumnName);
            tuple[PKTABLE_CAT] = s2b(referCatalog);
            tuple[PKTABLE_SCHEM] = null;
            tuple[PKTABLE_NAME] = s2b((isExport) ? table : referTable);
            tuple[PKCOLUMN_NAME] = s2b(referColumnName);
            tuple[KEY_SEQ] = s2b(Integer.toString(tuples.size()));
            tuple[UPDATE_RULE] = s2b(Integer.toString(
                                             java.sql.DatabaseMetaData.importedKeySetDefault));

            if (cascadeOptions == null) {
                tuple[DELETE_RULE] = s2b(Integer.toString(
                                                 java.sql.DatabaseMetaData.importedKeyNoAction));
            } else if (cascadeOptions.equalsIgnoreCase("ON DELETE CASCADE")) {
                tuple[DELETE_RULE] = s2b(Integer.toString(
                                                 java.sql.DatabaseMetaData.importedKeyCascade));
            } else if (cascadeOptions.equalsIgnoreCase("ON DELETE SET NULL")) {
                tuple[DELETE_RULE] = s2b(Integer.toString(
                                                 java.sql.DatabaseMetaData.importedKeySetNull));
            } else {
                // shouldn't ever reach here, but it's a fallback in case the cascade  
                // options change. 
            
                tuple[DELETE_RULE] = s2b(Integer.toString(
                                                 java.sql.DatabaseMetaData.importedKeyNoAction));
            }

            tuple[FK_NAME] = null; //not available from show table status
            tuple[PK_NAME] = null; //not available from show table status
            tuple[DEFERRABILITY] = s2b(Integer.toString(
                                               java.sql.DatabaseMetaData.importedKeyNotDeferrable));
            tuples.add(tuple);
        }
    }

    /**
     * Creates a result set similar enough to 'SHOW TABLE STATUS' to allow the
     * same code to work on extracting the foreign key data
     */
    public static ResultSet extractForeignKeyFromCreateTable(java.sql.Connection conn, 
                                                             java.sql.DatabaseMetaData metadata, 
                                                             String catalog, 
                                                             String tableName)
                                                      throws SQLException {

        ArrayList tableList = new ArrayList();
        java.sql.ResultSet rs = null;
        java.sql.Statement stmt = null;

        if (tableName != null) {
            tableList.add(tableName);
        } else {

            try {
                rs = metadata.getTables(catalog, "", "%", 
                                        new String[] { "TABLE" });

                while (rs.next()) {
                    tableList.add(rs.getString("TABLE_NAME"));
                }
            } finally {

                if (rs != null) {
                    rs.close();
                }

                rs = null;
            }
        }

        ArrayList rows = new ArrayList();
        Field[] fields = new Field[3];
        fields[0] = new Field("", "Name", Types.CHAR, Integer.MAX_VALUE);
        fields[1] = new Field("", "Type", Types.CHAR, 255);
        fields[2] = new Field("", "Comment", Types.CHAR, Integer.MAX_VALUE);

        int numTables = tableList.size();

        try {

            for (int i = 0; i < numTables; i++) {

                String tableToExtract = (String) tableList.get(i);
                stmt = conn.createStatement();

                String query = new StringBuffer("SHOW CREATE TABLE ").append(
                                       "`").append(catalog).append("`.`").append(
                                       tableToExtract).append("`").toString();
                rs = stmt.executeQuery(query);

                while (rs.next()) {
                    extractForeignKeyForTable(rows, rs, catalog);
                }
            }
        } finally {

            if (rs != null) {
                rs.close();
            }

            rs = null;

            if (stmt != null) {
                stmt.close();
            }

            stmt = null;
        }

        return new ResultSet(fields, new RowDataStatic(rows));
    }

    /** 
     * Extracts foreign key info for one table.
     */
    public static List extractForeignKeyForTable(ArrayList rows, 
                                                 java.sql.ResultSet rs,
                                                 String catalog)
                                          throws SQLException {

        byte[][] row = new byte[3][];
        row[0] = rs.getBytes(1);
        row[1] = s2b(SUPPORTS_FK);

        String createTableString = rs.getString(2);
        StringTokenizer lineTokenizer = new StringTokenizer(createTableString, 
                                                            "\n");
        StringBuffer commentBuf = new StringBuffer("comment; ");
        boolean firstTime = true;

        while (lineTokenizer.hasMoreTokens()) {

            String line = lineTokenizer.nextToken().trim();

            if (line.startsWith("FOREIGN KEY")) {

                if (line.endsWith(",")) {
                    line = line.substring(0, line.length() - 1);
                }

                // Remove all back-ticks
                int lineLength = line.length();
                StringBuffer lineBuf = new StringBuffer(lineLength);

                for (int i = 0; i < lineLength; i++) {

                    char c = line.charAt(i);

                    if (c != '`') {
                        lineBuf.append(c);
                    }
                }

                line = lineBuf.toString();

                StringTokenizer keyTokens = new StringTokenizer(line, "()", 
                                                                false);
                keyTokens.nextToken(); // eat 'FOREIGN KEY'

                String localColumnNamesString = keyTokens.nextToken();
                String referCatalogTableString = keyTokens.nextToken();
                StringTokenizer referSchemaTable = new StringTokenizer(referCatalogTableString.trim(), 
                                                                       " .");
                String referColumnNamesString = keyTokens.nextToken();
                referSchemaTable.nextToken(); //discard the REFERENCES token

                int numTokensLeft = referSchemaTable.countTokens();
                
                String referCatalog = null;
                String referTable = null;
                
                
                if (numTokensLeft == 2) {
                    // some versions of MySQL don't report the database name
                    referCatalog = referSchemaTable.nextToken();
                    referTable = referSchemaTable.nextToken();
                } else {
                    referTable = referSchemaTable.nextToken();
                    referCatalog = catalog;
                }
                

                if (!firstTime) {
                    commentBuf.append("; ");
                } else {
                    firstTime = false;
                }

                commentBuf.append("(");
                commentBuf.append(localColumnNamesString);
                commentBuf.append(") REFER ");
                commentBuf.append(referCatalog);
                commentBuf.append("/");
                commentBuf.append(referTable);
                commentBuf.append("(");
                commentBuf.append(referColumnNamesString);
                commentBuf.append(")");

                int lastParenIndex = line.lastIndexOf(")");

                if (lastParenIndex != (line.length() - 1)) {

                    String cascadeOptions = cascadeOptions = line.substring(
                                                                     lastParenIndex + 1);
                    commentBuf.append(" ");
                    commentBuf.append(cascadeOptions);
                }
            }
        }

        row[2] = s2b(commentBuf.toString());
        rows.add(row);

        return rows;
    }

    /**
     * Parses the cascade option string and returns the DBMD constant
     * that represents it
     */
    public static int getCascadeDeleteOption(String commentString) {

        int lastParenIndex = commentString.lastIndexOf(")");

        if (lastParenIndex != (commentString.length() - 1)) {

            String cascadeOptions = cascadeOptions = commentString.substring(
                                                             lastParenIndex + 1)
             .trim();

            if (cascadeOptions.equalsIgnoreCase("ON DELETE CASCADE")) {

                return DatabaseMetaData.importedKeyCascade;
            } else if (cascadeOptions.equalsIgnoreCase("ON DELETE SET NULL")) {

                return DatabaseMetaData.importedKeySetNull;
            }
        }

        return DatabaseMetaData.importedKeySetDefault;
    }

    private static byte[] s2b(String s) {

        return s.getBytes();
    }
}