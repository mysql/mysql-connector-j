package com.mysql.jdbc;

import java.sql.SQLException;

class RowDataKeyset implements RowData {

	private ResultSetInternalMethods keyset;
	
	private void buildKeysetColumnsClause(Field[] originalQueryMetadata) 
		throws SQLException {
		
		StringBuffer buf = new StringBuffer();
		
		for (int i = 0; i < originalQueryMetadata.length; i++) {
			if (originalQueryMetadata[i].isPrimaryKey()) {
				if (buf.length() != 0) {
					buf.append(", ");
				}
				
				buf.append("`");
				buf.append(originalQueryMetadata[i].getDatabaseName());
				buf.append("`.`");
				buf.append(originalQueryMetadata[i].getOriginalTableName());
				buf.append("`.`");
				buf.append(originalQueryMetadata[i].getOriginalName());
				buf.append("`");
			}
		}
	}
	
	private String extractWhereClause(String sql) {
		String delims = "'`\"";

		String canonicalSql = StringUtils.stripComments(sql, delims, delims,
				true, false, true, true);

		int whereClausePos = StringUtils.indexOfIgnoreCaseRespectMarker(0,
				canonicalSql, " WHERE ", delims, delims, false /* fixme */);

		if (whereClausePos == -1) {
			return "";
		}

		return canonicalSql.substring(whereClausePos);
	}

	public void addRow(ResultSetRow row) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void afterLast() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void beforeFirst() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void beforeLast() throws SQLException {
		// TODO Auto-generated method stub

	}

	public void close() throws SQLException {
		SQLException caughtWhileClosing = null;
		
		if (this.keyset != null) {
			try {
				this.keyset.close();
			} catch (SQLException sqlEx) {
				caughtWhileClosing = sqlEx;
			}
			
			this.keyset = null;
		}
		
		if (caughtWhileClosing != null) {
			throw caughtWhileClosing;
		}
	}

	public ResultSetRow getAt(int index) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getCurrentRowNumber() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public ResultSetInternalMethods getOwner() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean hasNext() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isAfterLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isBeforeFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isDynamic() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isEmpty() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void moveRowRelative(int rows) throws SQLException {
		// TODO Auto-generated method stub

	}

	public ResultSetRow next() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public void removeRow(int index) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setCurrentRow(int rowNumber) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void setOwner(ResultSetImpl rs) {
		// TODO Auto-generated method stub

	}

	public int size() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean wasEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public void setMetadata(Field[] metadata) {
		// no-op
	}
}
