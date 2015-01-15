package com.blogspot.arahuman.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.blogspot.arahuman.helper.CmdLineArgs;

public class DBHelper {

	private static final Logger logger = Logger.getLogger(DBHelper.class);
	private CmdLineArgs cmd;
	private Connection conn = null;
	private ResultSet rs = null;
	private Statement st = null;

	public DBHelper(CmdLineArgs cmd) {
		this.cmd = cmd;
	}

	public boolean testConnection() {
		try {
			logger.info("Testing the connectivity to the local DB");
			openConnection();
			logger.info("Successfully verified the local DB connectivity");
			return true;
		} catch (InstantiationException e) {
			logger.error("DB instatiation error", e);
		} catch (IllegalAccessException e) {
			logger.error("DB illegal Access Exception", e);
		} catch (ClassNotFoundException e) {
			logger.error("DB Class not found exception", e);
		} catch (SQLException e) {
			logger.error("SQL Exception", e);
		} finally {
			closeConnection();
		}
		return false;
	}

	public void openConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			SQLException {
		String URL = "jdbc:mysql://" + cmd.getDbHost() + "/" + cmd.getDbSchema()+"?jdbcCompliantTruncation=true";
		if (conn != null) {
			if (conn.isClosed()) {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
				conn = DriverManager.getConnection(URL, cmd.getDbUser(), cmd.getDbPassword());
			}
		} else {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(URL, cmd.getDbUser(), cmd.getDbPassword());
		}
		logger.debug("DB Connection successfully established");
	}

	public void closeConnection() {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException ignore) {
			}
		}
		if (st != null) {
			try {
				st.close();
			} catch (SQLException ignore) {
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception ignore) { /* ignore close errors */
			}
		}
		logger.debug("DB Connection closed successfully");

	}

	public List<String> GetTableNames(String prefix) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException {
		String query = "SELECT table_name,count(*) FROM information_schema.`COLUMNS` C " + "WHERE TABLE_SCHEMA = '"
				+ cmd.getDbSchema() + "' group by table_name order by 2 desc";
		List<String> tables = new ArrayList<String>();
		try {
			openConnection();
			logger.debug(query);			
			ResultSet rs = executeSelect(query, -1);
			while (rs.next()) {
				tables.add(rs.getString("table_name").replace(prefix.toLowerCase(), ""));
			}
			return tables;
		} finally {
			closeConnection();
		}
	}

	public int executeSingleStatement(String query) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException {
		openConnection();
		logger.debug(query);		
		try {
			Statement s = conn.createStatement();
			int count;
			count = s.executeUpdate(query);
			s.close();
			return count;
		} finally {
			closeConnection();
		}
	}

	public int executeStatement(String query) throws SQLException {
		logger.debug(query);
		Statement s = conn.createStatement();
		int count;
		count = s.executeUpdate(query);
		s.close();
		return count;
	}

	public ResultSet executeSelect(String query, int maxRows) throws SQLException {
		logger.debug(query);
		st = conn.createStatement();
		if (maxRows > 0) {
			st.setMaxRows(maxRows);
		}
		rs = st.executeQuery(query);
		return rs;
	}

	public String executeScalar(String query) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException {
		//openConnection();
		logger.debug(query);		
		try {
			st = conn.createStatement();
			st.setMaxRows(1);
			rs = st.executeQuery(query);
			if (rs.next()) {
				return rs.getString(1);
			}
		} finally {
			//closeConnection();
		}
		return null;
	}

	public static String[] getKeywords() {
		String[] keywords = { "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "AUTO_INCREMENT",
				"BDB", "BEFORE", "BERKELEYDB", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE",
				"CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN", "COLUMNS", "CONDITION",
				"CONNECTION", "CONSTRAINT", "CONTINUE", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME",
				"CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE",
				"DAY_SECOND", "DEC DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE",
				"DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "ELSE", "ELSEIF", "ENCLOSED",
				"ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FIELDS", "FLOAT", "FOR", "FORCE", "FOREIGN",
				"FOUND", "FRAC_SECOND", "FROM", "FULLTEXT", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY",
				"HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER",
				"INNODB", "INOUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERVAL", "INTO", "IO_THREAD", "IS",
				"ITERATE", "JOIN", "KEY KEYS", "KILL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINES", "LOAD",
				"LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY",
				"MASTER_SERVER_ID", "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT",
				"MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL",
				"NUMERIC", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE",
				"PRECISION", "PRIMARY", "PRIVILEGES", "PROCEDURE", "PURGE", "READ", "REAL", "REFERENCES", "REGEXP",
				"RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE",
				"SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SMALLINT", "SOME", "SONAME",
				"SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT",
				"SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SQL_TSI_DAY", "SQL_TSI_FRAC_SECOND", "SQL_TSI_HOUR",
				"SQL_TSI_MINUTE", "SQL_TSI_MONTH", "SQL_TSI_QUARTER", "SQL_TSI_SECOND", "SQL_TSI_WEEK", "SQL_TSI_YEAR",
				"SSL", "STARTING", "STRAIGHT_JOIN", "STRIPED", "TABLE", "TABLES", "TERMINATED", "THEN", "TIMESTAMPADD",
				"TIMESTAMPDIFF", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRUE", "UNDO", "UNION",
				"UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USER_RESOURCES", "USING UTC_DATE",
				"UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN",
				"WHERE", "WHILE", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL" };
		return keywords;
	}

	public void createLog(String tableName,String updatevalue,int rowCount) throws SQLException {
		String query;
		if(updatevalue != null) {
			query = "INSERT INTO DataDup_LOG(TableName,RowCount,UpdateValue,LastModifiedDate) VALUES('" + tableName + "'," + rowCount + ",'"+ updatevalue + "', now());";
		}else {
			query = "INSERT INTO DataDup_LOG(TableName,RowCount,LastModifiedDate) VALUES('" + tableName + "'," + rowCount + ", now());";
		}
		executeStatement(query);
	}
	
	public void LogError(String tableName,String msg) throws SQLException {
		String query = "CREATE TABLE IF NOT EXISTS DataDup_Error ( Id INT AUTO_INCREMENT NOT NULL," +
						   "TableName VARCHAR(100)," +
						   "Error TEXT,"+
						   "LastModifiedDate DATETIME NOT NULL,"+
						   "PRIMARY KEY (Id)"+
						   ") ENGINE = InnoDB ROW_FORMAT = DEFAULT;";
		executeStatement(query);
		query = "INSERT INTO DataDup_Error(TableName,Error,LastModifiedDate) VALUES('" + tableName + "','" + msg + "', now());";
		executeStatement(query);
	}
	
	public String[] getLoadedTables() throws SQLException {
		String query = "CREATE TABLE IF NOT EXISTS DataDup_Log ( Id INT AUTO_INCREMENT NOT NULL," +
				   "TableName VARCHAR(100)," +
				   "RowCount INT,"+
				   "UpdateValue DATETIME ,"+
				   "LastModifiedDate DATETIME NOT NULL,"+
				   "PRIMARY KEY (Id)"+
				   ") ENGINE = InnoDB ROW_FORMAT = DEFAULT;";
		executeStatement(query);
		query = "SELECT tableName FROM datadup_log WHERE LastModifiedDate >= (NOW() - INTERVAL 12 * 60 MINUTE)";
		ResultSet rs=this.executeSelect(query,-1);
		List<String> result = new ArrayList<String>();
		while (rs.next()) {
			result.add(rs.getString("tableName"));
		}
		rs.close();
		return result.toArray(new String[0]);
	}
	
	public String[] getExcludedObjects() throws SQLException {
		String query = "CREATE TABLE IF NOT EXISTS DataDup_Exclude ( Id INT AUTO_INCREMENT NOT NULL," +
				   "TableName VARCHAR(100)," +
				   "LastModifiedDate DATETIME NOT NULL,"+
				   "PRIMARY KEY (Id)"+
				   ") ENGINE = InnoDB ROW_FORMAT = DEFAULT;";
		executeStatement(query);
//		String[] SpecialObjects_ = {"Scontrol","FeedItem","AggregateResult","ActivityHistory","Vote","UserProfileFeed","ProcessInstanceHistory","OpenActivity","NoteAndAttachment","Name","MailmergeTemplate","Group","FeedTrackedChange","FeedLike","EmailStatus","CronTrigger","ContentDocumentLink","BusinessHours"};
//		for(String tbl:SpecialObjects_) {
//			query = "INSERT INTO DataDup_Exclude(TableName,LastModifiedDate) VALUES('"+tbl+"',now());";
//			executeStatement(query);	
//		}
		query = "SELECT tableName FROM DataDup_Exclude";
		ResultSet rs=this.executeSelect(query,-1);
		List<String> result = new ArrayList<String>();
		while (rs.next()) {
			result.add(rs.getString("tableName"));
		}
		rs.close();
		return result.toArray(new String[0]);
	}
}