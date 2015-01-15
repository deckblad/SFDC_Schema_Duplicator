package com.blogspot.arahuman.helper;

import org.apache.log4j.Logger;

import com.blogspot.arahuman.data.DBHelper;
import com.blogspot.arahuman.sf.SfDownloader;

public class CmdLineArgs {

	private static final Logger logger = Logger.getLogger(CmdLineArgs.class);
	private boolean enableProxy=false;
    
    private boolean isProxyAuthRequired=false;
  
	private String proxyHost;
    private String proxyPort;
    private String proxyUser;
    private String proxyPassword;
    
    private String sfUser;
    private String sfPassword;
    private String sfToken;
    private String sfEndpoint;
    
    private String dbUser;
    private String dbPassword;
    private String dbHost;
    private String dbSchema;
    	                
    private String sqlPath="c:\\temp\\salesforce\\sqls";
    private String sqlTablefile="sf_objects_create.sql";
    private String sqlRelationfile="sf_relations_create.sql";
    private String sqlPrefix="sfsb_";
    
    private boolean createLocalSqlfiles=false;
    private boolean createLocalTables=true;
    private boolean dropIfTableExists=false;
	private boolean populateDb=true;
    private String errorMessage=null;

	/**
	 * @return the enableProxy
	 */
	public boolean isEnableProxy() {
		return enableProxy;
	}

	/**
	 * @param enableProxy the enableProxy to set
	 */
	public void setEnableProxy(boolean enableProxy) {
		this.enableProxy = enableProxy;
	}
	
	/**
	 * @return the isProxyAuthRequired
	 */
	public boolean isProxyAuthRequired() {
		return isProxyAuthRequired;
	}

	/**
	 * @param isProxyAuthRequired the isProxyAuthRequired to set
	 */
	public void setProxyAuthRequired(boolean isProxyAuthRequired) {
		this.isProxyAuthRequired = isProxyAuthRequired;
	}
	
	/**
	 * @return the proxyHost
	 */
	public String getProxyHost() {
		return proxyHost;
	}

	/**
	 * @param proxyHost the proxyHost to set
	 */
	public void setProxyHost(String proxyHost) {
		this.proxyHost = proxyHost;
	}

	/**
	 * @return the proxyPort
	 */
	public String getProxyPort() {
		return proxyPort;
	}

	/**
	 * @param proxyPort the proxyPort to set
	 */
	public void setProxyPort(String proxyPort) {
		this.proxyPort = proxyPort;
	}

	/**
	 * @return the proxyUser
	 */
	public String getProxyUser() {
		return proxyUser;
	}

	/**
	 * @param proxyUser the proxyUser to set
	 */
	public void setProxyUser(String proxyUser) {
		this.proxyUser = proxyUser;
	}

	/**
	 * @return the proxyPassword
	 */
	public String getProxyPassword() {
		return proxyPassword;
	}

	/**
	 * @param proxyPassword the proxyPassword to set
	 */
	public void setProxyPassword(String proxyPassword) {
		this.proxyPassword = proxyPassword;
	}

	/**
	 * @return the sfUser
	 */
	public String getSfUser() {
		return sfUser;
	}

	/**
	 * @param sfUser the sfUser to set
	 */
	public void setSfUser(String sfUser) {
		this.sfUser = sfUser;
	}

	/**
	 * @return the sfPassword
	 */
	public String getSfPassword() {
		return sfPassword;
	}

	/**
	 * @param sfPassword the sfPassword to set
	 */
	public void setSfPassword(String sfPassword) {
		this.sfPassword = sfPassword;
	}

	/**
	 * @return the sfToken
	 */
	public String getSfToken() {
		return sfToken;
	}

	/**
	 * @param sfToken the sfToken to set
	 */
	public void setSfToken(String sfToken) {
		this.sfToken = sfToken;
	}

	/**
	 * @return the sfEndpoint
	 */
	public String getSfEndpoint() {
		return sfEndpoint;
	}

	/**
	 * @param sfEndpoint the sfEndpoint to set
	 */
	public void setSfEndpoint(String sfEndpoint) {
		this.sfEndpoint = sfEndpoint;
	}

	/**
	 * @return the dbUser
	 */
	public String getDbUser() {
		return dbUser;
	}

	/**
	 * @param dbUser the dbUser to set
	 */
	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	/**
	 * @return the dbPassword
	 */
	public String getDbPassword() {
		return dbPassword;
	}

	/**
	 * @param dbPassword the dbPassword to set
	 */
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	/**
	 * @return the dbHost
	 */
	public String getDbHost() {
		return dbHost;
	}

	/**
	 * @param dbHost the dbHost to set
	 */
	public void setDbHost(String dbHost) {
		this.dbHost = dbHost;
	}

	/**
	 * @return the dbSchema
	 */
	public String getDbSchema() {
		return dbSchema;
	}

	/**
	 * @param dbSchema the dbSchema to set
	 */
	public void setDbSchema(String dbSchema) {
		this.dbSchema = dbSchema;
	}

	/**
	 * @return the sqlPath
	 */
	public String getSqlPath() {
		return sqlPath;
	}

	/**
	 * @param sqlPath the sqlPath to set
	 */
	public void setSqlPath(String sqlPath) {
		this.sqlPath = sqlPath;
	}

	/**
	 * @return the sqlTablefile
	 */
	public String getSqlTablefile() {
		return sqlTablefile;
	}

	/**
	 * @param sqlTablefile the sqlTablefile to set
	 */
	public void setSqlTablefile(String sqlTablefile) {
		this.sqlTablefile = sqlTablefile;
	}

	/**
	 * @return the sqlRelationfile
	 */
	public String getSqlRelationfile() {
		return sqlRelationfile;
	}

	/**
	 * @param sqlRelationfile the sqlRelationfile to set
	 */
	public void setSqlRelationfile(String sqlRelationfile) {
		this.sqlRelationfile = sqlRelationfile;
	}

	/**
	 * @return the sqlPrefix
	 */
	public String getSqlPrefix() {
		return sqlPrefix;
	}

	/**
	 * @param sqlPrefix the sqlPrefix to set
	 */
	public void setSqlPrefix(String sqlPrefix) {
		this.sqlPrefix = sqlPrefix;
	}

	/**
	 * @return the createLocalSqlfiles
	 */
	public boolean isCreateLocalSqlfiles() {
		return createLocalSqlfiles;
	}

	/**
	 * @param createLocalSqlfiles the createLocalSqlfiles to set
	 */
	public void setCreateLocalSqlfiles(boolean createLocalSqlfiles) {
		this.createLocalSqlfiles = createLocalSqlfiles;
	}

    /**
	 * @return the dropIfTableExists
	 */
	public boolean isTableExistsThenDrop() {
		return dropIfTableExists;
	}

	/**
	 * @param dropIfTableExists the dropIfTableExists to set
	 */
	public void setTableExistsThenDrop(boolean dropIfTableExists) {
		this.dropIfTableExists = dropIfTableExists;
	}
	
	/**
	 * @return the createDbSchema
	 */
	public boolean isCreateLocalTables() {
		return createLocalTables;
	}

	/**
	 * @param createDbSchema the createDbSchema to set
	 */
	public void setCreateDbSchema(boolean createDbSchema) {
		this.createLocalTables = createDbSchema;
	}

	/**
	 * @return the populateDb
	 */
	public boolean isPopulateDb() {
		return populateDb;
	}

	/**
	 * @param populateDb the populateDb to set
	 */
	public void setPopulateDb(boolean populateDb) {
		this.populateDb = populateDb;
	}

	public CmdLineArgs() {
	}
	
	public String getErrorMessage(){
		return this.errorMessage;
	}
	
	public boolean validate() {
		if (this.isEnableProxy()){
			if (this.proxyHost == null || this.proxyPort == null) {
				errorMessage="Proxy Enabled, but host/port is null";
				return false;
			}
			if (this.isProxyAuthRequired) {
				if (this.proxyUser == null || this.proxyPassword == null){
					errorMessage="Proxy Auth required is set, but user/password is null";
					return false;
				}
			}
		}
		DBHelper dbHelper = new DBHelper(this);
		if(!dbHelper.testConnection())
			return false;
		
		SfDownloader sfd = new SfDownloader(this);
		if (!sfd.testConnectivity())
			return false;
		
		return true;
	}

	public void printArguments() {
		logger.debug("enableProxy ["+ enableProxy + "]");
	    
	    logger.debug("isProxyAuthRequired [" + isProxyAuthRequired+ "]");
	  
		logger.debug("proxyHost [" + proxyHost + "]");
	    logger.debug("proxyPort [" + proxyPort + "]");
	    logger.debug("proxyUser [" + proxyUser + "]");
	    logger.debug("proxyPassword [" + proxyPassword + "]");
	    
	    logger.debug("sfUser [" + sfUser + "]");
	    logger.debug("sfPassword [" + sfPassword + "]");
	    logger.debug("sfToken [" + sfToken + "]");
	    logger.debug("sfEndpoint [" + sfEndpoint + "]");
	    
	    logger.debug("dbUser [" + dbUser + "]");
	    logger.debug("dbPassword [" + dbPassword + "]");
	    logger.debug("dbHost [" + dbHost + "]");
	    logger.debug("dbSchema [" + dbSchema + "]");
	    	                
	    logger.debug("sqlPath [" + sqlPath + "]");
	    logger.debug("sqlTablefile [" + sqlTablefile + "]");
	    logger.debug("sqlRelationfile [" + sqlRelationfile + "]");
	    logger.debug("sqlPrefix [" + sqlPrefix + "]");
	    
	    logger.debug("createLocalSqlfiles [" + createLocalSqlfiles + "]");
	    logger.debug("createDbSchema [" + createLocalTables + "]");
	    logger.debug("populateDb [" + populateDb + "]");
	}
}
