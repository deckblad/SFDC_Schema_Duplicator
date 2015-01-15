package com.blogspot.arahuman.sf;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.blogspot.arahuman.data.DBHelper;
import com.blogspot.arahuman.helper.CmdLineArgs;
import com.blogspot.arahuman.helper.Utilities;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class SfDownloader extends Utilities {

	private static final Logger logger = Logger.getLogger(SfDownloader.class);
	private PartnerConnection connection;
	private ConnectorConfig config;
	private CmdLineArgs Cmd_;
	
	public SfDownloader(CmdLineArgs cmd) {
		Cmd_ = cmd;
	}
	
	private void connect() throws ConnectionException {
		config = new ConnectorConfig();
		config.setUsername(Cmd_.getSfUser());
		config.setPassword(Cmd_.getSfPassword()+Cmd_.getSfToken());
		if (Cmd_.isEnableProxy()) {
			config.setProxy("http-proxy.cuc.com", 80);
			if (Cmd_.isProxyAuthRequired()){
				config.setProxyUsername(Cmd_.getProxyUser());
				config.setProxyPassword(Cmd_.getProxyPassword());
			}
		}
		config.setAuthEndpoint(Cmd_.getSfEndpoint());
		config.setReadTimeout(90000);
		config.setConnectionTimeout(90000);
		connection=Connector.newConnection(config);
	}
	
	public boolean testConnectivity() {
		try {
			connect();
			logger.info("Trying to connect to sforce.com");
			logger.info("Salesforce.com auth endPoint is [" + config.getAuthEndpoint() + "]");
			logger.debug("Salesforce.com service endPoint ["	+ config.getServiceEndpoint() + "]");
			logger.debug("Salesforce.com username [" + config.getUsername() + "]");
			logger.debug("Salesforce.com sessionId [" + config.getSessionId() + "]");
			logger.info("Successfully verified the salesforce.com connectivity");
			return true;
		} catch (ConnectionException e) {
			logger.error("Error Occurred while connecting to salesforce.com",e);
		}
		return false;
	}

	public void execute() {
		DescribeGlobalResult dgResult;
		DBHelper dbHelper=new DBHelper(Cmd_);
		try {
			connect();
			dgResult = connection.describeGlobal();
			DescribeGlobalSObjectResult[] soResults = dgResult.getSobjects();
			logger.info("Max batch size for your Organization is " + dgResult.getMaxBatchSize());
			dbHelper.openConnection();
			SfObject sfo;
			String[] existingTables = dbHelper.getLoadedTables();
			String[] specialObjects = dbHelper.getExcludedObjects();
			for(DescribeGlobalSObjectResult soResult : soResults ) {
				//if (soResult.getName().toLowerCase().equals("old_sfdc_opportunities__c")) {
				if(!Utilities.inArray(existingTables, Cmd_.getSqlPrefix()+soResult.getName())) {
					sfo = new SfObject(connection, soResult, Cmd_,specialObjects);
					logger.info("Starting object [" + soResult.getName() + "]");
					//if the connection is already open it will skip it
					if (Cmd_.isCreateLocalTables()) {
						sfo.createTable(dbHelper);
					}
					if (Cmd_.isCreateLocalSqlfiles()) {
						sfo.writeTableScript();
					}
					if(Cmd_.isPopulateDb()) {
						sfo.upsertSfDataIntoDB(dbHelper);
					}
					sfo=null;
					logger.debug("Finished working on object [" + soResult.getName() + "]");
				//}
				}else {
					logger.info("Skipping the object [" + soResult.getName() + "], since it was downloaded in the last 12 hours");
				}
			}
		} catch (ConnectionException e) {
			logger.error("Error Occurred while connecting to salesforce.com",e);
		} catch (InstantiationException e) {
			logger.error("DB Error Occurred while connecting to local db",e);
		} catch (IllegalAccessException e) {
			logger.error("DB Error Occurred while connecting to local db",e);
		} catch (ClassNotFoundException e) {
			logger.error("DB Error Occurred while connecting to local db",e);
		} catch (SQLException e) {
			logger.error("DB Error Occurred while connecting to local db",e);
		} catch (IOException e) {
			logger.error("File IO error",e);
		}
		finally{
			dbHelper.closeConnection();
		}
	}

}
