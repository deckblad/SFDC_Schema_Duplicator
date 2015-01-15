package com.blogspot.arahuman.sf;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.blogspot.arahuman.data.DBHelper;
import com.blogspot.arahuman.helper.CmdLineArgs;
import com.blogspot.arahuman.helper.Utilities;
import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

public class SfObject extends Utilities {

	private String Name_;
	private List<SfField> Fields_ = new ArrayList<SfField>();
	private static final Logger logger = Logger.getLogger(SfObject.class);
	private CmdLineArgs Cmd_;
	private DescribeGlobalSObjectResult DgsObject_;
	private DescribeSObjectResult DsObjectResult_;
	private PartnerConnection Connection_;
	private boolean HasLastModified_;
	private boolean HasCreatedDate_;
	private String updateValue;
	private String[] SpecialObjects_;
	
	public SfObject(PartnerConnection connection,DescribeGlobalSObjectResult dgsObject,CmdLineArgs cmd,String[] spo) throws ConnectionException {
		this.Connection_=connection;
		Name_=dgsObject.getName();
		this.DgsObject_=dgsObject;
		this.Cmd_ = cmd;
		this.SpecialObjects_=spo;
		DsObjectResult_ = connection.describeSObject(this.Name_);
		if (DsObjectResult_ != null) {
			Field[] fields = DsObjectResult_.getFields();
			logger.debug(this.Name_ + ":" + fields.length);
			if (fields != null) {
				for(Field field : fields){
					Fields_.add(new SfField(field));
					if (field.getName().toLowerCase().equals("lastmodifieddate")) {
						this.HasLastModified_=true;
					}
					if (field.getName().toLowerCase().equals("createddate")) {
						this.HasCreatedDate_=true;
					}
				}
			}
		}
	}
	
	/**
	 * @return the specialObject
	 */
	public boolean isSpecialObject() {
		return Utilities.inArray(SpecialObjects_, this.Name_);
	}
	
	/**
	 * @return the hasLastModified_
	 */
	public boolean hasLastModifiedDate() {
		return HasLastModified_;
	}
	
	public boolean hasCreatedDate() {
		return HasCreatedDate_;
	}
	
	public String getDBTableName() {
		if (Cmd_.getSqlPrefix()==null) {
			if (Utilities.inArray(DBHelper.getKeywords(),this.Name_)) 
				return  "`" + this.Name_ + "`";
		}
		return Cmd_.getSqlPrefix()+this.Name_;
	}
	
	public String getObjectName() {
		return this.Name_;
	}
	
	public String getRelationshipScript() {
		StringBuilder relationships = new StringBuilder();
		String relationship=null;
		if (DsObjectResult_.getChildRelationships() != null)
		{
				// if multiple objects returned
				for(int i =0;i<DsObjectResult_.getChildRelationships().length;i++)
				{
					ChildRelationship cr = DsObjectResult_.getChildRelationships()[i];
					relationship = "ALTER TABLE " + Cmd_.getSqlPrefix() + cr.getChildSObject()+ " ADD INDEX (" +cr.getField()+ ");\n";
					relationships.append(relationship);
					relationship = "ALTER TABLE " + Cmd_.getSqlPrefix() + cr.getChildSObject()+ "\n";
					relationship += " ADD FOREIGN KEY (" + cr.getField() + ")";
					relationship += " REFERENCES " + Cmd_.getSqlPrefix() + this.DgsObject_.getName() + " (Id);\n";
					relationships.append(relationship);
				}
		}
		return relationships.toString();
	}
	
	public void writeTableScript() throws IOException {
		Utilities.writeSforceObject(getTableScript(), Cmd_);
	}
	
	public void writeRelationScript() throws IOException {
		Utilities.writeSforceRelation(getRelationshipScript(), Cmd_);
	}
	
	public void createTable(DBHelper dbHelper) throws SQLException {
		if (Cmd_.isTableExistsThenDrop()) {
			dbHelper.executeStatement(getDropScript());
		}
		dbHelper.executeStatement(getTableScript());
		logger.debug("Table [" + this.getDBTableName() + "] created/already exists");
	}

	public void upsertSfDataIntoDB(DBHelper dbHelper)  {
		try {
			if (!this.isSpecialObject()) {
				boolean done = false;
				Connection_.setQueryOptions(250);
				QueryResult qr=getQueryResult(getSoql(dbHelper));
				logger.info("Total records found is "+ qr.getSize());
				//logger.info("Now downloading data from Salesforce.com......");
				int rcds=0;
				if (qr.getSize()>0){
					SObject[] sfrecords=null;
					while(! done) {
						sfrecords = qr.getRecords();
						rcds += storeInDB(sfrecords,dbHelper);
						if (rcds % 1000 == 0) {
							logger.info("[" + rcds + "]Records loaded into table [" + getDBTableName() + "]");
						}
						if (qr.isDone()) {
							logger.debug("Inserted all the records");
							done = true;
						} else {
							logger.debug("Getting next batch from sfdc");
							qr = getQueryMore(qr);
						}
						sfrecords=null;
					}
				}
				dbHelper.createLog(this.getDBTableName(), updateValue,rcds);
			}
			else {
				logger.info("Ignoring the special object [" + this.Name_+"]");
			}
		}catch(Exception e) {
			logger.error("Moving on with the next object",e);
		}
	}

	private QueryResult getQueryMore(QueryResult qr) throws ConnectionException {
		int retryCount = 3;
		int cnt=0;
		while(cnt < retryCount) {
			try {
				return Connection_.queryMore(qr.getQueryLocator());
			}catch(ConnectionException ex) {
				cnt++;
				logger.error("Salesforce read connection timedout retrying ..["+cnt+"]",ex);
			}
		}
		logger.error("Salesforce read connection timedout after ["+cnt+"] retries.");
		throw new ConnectionException("Salesforce read connection timedout");
	}

	private QueryResult getQueryResult(String query) throws ConnectionException {
		QueryResult qr=null;
		int retryCount = 3;
		int cnt=0;
		while(cnt < retryCount) {
			try {
				qr =Connection_.query(query);
				return qr;
			}catch(ConnectionException ex) {
				cnt++;
				logger.error("Salesforce read connection timedout retrying ..["+cnt+"]",ex);
			}
		}
		logger.error("Salesforce read connection timedout after ["+cnt+"] retries.");
		throw new ConnectionException("Salesforce read connection timedout");
	}
	
	private String getTableScriptHeader() {
	    String header = "\n\n\n/********************************************************************" + 
				"\n          SALESFORCE.COM OBJECT " + this.Name_ + "\n*********************************************************************/" + 
    			"\n" + "\nCREATE TABLE IF NOT EXISTS " + getDBTableName() + "(";
	    return header;
	}
	
	private String getTableScriptFooter() {
		 return "\n\t)ENGINE=InnoDB DEFAULT CHARSET=utf8;";
	}
	
	private String getTableScript(){
		StringBuilder sb = new StringBuilder();
		sb.append(this.getTableScriptHeader());
		for(SfField field : Fields_) {
			sb.append(field.getCreateScript());
		}
		sb=Utilities.removeLastChar(sb);
		sb.append(getTableScriptFooter());
		return sb.toString();
	}

	private String getDropScript() {
		return "DROP TABLE IF EXISTS " + this.getDBTableName() + ";";
	}
	
	private String getLastModifiedDate(DBHelper dbHelper) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String modifiedDate=null;
		String sql=null;
		if (hasLastModifiedDate()) {
			sql = "SELECT LastModifiedDate FROM "+ this.getDBTableName() + " ORDER BY LastModifiedDate Desc";
			//sql = "SELECT Id FROM "+ this.getDBTableName() + " ORDER BY Id Desc";
			modifiedDate=(String)dbHelper.executeScalar(sql);
			// sometime salesforce.com has multiple rows with the same last modified date. If the process got aborted in the middle of one such timestamp 
			// we need to clean that timestamp locally and restart the same timestamp.
			sql = "DELETE FROM " + this.getDBTableName() + " WHERE LastModifiedDate = '" + modifiedDate + "'";
			int del=dbHelper.executeStatement(sql);
			if (del >0)
				logger.info("***Deleted "+del+" rows***");
		} else if (hasCreatedDate()) {
			sql = "SELECT CreatedDate FROM "+ this.getDBTableName() + " ORDER BY CreatedDate Desc";
			//sql = "SELECT Id FROM "+ this.getDBTableName() + " ORDER BY Id Desc";
			modifiedDate=(String)dbHelper.executeScalar(sql);
			// sometime salesforce.com has multiple rows with the same last modified date. If the process got aborted in the middle of one such timestamp 
			// we need to clean that timestamp locally and restart the same timestamp.
			sql = "DELETE FROM " + this.getDBTableName() + " WHERE CreatedDate = '" + modifiedDate + "'";
			int del=dbHelper.executeStatement(sql);
			if (del > 0)
				logger.info("***Deleted "+del+" rows***");
		}
		if (modifiedDate != null) {
			updateValue=modifiedDate;
			logger.info("Table [" + this.getDBTableName() + "] has been last modified on [" + modifiedDate + "]");
			modifiedDate = modifiedDate.replace(' ', 'T');
			modifiedDate = modifiedDate.replace(".0", ".000Z");
		}
		else {
			sql="TRUNCATE TABLE " + this.getDBTableName();
			dbHelper.executeStatement(sql);
			logger.info("***[" + this.getDBTableName() + "] table Truncated***");
		}
		return modifiedDate;
	}
	
    private String getSoql(DBHelper dbHelper) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException{
		StringBuilder sb=new StringBuilder();
		sb.append("SELECT ");
		int counter=1;
		for(SfField field : Fields_){
			sb.append(field.getName());
			if (Fields_.size() != counter) {
				sb.append(",");
			}
			counter++;
		}
		sb.append(" FROM ");
		sb.append(this.getObjectName());
		sb.append(" ");
		String modifiedDate = getLastModifiedDate(dbHelper);
		if (this.hasLastModifiedDate()) {
			if (modifiedDate != null) {
				sb.append(" WHERE LastModifiedDate >= " + modifiedDate + "");
			}
			sb.append(" ORDER BY LastModifiedDate ASC");
		}else if (this.hasCreatedDate()) {
			if (modifiedDate != null) {
				sb.append(" WHERE CreatedDate >= " + modifiedDate + "");
			}
			sb.append(" ORDER BY CreatedDate ASC");
		}
		logger.debug(sb.toString());
		return sb.toString();
	}
    
	private int storeInDB(SObject[] sfrecords,DBHelper dbHelper) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		int rcds=0;
		SfRecord sfRecord;
		for(SObject sfrcd:sfrecords){
			sfRecord= new SfRecord();
			rcds += sfRecord.executeUpsert(sfrcd,this.Fields_, this.getDBTableName(),dbHelper);
			sfRecord=null;
		}
		//logger.info("[" + rcds + "]Records loaded into table [" + getDBTableName() + "]");
		return rcds;
	}

}