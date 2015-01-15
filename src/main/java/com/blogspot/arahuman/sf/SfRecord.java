package com.blogspot.arahuman.sf;

import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.blogspot.arahuman.data.DBHelper;
import com.blogspot.arahuman.helper.Utilities;
import com.sforce.soap.partner.sobject.SObject;

public class SfRecord extends Utilities {
	
	private static final Logger logger = Logger.getLogger(SfRecord.class);
	public SfRecord() {
	}
	
	private String getUpsertScript(String dbTableName,String keys,String values,String upsert) {
		return  "INSERT INTO " + dbTableName + " (" + keys + ") VALUES (" + values + 
				") ON DUPLICATE KEY UPDATE " + upsert + ";";
	}

	public int executeUpsert(SObject sfRecord_,List<SfField> fields, String dbTableName,DBHelper dbHelper) throws SQLException {
		StringBuilder updateStmt = new StringBuilder();;
		String insertStmt = null;
		StringBuilder insertFields=new StringBuilder();
		StringBuilder valueFields=new StringBuilder();

		for(SfField sfField :fields){
			insertFields.append(sfField.getDBFieldName());
			insertFields.append(",");
			if (sfRecord_.getField(sfField.getName()) == null) {
				valueFields.append("null");
				valueFields.append(",");
			}else {
				String val = sfRecord_.getField(sfField.getName()).toString();
				if (sfRecord_.getField(sfField.getName()).toString().endsWith(".000Z")){
					val=val.replace(".000Z", ""); //.replace("T", " ")
					valueFields.append("'" + val + "'");
				}else {
					val=Utilities.addSlashes(val);
					valueFields.append("'" + val + "'");
				}
				valueFields.append(",");
			}
			updateStmt.append(sfField.getUpsertScript());
		}
		insertFields = Utilities.removeLastChar(insertFields);
		valueFields  = Utilities.removeLastChar(valueFields);
		updateStmt   = Utilities.removeLastChar(updateStmt);
		try {
			insertStmt = getUpsertScript(dbTableName,insertFields.toString(),valueFields.toString(),updateStmt.toString()); 
			return dbHelper.executeStatement(insertStmt);
		} catch(Exception ex) {
			logger.error("Error occurred during UPSERT",ex);
			dbHelper.LogError(dbTableName, Utilities.addSlashes(insertStmt));
		}
		return 0;
	}

}
