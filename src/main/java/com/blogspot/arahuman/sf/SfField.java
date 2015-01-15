package com.blogspot.arahuman.sf;
//import org.apache.log4j.Logger;


import com.blogspot.arahuman.data.DBHelper;
import com.blogspot.arahuman.helper.Utilities;
import com.sforce.soap.partner.Field;

public class SfField extends Utilities implements Comparable<SfField> {
	
	private String Name_;
	//private String Type_;
	private boolean IsPrimary_ = false;
	private Field Field_;
	private int Precision_;
	private int Length_;

	public SfField(Field field) {
		this.Field_ = field;
		this.Name_ = field.getName();
		//this.Type_ = field.getType().toString();
		if (field.getName().toLowerCase().equals("id")) {
			this.IsPrimary_ = true;
		}
		this.Precision_ = field.getPrecision();
		this.Length_ = field.getLength();
	}

	public String getDBFieldName() {
		if (Utilities.inArray(DBHelper.getKeywords(), this.Name_))
			return "`" + this.Name_ + "`";
		else
			return this.Name_;
	}

	public String getName() {
		return Name_;
	}

	private String getDBType() {
		String dbType;

		switch (this.Field_.getType())
		{
			case id:
			case reference:
				//dbType = "VARCHAR("+this.Length_+") DEFAULT ''";
				dbType = "VARCHAR("+this.Length_+") CHARSET utf8 COLLATE utf8_bin DEFAULT ''";
				break;
			case percent:
			case _double:
			case currency:				
				dbType = "DOUBLE(" +this.Precision_+ "," + this.Field_.getScale() +")";
				break;
			case _boolean :
				dbType = "VARCHAR(5) DEFAULT ''";
				break;
			case base64:
				dbType = "LONGBLOB";
				break;
			case _int:
				dbType = "INT";
				break;
			case time:
				dbType = "TIME";
				break;
				/*			case date:
				dbType = "DATE";
				break;
			case datetime:
				dbType = "DATETIME NULL";
				break;*/				
			case date:
			case datetime:	
				dbType = "VARCHAR(32) DEFAULT ''";
				break;
			case phone:
			case string:
			case textarea:
			case url:
			case email:
			case picklist:
			case multipicklist:
			case combobox:
			case encryptedstring:

				//de: use varchar for small fields so we don't have to worry about default value.
/*				if (this.Length_ < 256){
					dbType = "TINYTEXT";
				}else if(this.Length_ >=256 && this.Length_ < 65535){
					dbType = "TEXT";
				}else {
					dbType = "LONGTEXT";
				}
				break;
*/
			default: //calculated,
				if (this.Length_ < 256){
					dbType = "VARCHAR(" + this.Length_ + ") DEFAULT ''";
				}else if(this.Length_ >=256 && this.Length_ < 65535){
					dbType = "TEXT";
				}else {
					dbType = "LONGTEXT";
				}
				break;
				
		}
		return dbType;
	}

	private String getNullable() {
		if (!Field_.isNillable()) {
			return " NOT NULL";
		} else {
			return "";
		}
	}

	private String getPrimary() {
		if (this.IsPrimary_)
			return " PRIMARY KEY";
		else
			return "";
	}

	public String getCreateScript() {
		return "\n\t\t" + getDBFieldName() + " " + this.getDBType() + getNullable() + getPrimary() + ",";
	}

	public String getUpsertScript() {
		return " " + this.getDBFieldName() + " = VALUES(" + this.getDBFieldName() + "),";
	}
	
	public int compareTo(SfField other) {
		return getName().toLowerCase().compareTo(other.getName().toLowerCase());
	}	
}
