package com.blogspot.arahuman.helper;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
//import org.apache.log4j.Logger;
import java.text.StringCharacterIterator;

import org.apache.log4j.Logger;

//import com.blogspot.arahuman.data.DBHelper;

public class Utilities {

	private static final Logger logger = Logger.getLogger(Utilities.class);

	public Utilities() {
	}

	protected static void writeSforceObject(String query, CmdLineArgs cmd) throws IOException {
		write(query, cmd.getSqlPath(),cmd.getSqlTablefile());
	}

	protected static void writeSforceRelation(String query, CmdLineArgs cmd) throws IOException {
		write(query, cmd.getSqlPath(),cmd.getSqlRelationfile());
	}

	private static void write(String query,String path, String fileName) throws IOException {
		FileWriter fw;
		BufferedWriter bw = null;
		try {
			File f= new File(path+"\\"+fileName);
			if (!f.exists()) {
				File p = new File(path);
				if (!p.exists()) {
					p.mkdirs();
				}
				f.createNewFile();
				logger.debug("New file created [" + fileName + "]");
			}
			fw = new FileWriter(f);
			bw = new BufferedWriter(fw);
			bw.write(query);
		} finally {
			try {
				bw.close();
			} catch (Exception e) {

			}
		}
	}

	protected static String readFileAsString(String filePath) throws java.io.IOException {
		byte[] buffer = new byte[(int) new File(filePath).length()];
		BufferedInputStream f = null;
		try {
			f = new BufferedInputStream(new FileInputStream(filePath));
			f.read(buffer);
		} finally {
			if (f != null)
				try {
					f.close();
				} catch (IOException ignored) {
				}
		}
		return new String(buffer);
	}

	protected static String removeLastChar(String text) {
		return text.substring(0, text.length() - 1);
	}
	
	protected static StringBuilder removeLastChar(StringBuilder text) {
		return text.deleteCharAt(text.length()-1);
	}
	
	protected static boolean inArray(String[] haystack, String needle) {
	    for(int i=0;i<haystack.length;i++) {
	        if(haystack[i].toLowerCase().equals(needle.toLowerCase())) {
	            return true;
	        }
	    }
	    return false;
	}
	
    protected static String addSlashes( String text ){    	
        final StringBuffer sb                   = new StringBuffer( text.length() * 2 );
        final StringCharacterIterator iterator  = new StringCharacterIterator( text );
        
  	  	char character = iterator.current();
        
        while( character != StringCharacterIterator.DONE ){
            if( character == '"' ) sb.append( "\\\"" );
            else if( character == '\'' ) sb.append( "\\\'" );
            else if( character == '\\' ) sb.append( "\\\\" );
            else if( character == '\n' ) sb.append( "\\n" );
            else if( character == '{'  ) sb.append( "\\{" );
            else if( character == '}'  ) sb.append( "\\}" );
            else sb.append( character );
            
            character = iterator.next();
        }
        
        return sb.toString();
    }
}
