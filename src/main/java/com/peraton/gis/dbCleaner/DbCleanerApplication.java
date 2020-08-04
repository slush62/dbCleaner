package com.peraton.gis.dbCleaner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DbCleanerApplication {

	static Logger ivLogger;
	static Properties ivArtemisProperties;
	static Properties ivDBProperties;
	static String ivDbUrl;

	public DbCleanerApplication () throws IOException {
		java.sql.Connection aConnection = null;
	    try {
			_setLogger("dbCleaner");
			Properties dbProps = _loadDatabasePropertiesFile("db.properties");
			ivDbUrl = "jdbc:postgresql://"+dbProps.getProperty("db.url")+"/"+dbProps.getProperty("db.DataBase");
			ivDBProperties = new Properties();
			ivDBProperties.setProperty("user", dbProps.getProperty("db.UserName"));
			ivDBProperties.setProperty("password", dbProps.getProperty("db.PassWord"));
			ivLogger.info("URL: "+ivDbUrl+" USER: "+ivDBProperties.getProperty("user")+" PASS: "+ivDBProperties.getProperty("password"));
			aConnection = DriverManager.getConnection(ivDbUrl, ivDBProperties.getProperty("user"), ivDBProperties.getProperty("password"));
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(aConnection != null) {
				try {
					aConnection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}				
			}
		}
	}
	
	public static void main(String[] args) {
		SpringApplication.run(DbCleanerApplication.class, args);
		DateTimeFormatter ivFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd' 'HH:mm:ss").appendTimeZoneOffset("Z", true, 2, 4).toFormatter();
		List<String> dbNameList = null;
		Properties appProperties = null;
		try {
			appProperties = _getProperties("dbCleaner.properties");
			String dbNames = appProperties.getProperty("databases");
			dbNameList = Arrays.asList(dbNames.split(","));
		} catch (IOException e) {
			e.printStackTrace();
		}
		while(true) {
			if(dbNameList != null && appProperties != null) {
				DateTime now = new DateTime(DateTimeZone.UTC);
				for(String dbName : dbNameList) {
					String dbColumn = appProperties.getProperty(dbName+"Column");
					Integer numSeconds = Integer.parseInt(appProperties.getProperty(dbName+"Interval"));
					String cutoffTime = now.minusSeconds(numSeconds).toString(ivFormatter);
					String sqlQuery = "DELETE FROM public."+dbName+" WHERE "+dbColumn+"<"+cutoffTime;
					System.out.println(sqlQuery);
				}
				
			}
		}

	}

	private void _setLogger(String appName) throws IOException {
		ivLogger = Logger.getLogger(appName);
		File logDir = new File ("./logs");
		if( !logDir.exists()) {
			logDir.mkdir();
		}
		FileHandler fh = new FileHandler(logDir+"/logfile");
		fh.setFormatter(new SimpleFormatter());
		fh.setLevel(Level.INFO);
		ivLogger.addHandler(fh);
	}

	private Properties _loadDatabasePropertiesFile(String aPropertyFile) throws IOException {
		try {
    		Properties aProp = _getProperties(aPropertyFile);

    		// Environment values can override property values
    		if(System.getenv("DBURL") != null)  { aProp.setProperty("db.url", System.getenv("DBURL")); }
    		if(System.getenv("DBUSER") != null) { aProp.setProperty("db.UserName", System.getenv("DBUSER")); }
    		if(System.getenv("DBPASS") != null)   { aProp.setProperty("db.PassWord", System.getenv("DBPASS")); }
    		if(System.getenv("DATABASE") != null)   { aProp.setProperty("db.DataBase", System.getenv("DATABASE")); }
    		return aProp;
		} catch (IOException e) {
			ivLogger.info("Cannot load properties file: "+aPropertyFile);
			throw e;
		}
	}

	private static Properties _getProperties(String fileName) throws IOException {
		try {
			InputStream input = new FileInputStream(fileName);
			Properties prop = new Properties();
			prop.load(input);
			return prop;
		} catch (IOException e) { 
			System.out.println(e.getMessage());
			e.printStackTrace(); 
		}
		return null;		
	}

}
