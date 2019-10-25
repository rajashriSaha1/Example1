package com.rs.data.sap.transfer;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;
 

public class CollectionDataInsert {

	static String fsep = System.getProperty("file.separator");
	Vector<String> log = new Vector<String>(); 
	public static void main(String[] args) throws ParseException,IOException,FileNotFoundException, SQLException 
	{ 
		System.out.println("Inside the main program");
		CollectionDataInsert collectinDataInsert = new CollectionDataInsert();
		collectinDataInsert.run(); 
	}

	public void run() 
	{

		ArrayList<String> fileNameList;
		String fileName = null;
		File readFile =null;
		Scanner scanFile = null;
		
		FileOutputStream fout = null;
		FileOutputStream fout1 = null;
		File collectionDir = new File("D:" + fsep + "Shares" + fsep + "sap-nfs" + fsep + "sap-shares" + fsep + "SAPTEST" + fsep + "PRD" + fsep + "INBOUND" + fsep + "BJ"+ fsep + "BJ_COLLECTION");	
		try 
		{ 				    
			File[] minFilesList = collectionDir.listFiles();
			int fileNos = minFilesList.length; 
			Connection connectionDB2 = null;
			connectionDB2 = getDBConnection();
			for (int i= 0;i<fileNos;i++)
			{
				try{
					log.clear();	
					//connectionDB2.setAutoCommit(false);
					fileName = minFilesList[i].getName();
					String message=bjSchdInsert(fileName);
					log.addElement(this.timeStamp() +" :: "+message);
					
					fout = new FileOutputStream("D:" + fsep + "Shares" + fsep + "sap-nfs" + fsep + "sap-shares" + fsep + "SAPTEST" + fsep + "PRD" + fsep + "INBOUND" + fsep + "BJ"+ fsep + "BJ_COLLECTION_PROCESSED" + fsep+ fileName,true); 
					fout1 = new FileOutputStream("D:" + fsep + "Shares" + fsep + "sap-nfs" + fsep + "sap-shares" + fsep + "SAPTEST" + fsep + "PRD" + fsep + "INBOUND" + fsep + "BJ"+ fsep + "BJ_COLLECTION_LOG" + fsep+ fileName,true); 
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
					scanFile = new Scanner(minFilesList[i]);
					while(scanFile.hasNextLine())
					{
						String stringValueOfManDateData = scanFile.nextLine();
						fout.write(stringValueOfManDateData.getBytes());
						fout.write(System.getProperty("line.separator").getBytes());
						if(!stringValueOfManDateData.startsWith("MIN"))
						{
							if(stringValueOfManDateData.trim().length() > 0) 
							{
																
								List<String> mandateDataList = parseLine(stringValueOfManDateData);
								    CallableStatement callableStatement = null;
								    String callableStatementSql = null;

								    callableStatementSql = "{CALL EPS.PROCEDURE_INSERT_COLLECTION_UPDATE_MANDATEPO_DATA(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";

									callableStatement = connectionDB2.prepareCall(callableStatementSql);
									//System.out.println("MIN "+mandateDataList.get(0).trim().concat("/000010"));
									callableStatement.setString(1,mandateDataList.get(0).trim().concat("/000010"));  
									callableStatement.setString(2, mandateDataList.get(1).trim());									
									callableStatement.setBigDecimal(3,new BigDecimal(mandateDataList.get(2).trim()));
									String collectionDate = mandateDataList.get(3).trim(); 
									callableStatement.setTimestamp(4,new java.sql.Timestamp(
											dateFormat.parse(collectionDate.substring(6,10) +"-"+ collectionDate.substring(3,5) +"-"+ 
													collectionDate.substring(0, 2)).getTime()));
									callableStatement.setString(5, mandateDataList.get(4).trim());
									callableStatement.setString(6,mandateDataList.get(5).trim());									
									callableStatement.setString(7,mandateDataList.get(6).trim());
									callableStatement.setString(8,mandateDataList.get(7).trim());
									callableStatement.registerOutParameter(9, java.sql.Types.VARCHAR);
									callableStatement.registerOutParameter(10, java.sql.Types.INTEGER);
									int collectionInserted = 0;
									
									//int collectionInserted=callableStatement.executeUpdate();
									
									if (collectionInserted > 0)
										{
											log.addElement(	this.timeStamp() +" :: "+callableStatement.getInt(10));
										}
									else
										{
											log.addElement(	this.timeStamp() +" :: "+callableStatement.getString(9));
										}
									if (callableStatement != null)
										callableStatement.close();
							}
						}
					}
					for (int i1 = 0; i1 < log.size(); i1++)
					{ 
						fout1.write(((String) log.elementAt(i1)).getBytes());
						fout1.write(System.getProperty("line.separator").getBytes());
					}
					//connectionDB2.commit();
					bjSchdUpdate(fileName,"S","");
					fout.close(); 
					fout1.close(); 
					if(scanFile!=null)
						 scanFile.close();  
					minFilesList[i].delete();
				}

				catch (IOException  exp) 
				{  
					try{
						 if(connectionDB2!=null)
							 connectionDB2.rollback(); 
				      }catch(SQLException se2){
				         se2.printStackTrace();
				      }
					bjSchdUpdate(fileName,"F",exp.getMessage());
				}
				finally{	  
				} 
			}
		if(connectionDB2!=null)
	        	connectionDB2.close();
		} catch (Exception  exp) {
			exp.printStackTrace(); 
		}finally{ 
			 
		} 

	}
	public static String bjSchdInsert(String fileName) throws SQLException, IOException
	{

		Connection connectionDB = null;
		connectionDB = getDBConnection();
		String message="";
		try
		{

			String genericSql = "";
			ResultSet resultSet = null;
			PreparedStatement preparedStatement = null; 
			File file = new File("D:" + fsep + "Shares" + fsep + "sap-nfs" + fsep + "sap-shares" + fsep + "SAPTEST" + fsep + "PRD" + fsep + "INBOUND" + fsep + "BJ"+ System.getProperty("file.separator") + "BJ_COLLECTION" + System.getProperty("file.separator") + fileName);
			InputStream fis = new FileInputStream(file);
			
			int fileCount=0;
			int bjSchdInserted = 0;
			genericSql = " SELECT COUNT(*) AS FILECOUNT FROM EPS.BJSCHEDULER WHERE FILENAME = ?  ";
			preparedStatement = connectionDB.prepareStatement(genericSql);
			preparedStatement.setString(1, fileName);
			resultSet = preparedStatement.executeQuery();

			if (resultSet != null)
			{
				while (resultSet.next())
				{
					fileCount = resultSet.getInt("FILECOUNT");
				}
			}

			if(fileCount>0)
			{

				genericSql =  " UPDATE EPS.BJSCHEDULER SET SCHEDULERDATA = ?	WHERE FILENAME = ? ";
				preparedStatement = connectionDB.prepareStatement(genericSql);
				preparedStatement.setAsciiStream(1,fis,(int)(file.length()));
				preparedStatement.setString(2, fileName);
				bjSchdInserted = preparedStatement.executeUpdate();
				message = "Successfully Updated data into BJSCHEDULER table";
			}
			else
			{											
				genericSql =  "INSERT INTO EPS.BJSCHEDULER (BJSCHEDULER_ID,FILENAME,SCHEDULERDATA,STATUS,CREATEDATE) " +
						      " VALUES (NEXTVAL FOR EPS.SEQUENCE_BJSCHEDULER_ID,?,?,?,CURRENT TIMESTAMP)";

				preparedStatement = connectionDB.prepareStatement(genericSql);
				preparedStatement.setString(1, fileName);
				preparedStatement.setAsciiStream(2,fis,(int)(file.length()));
				preparedStatement.setString(3, "I");
				bjSchdInserted = preparedStatement.executeUpdate(); 
				message = "Successfully Inserted data into BJSCHEDULER table";
			} 
			if (bjSchdInserted == 0)
			{
				message="Failed to Insert/Update data into BJSCHEDULER table";
			} 
			fis.close(); 
			return message;
		}
		catch (SQLException e) {
			throw new RuntimeException(e.getMessage());
		}

		finally{ 
			try{
		         if(connectionDB!=null)
		        	 connectionDB.close();
		      }catch(SQLException se2){
		         se2.printStackTrace();
		      } 
		}
	} 


	public static ArrayList<String> dirContents(String dir)
	{
		File myDir = new File(dir);
		ArrayList<String> fileNameList = new ArrayList<String>();
		File[] minFilesList = myDir.listFiles();
		
		for (int i = 0; i < minFilesList.length; i++)
		{
			File getFileList = minFilesList[i];
			if(!getFileList.isDirectory())
			{
				fileNameList.add(getFileList.getName());
			}	
		}
		return fileNameList;
	}

	private static List<String> parseLine(String stringValuesMandateFileData)
	{

		Scanner scanMandateDataFile = new Scanner(stringValuesMandateFileData);
		scanMandateDataFile.useDelimiter("[|]");
		List<String> mandateDataList = new ArrayList<String>();

		while(scanMandateDataFile.hasNext())
		{							 
			mandateDataList.add(scanMandateDataFile.next());
		}

		scanMandateDataFile.close();
		return mandateDataList;
	} 


	private static Connection getDBConnection() 
	{
		Connection connection = null;
		String userLoginId= "db2inst1";
		String userPassword= "db2inst1";
                //String userLoginId= "db2inst1";
		        //String userPassword= "michiko1@2014";
	      String url= "jdbc:db2://172.16.36.19:50000/RADB";
       // String url= "jdbc:derby:net://172.16.2.212:50000/RADB";

		try
		{
			//Class.forName("com.ibm.db2.jcc.DB2Driver");
			connection = DriverManager.getConnection(url, userLoginId, userPassword);
			return connection;
		} catch (SQLException exception) {
			System.err.println(exception);
		} catch (Exception excp) {
			System.err.println(excp);
		}
		return null;
	}


	public void bjSchdUpdate(String fileName,String status,String message) throws SQLException
	{
		Connection connectionDB1 = null;
		connectionDB1 = getDBConnection();
		PreparedStatement preparedStatement = null;
		try
		{ 
			String updateSchedularStatusSql =  " UPDATE EPS.BJSCHEDULER SET STATUS = ?, EXCEPTION = ?	" +
										 	   " WHERE FILENAME = ? ";
			preparedStatement = connectionDB1.prepareStatement(updateSchedularStatusSql);
			preparedStatement.setString(1, status);
			preparedStatement.setString(2, message);
			preparedStatement.setString(3, fileName);
			preparedStatement.executeUpdate();
		
		}
		catch (Exception e) 
		{
			throw new RuntimeException(e.getMessage());
		}

		finally
		{
			preparedStatement.close();
			connectionDB1.close();
		}
	}
	
	private String timeStamp()
	{
		Calendar cal = Calendar.getInstance();
		String mth = "";
		String date = "";
		String fileName = "";
		String todayDate = "";
		int day = cal.get(Calendar.DAY_OF_WEEK);
		int month = cal.get(Calendar.MONTH);
		int num = cal.get(Calendar.DAY_OF_MONTH);
		int yr = cal.get(Calendar.YEAR);
		int hr = cal.get(Calendar.HOUR_OF_DAY);
		int min = cal.get(Calendar.MINUTE);
		int sec = cal.get(Calendar.SECOND);
		int millisec = cal.get(Calendar.MILLISECOND);
		if (num <= 9)
		{
			date = "0" + String.valueOf(num);
		}
		else
		{
			date = String.valueOf(num);
		}
		if (month + 1 <= 9)
		{
			mth = "0" + String.valueOf(month + 1);
		}
		else
		{
			mth = String.valueOf(month + 1);
		}
		todayDate =
			yr + "-" + mth + "-" + date + " " + hr + ":" + min + ":" + sec;
		todayDate = " Timestamp :" + todayDate;
		return (todayDate);
	} // End of  timeStamp Method
	
	private boolean copyProcessedFile(String fileName) throws IOException
	{  
		FileInputStream Fread =new FileInputStream("D:" + fsep + "Shares" + fsep + "sap-nfs" + fsep + "sap-shares" + fsep + "SAPTEST" + fsep + "PRD" + fsep + "INBOUND" + fsep + "BJ"+ fsep + "BJ_COLLECTION" + fsep +fileName); 
        FileOutputStream Fwrite=new FileOutputStream("D:" + fsep + "Shares" + fsep + "sap-nfs" + fsep + "sap-shares" + fsep + "SAPTEST" + fsep + "PRD" + fsep + "INBOUND" + fsep + "BJ"+ fsep + "BJ_COLLECTION_PROCESSED" + fsep +fileName) ;
        File origFile =  new File("D:" + fsep + "Shares" + fsep + "sap-nfs" + fsep + "sap-shares" + fsep + "SAPTEST" + fsep + "PRD" + fsep + "INBOUND" + fsep + "BJ"+ fsep + "BJ_COLLECTION" + fsep + fileName);
        boolean tFlag = true;
        int c; 
        try {
			while((c=Fread.read())!=-1) 
			Fwrite.write((char)c);
			
		} catch (IOException e) { 
			e.printStackTrace();
		} 
        
        Fread.close(); 
        Fwrite.close();  
       
       return tFlag;  
	}
	
	/*private void removeProcessedFile() {
		List sourceFileList = dirContents("E:"+ fsep + "BJ_COLLECTION");
		List destFileList = dirContents("E:"+ fsep + "BJ_COLLECTION_PROCESSED");
		int counter = 0; 
		File fileToBeDeleted= null;
		for (int i= 0;i<sourceFileList.size();i++){
			if(destFileList.contains(sourceFileList.get(i)))
				counter++;	 
		} 
		if (counter == sourceFileList.size()){
			for(File file: new File("E:"+ fsep + "BJ_COLLECTION").listFiles()) 
				if(file.isFile())
					System.out.println(file.delete());
		}
	}
	
	private void deleteProcessedFile(){ 
		Scanner scanFile = null;
		FileOutputStream fout = null;
		try{
			File sourceDir = new File("E:"+ fsep + "BJ_COLLECTION");
			File[] minFilesList = sourceDir.listFiles();
			for (int i= 0;i<minFilesList.length;i++)
			{
				try{
					fout = new FileOutputStream("E:"+ fsep + "BJ_COLLECTION_PROCESSED"+ fsep + minFilesList[i].getName(),true);
					scanFile = new Scanner( minFilesList[i]);
					while(scanFile.hasNextLine())
					{
						String stringValueOfManDateData = scanFile.nextLine();
						fout.write(stringValueOfManDateData.getBytes());
						fout.write(fsep.getBytes());						
					}
					fout.close(); 
					scanFile.close();	
					if (minFilesList[i].exists())
							minFilesList[i].delete();
						
					
				}catch (IOException  exp) {
					exp.fillInStackTrace();
				}
			}			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	*/
	 
}
