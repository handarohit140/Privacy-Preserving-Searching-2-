import java.io.*;
import java.security.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.net.*;

class PTSearch
{
public static void main(String args[])throws Exception
{
Search ob=new Search();
ob.find();
}
}

class Search
{
public void find()throws Exception
{
	
	Class.forName("com.mysql.jdbc.Driver");
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/Conjunctive_Bucket","root","111111");
	Statement stmt=con.createStatement();
	Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/PT_Search","root","111111");
	Statement stmt1=con.createStatement();
	Statement stmt2=con1.createStatement();
	String query="select * from `Q19043`";
	ResultSet rs=stmt.executeQuery(query);
	int k=0;
	while(rs.next())
	{
		int flag1=0,flag2=0,flag3=0,flag4=0,flag5=0;
		String fres="";
		String db_did="";
		String keyword1=rs.getString("Keyword1");
		//String keyword2=rs.getString("Keyword2");
		//String keyword3=rs.getString("Keyword3");
		//String keyword4=rs.getString("Keyword4");
		//String keyword5=rs.getString("Keyword5");
		String query1="select * from `table19000` where `Sr`<='19043'";
		ResultSet rs1=stmt1.executeQuery(query1);
		while(rs1.next())
		{	flag1=0;flag2=0;flag3=0;flag4=0;flag5=0;
			k=0;
			for(int i=1;i<=35;i++)
			{	
				
				String check="Keyword"+i;
				String db_keyword=rs1.getString(check);	
				db_did=rs1.getString("DID");
				
				if(keyword1.equals(db_keyword))
				flag1=1;
				//if(keyword2.equals(db_keyword))
				//flag2=1;
				//if(keyword3.equals(db_keyword))
				//flag3=1;
				//if(keyword4.equals(db_keyword))
				//flag4=1;
				//if(keyword5.equals(db_keyword))
				//flag5=1;
			
			}
			//if(flag1==1&&flag2==1&&flag3==1&&flag4==1&&flag5==1) fres=fres+"\t"+db_did;
			//if(flag1==1&&flag2==1&&flag3==1&&flag4==1) fres=fres+"\t"+db_did;
			//if(flag1==1&&flag2==1&&flag3==1) fres=fres+"\t"+db_did;
			//if(flag1==1&&flag2==1) fres=fres+"\t"+db_did;
			if(flag1==1) fres=fres+"\t"+db_did;
		}
		//if(!(fres.equals("Result-")))
		{	
			//System.out.println(fres);
			String query2="insert into `query_k1_19043`(`DID`,`time`) values('"+fres+"','');";
			stmt2.executeUpdate(query2);
		}
	}
}
}
