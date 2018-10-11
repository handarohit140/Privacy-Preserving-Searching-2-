import java.io.*;
import java.security.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.net.*;
import java.util.Random;

class Orencik_search
{

public static void main(String args[])throws Exception
{
	Orencik_search_detail ob=new Orencik_search_detail();
	long xx=0;
	Class.forName("com.mysql.jdbc.Driver");
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/Conjunctive_Bucket","root","111111");
	Statement stmt=con.createStatement();
	String q1="select * from `query_k5_19043`";
	ResultSet rs=stmt.executeQuery(q1);
	while(rs.next())
	{
		String keyword=rs.getString("index");
	
		String bid=keyword.substring(448,450);
		keyword=keyword.substring(0,448);
	
		//System.out.println(bid+"\n"+keyword);
		xx=ob.search(keyword,bid);
		
	}
System.out.println(xx/200.0);

}
}

class Orencik_search_detail
{
public long startTime1=0,endTime1=0,xtime;
public long search(String keyword_index,String bid)throws Exception
{
	Class.forName("com.mysql.jdbc.Driver");
	Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/Conjunctive_Bucket","root","111111");
	Statement stmt1=con1.createStatement();
	String q1="select * from `Doc_Index` where `Sr`<='19043'";
	int res=0;
	String bbb="",did="",index="";
	ResultSet rs1=stmt1.executeQuery(q1);
	while(rs1.next())
	{
		did=rs1.getString("DID");	
		index=rs1.getString("DIndex");
		res=compare_index(keyword_index,index);
		xtime+=endTime1-startTime1;
		if(res==1)
		{
			bbb+=did+"\t";
		}
	}

	Class.forName("com.mysql.jdbc.Driver");
	Connection con2=DriverManager.getConnection("jdbc:mysql://localhost:3306/Cengiz_Search","root","111111");
	Statement stmt2=con2.createStatement();

	String queryy="insert into `query_k5_19043`(`time`,`DID`) value ('"+xtime+"','"+bbb+"')";
	stmt2.executeUpdate(queryy);
return xtime;
}

public int compare_index(String input,String index)
{
	int i,like=0,like1=0;
	char array1[]=new char[448];
	char array2[]=new char[448];
	array1=input.toCharArray();
	array2=index.toCharArray();
	startTime1 = System.currentTimeMillis();
	for(i=0;i<448;i++)
	{
		if(array1[i]=='0')
		{
			like++;
			if(array2[i]=='0')
				like1++;
			else
				break;
		}
	}
	endTime1 = System.currentTimeMillis();			
	xtime+=endTime1-startTime1;
	
	if(like==like1)
		return 1;
	else
		return 0;
}
}
