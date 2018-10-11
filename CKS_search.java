import java.io.*;
import java.security.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.net.*;
import java.util.Random;

class CKS_search
{
public static void main(String args[])throws Exception
{
	CKS_search_detail ob=new CKS_search_detail();
	Class.forName("com.mysql.jdbc.Driver");
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/Test_Rank","root","111111");
	Statement stmt=con.createStatement();
	String q1="select * from `query_k2_19043`";
	ResultSet rs=stmt.executeQuery(q1);
	while(rs.next())
	{
		String keyword=rs.getString("index");
	
		String bid=keyword.substring(448,450);
		keyword=keyword.substring(0,448);
	
		//System.out.println(bid+"\n"+keyword);
		ob.search(keyword,bid);
	}

}
}

class CKS_search_detail
{
public long startTime1=0,endTime1=0,xtime,count,srr=0;
public void search(String keyword_index,String bid)throws Exception
{
	int bid1=Integer.parseInt(bid);
	Class.forName("com.mysql.jdbc.Driver");
	Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/Conjunctive_Bucket","root","111111");
	Statement stmt1=con1.createStatement();
	Statement stmt12=con1.createStatement();
	String q1="select * from `Bucket_Map` where `BID`='"+bid1+"' and `DID_1`<='2500'";
	int res=0;
	String bbb="",did="",index="";
	ResultSet rs1=stmt1.executeQuery(q1);
	while(rs1.next())
	{
		did=rs1.getString("DID");
		index=rs1.getString("DIndex");
		//String q12="select `DIndex` from `Doc_Index` where `DID`='"+did+"'";
		//ResultSet rs12=stmt12.executeQuery(q12);
		//rs12.next();
		//index=rs12.getString("DIndex");
		res=compare_index(keyword_index,index);
		count++;
		xtime+=endTime1-startTime1;
		if(res==1)
		{
			bbb+=did+"\t";
		}
	}

	Connection con13=DriverManager.getConnection("jdbc:mysql://localhost:3306/Handa_Search","root","111111");
	Statement stmt13=con13.createStatement();
	srr++;
	System.out.print(srr+"\t");
	String queryy1="insert into `query_k5_2500`(`time`,`DID`,`Comparison`) value ('"+xtime+"','"+bbb+"','"+count+"')";
	stmt13.executeUpdate(queryy1);

	if(srr==200)System.out.println("\n"+xtime+"---"+count);
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
