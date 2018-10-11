import java.io.*;
import java.security.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.net.*;
import java.util.Random;

class CKS_search_bid5
{

public static void main(String args[])throws Exception
{
	int db_size=Integer.parseInt(args[0]);
	CKS_search_detail_bid2 ob=new CKS_search_detail_bid2();
	Class.forName("com.mysql.jdbc.Driver");
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/Conjunctive_Bucket","root","111111");
	Statement stmt=con.createStatement();
	String q1="select * from `query_k1_"+db_size+"`";
	ResultSet rs=stmt.executeQuery(q1);
	while(rs.next())
	{
		String keyword=rs.getString("index");
	
		String bid1=keyword.substring(448,450);
		String bid2=keyword.substring(450,452);
		keyword=keyword.substring(0,448);
	
		//System.out.println(bid1+"\t"+bid2+"\n"+keyword);
		ob.search(keyword,bid1,bid2);
	}

}
}

class CKS_search_detail_bid5
{
public long startTime1=0,endTime1=0,xtime=0,count=0;
public void search(String keyword_index,String bid1,String bid2)throws Exception
{
	count=0;
	startTime1=0;
	endTime1=0;
	xtime=0;
	int bid3=Integer.parseInt(bid1);
	int bid4=Integer.parseInt(bid2);
	Class.forName("com.mysql.jdbc.Driver");
	Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/cks","root","");
	Statement stmt1=con1.createStatement();
	Statement stmt2=con1.createStatement();
	String q1="select * from `cks_index6000` where `BID`='"+bid3+"' order by `DID`";
	String q2="select * from `cks_index6000` where `BID`='"+bid4+"' order by `DID`";
	//System.out.println(q1+"\n"+q2);
	int res=0;
	String bbb="",did1="",did2="",index="";
	ResultSet rs1=stmt1.executeQuery(q1);
	ResultSet rs2=stmt2.executeQuery(q2);
	boolean keepGoing = true;
	rs1.last();rs2.last();
	int l1=rs1.getRow();
	int l2=rs2.getRow();
	rs1.beforeFirst();
	rs2.beforeFirst();
	//System.out.println(l1+"\t"+l2);
	rs1.next(); rs2.next();
	int i=1,j=1;
	int iterate=0;
	while(i<=l1&&j<=l2)
	{
		did1=rs1.getString("DID");
		did2=rs2.getString("DID");
		String did3=did1.substring(1);
		String did4=did2.substring(1);
		//System.out.println(did3+"\t"+did4);

		startTime1 = System.currentTimeMillis();
		if(Integer.parseInt(did3)==Integer.parseInt(did4))
		{
			
			index=rs1.getString("Doc_Index");
			res=compare_index(keyword_index,index);
			count++;
			xtime+=endTime1-startTime1;
			if(res==1)
			{
				//System.out.println(did3+"\t"+did4);
				bbb+=did1+"\t";
			}
			rs1.next(); rs2.next();i++;j++;
		}
		else if(Integer.parseInt(did3)<Integer.parseInt(did4))
			{rs1.next();i++;}
		else
			{rs2.next();j++;}
	}
	endTime1 = System.currentTimeMillis();	
	xtime+=endTime1-startTime1;
	String queryy="insert into `result_search_k5_2bid`(`BID`,`time`,`DID`,`Comparison`) value ('"+bid3+"','"+xtime+"','"+bbb+"','"+count+"')";
	//System.out.println(queryy);
	stmt1.executeUpdate(queryy);
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
