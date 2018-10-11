import java.io.*;
import java.security.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.net.*;
import java.util.Random;


class CKS_search_bid3
{

public static void main(String args[])throws Exception
{
	int db_size=Integer.parseInt(args[0]);
	CKS_search_detail_bid3 ob=new CKS_search_detail_bid3();
	Class.forName("com.mysql.jdbc.Driver");
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/Test_Map","root","111111");
	Statement stmt=con.createStatement();
	String q1="select * from `query_k2_"+db_size+"`";
	ResultSet rs=stmt.executeQuery(q1);
	while(rs.next())
	{
		String keyword=rs.getString("index");
	
		String bid1=keyword.substring(448,450);
		String bid2=keyword.substring(450,452);
		//String bid3=keyword.substring(452,454);
		//String bid2="98";
		String bid3="98";
		//String bid3="12";
		keyword=keyword.substring(0,448);
	
		ob.search(keyword,bid1,bid2,bid3,db_size);
	}

}
}

class CKS_search_detail_bid3
{
public long startTime1=0,endTime1=0,xtime,count,srr=0;
public void search(String keyword_index,String bid1,String bid2,String bid31,int db_size)throws Exception
{
	int bid3=Integer.parseInt(bid1);
	int bid4=Integer.parseInt(bid2);
	int bid5=Integer.parseInt(bid31);
	Class.forName("com.mysql.jdbc.Driver");
	Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/Conjunctive_Bucket","root","111111");
	Statement stmt1=con1.createStatement();
	Statement stmt2=con1.createStatement();
	Statement stmt3=con1.createStatement();
	String table1="Bucket_Map_"+bid3;
	String table2="Bucket_Map_"+bid4;
	String table3="Bucket_Map_"+bid5;
	String q1="select * from "+table1+" where `DID_1`<='"+db_size+"' order by `DID_1`";
	String q2="select * from "+table2+" where `DID_1`<='"+db_size+"' order by `DID_1`";
	String q3="select * from "+table3+" where `DID_1`<='"+db_size+"' order by `DID_1`";
	//System.out.println(q1+"\n"+q2);
	int res=0;
	String bbb="",did1="",did2="",index="",did31="";
	ResultSet rs1=stmt1.executeQuery(q1);
	ResultSet rs2=stmt2.executeQuery(q2);
	ResultSet rs3=stmt3.executeQuery(q3);
	boolean keepGoing = true;
	rs1.last();rs2.last();rs3.last();
	int l1=rs1.getRow();
	int l2=rs2.getRow();
	int l3=rs3.getRow();
	rs1.beforeFirst();
	rs2.beforeFirst();
	rs3.beforeFirst();

	rs1.next(); rs2.next();rs3.next();
	int i=1,j=1,k=1;
	int iterate=0;
	while(i<=l1&&j<=l2&&k<=l3)
	{
		did1=rs1.getString("DID");
		did2=rs2.getString("DID");
		did31=rs3.getString("DID");
		String did3=did1.substring(1);
		String did4=did2.substring(1);
		String did5=did31.substring(1);


		startTime1 = System.currentTimeMillis();
		if(Integer.parseInt(did3)==Integer.parseInt(did4)&&Integer.parseInt(did4)==Integer.parseInt(did5))
		{
			index=rs1.getString("DIndex");
			res=compare_index(keyword_index,index);
			count++;
			xtime+=endTime1-startTime1;
			if(res==1)
			{
				bbb+=did1+"\t";
			}
			rs1.next(); rs2.next();rs3.next();
			i++;j++;k++;
		}
		else if(Integer.parseInt(did3)<Integer.parseInt(did4)||Integer.parseInt(did3)<Integer.parseInt(did5))
			{rs1.next();i++;}
		else if(Integer.parseInt(did4)<Integer.parseInt(did3)||Integer.parseInt(did4)<Integer.parseInt(did5))
			{rs2.next();j++;}
		else
			{rs3.next();k++;}
	}
	endTime1 = System.currentTimeMillis();	
	xtime+=endTime1-startTime1;
	Connection con13=DriverManager.getConnection("jdbc:mysql://localhost:3306/Test_Map","root","111111");
	Statement stmt13=con13.createStatement();
	srr++;
	String queryy1="insert into `handa_query_k2_b3_"+db_size+"`(`time`,`DID`,`Comparison`) value ('"+xtime+"','"+bbb+"','"+count+"')";
	stmt13.executeUpdate(queryy1);
	if(srr==200)System.out.println(xtime+"---"+count);
	con1.close();
	con13.close();
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
