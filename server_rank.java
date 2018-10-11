import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

class server_rank
{
public long time=0, xtime=0, startTime1=0,endTime1=0,startTime2=0,endTime2=0,xtime2=0,startTime3=0,endTime3=0,xtime3=0;
TreeMap tm=new TreeMap();
TreeMap rm=new TreeMap();
Set set=tm.entrySet();
Set set1=tm.entrySet();
Set set2=rm.entrySet();
Set set3=rm.entrySet();

Iterator i;
Iterator i1;
Iterator i2;
Iterator i3;

byte b[];
DatagramPacket dp;
DatagramSocket ds;
String str,bbb="";
int sr=1;

	
public int compare_index(String input,String index)
{
	int i,like=0,like1=0;
	char array1[]=new char[384];
	char array2[]=new char[384];
	array1=input.toCharArray();
	array2=index.toCharArray();
	startTime1 = System.currentTimeMillis();
	for(i=0;i<384;i++)
	{
		if(array2[i]=='0')
		{
			like++;
			if(array1[i]=='0')
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

public void input(String block)throws Exception
{
	Class.forName("com.mysql.jdbc.Driver");
	int iii=1;
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/dks_test","root","");
	Statement stmt=con.createStatement();
	String queryy="select * from `rank_server_50_relnew` where Block='"+block+"'";
	stmt.executeQuery(queryy);
	ResultSet rs3=stmt.executeQuery(queryy);
	startTime2 = System.currentTimeMillis();
	while(rs3.next())
	{	
		for(iii=1;iii<=8;iii++)
		{
			Double value1=1.0;
			String D="D"+iii;
			String V="V"+iii;
			String Doc=rs3.getString(D);
			Double val=rs3.getDouble(V);
			if(tm.containsKey(Doc))
			{
				Double value= ((Double)tm.get(Doc)).doubleValue();
				tm.put(Doc,val+value);
				Double value11= ((Double)rm.get(Doc)).doubleValue();
				rm.put(Doc,value11+value1);
			}
			else
			{
				tm.put(Doc,val);		
				rm.put(Doc,value1);		
			}
		}
	}
	endTime2 = System.currentTimeMillis();
	xtime2+=endTime2-startTime2;
}
	
public void search(String input)throws Exception
{
	int comp=0,flag=0;
	String doc_result="";
	int k=0;
	String index="",ss="",query2="";
	int result;
	Class.forName("com.mysql.jdbc.Driver");
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/dks_test","root","");
	Statement stmt=con.createStatement();
	Statement stmt1=con.createStatement();
	String query="select * from index_test_d3";
	ResultSet rs=stmt.executeQuery(query);

	int j;
	bbb="";
	while(rs.next())
	{
		index=rs.getString("Keyword");
		ss=rs.getString("Block");

		result=compare_index(input,index);

		if(result==1)
		{
			input(ss);
			bbb+=ss+"\t";

		}
			
	}

}

public void disp()throws Exception
{
	int count=0;
	i = set.iterator();
	startTime3 = System.currentTimeMillis();
	while(i.hasNext()) 
	{
		Map.Entry me = (Map.Entry)i.next();
	
		if(((double)me.getValue())>0.0)
		{
			count++;
		}
	}
	String sort_array[][]=new String[count][3];
	set1=tm.entrySet();
	i1 = set1.iterator();
	int uu=0;
	while(i1.hasNext()) 
	{		
		Map.Entry me1 = (Map.Entry)i1.next();
		if(((double)me1.getValue())>0.0)
		{
			sort_array[uu][0]=String.valueOf(me1.getKey());
			String tt=sort_array[uu][0];
			sort_array[uu][1]=String.valueOf(me1.getValue());
			if(rm.containsKey(tt))
			{
				sort_array[uu][2]=String.valueOf(((Double)rm.get(tt)).doubleValue());
			}
			uu++;
		}
	}

	for(int xx=0;xx<count;xx++)
	{
		for(int yy=0;yy<count-xx-1;yy++)
		{
			if(Double.parseDouble(sort_array[yy][2])>Double.parseDouble(sort_array[yy+1][2]))
			{
				String temp=sort_array[yy][0];
				String vv=sort_array[yy][1];
				String ww=sort_array[yy][2];
				sort_array[yy][0]=sort_array[yy+1][0];
				sort_array[yy][1]=sort_array[yy+1][1];
				sort_array[yy][2]=sort_array[yy+1][2];
				sort_array[yy+1][0]=temp;
				sort_array[yy+1][1]=vv;
				sort_array[yy+1][2]=ww;
			}
		}
	}

	for(int xx=0;xx<count;xx++)
	{
		for(int yy=0;yy<count-xx-1;yy++)
		{
			if((Double.parseDouble(sort_array[yy][2])==Double.parseDouble(sort_array[yy+1][2]))&&(Double.parseDouble(sort_array[yy][1])>Double.parseDouble(sort_array[yy+1][1])))
			{
				String temp=sort_array[yy][0];
				String vv=sort_array[yy][1];
				String ww=sort_array[yy][2];
				sort_array[yy][0]=sort_array[yy+1][0];
				sort_array[yy][1]=sort_array[yy+1][1];
				sort_array[yy][2]=sort_array[yy+1][2];
				sort_array[yy+1][0]=temp;
				sort_array[yy+1][1]=vv;
				sort_array[yy+1][2]=ww;
			}
		}
	}
String ff="Document Name (Relevance, Count)\n";
		for(int xx=count-1;xx>=0;xx--)
		{	
			//System.out.println(sort_array[xx][0]+"\t"+sort_array[xx][1]+"\t"+sort_array[xx][2]);
			ff=ff+sort_array[xx][0]+"("+sort_array[xx][1]+","+sort_array[xx][2]+")\t";
		}
		endTime3 = System.currentTimeMillis();
		//System.out.println(startTime2+"\t"+endTime2+"\t"+startTime3+"\t"+endTime3);
		xtime3=endTime3-startTime3;
		long aaa=xtime3+xtime2;
		//System.out.println("Rank Time="+(aaa));
//		System.out.println("Search Time="+xtime);
		Class.forName("com.mysql.jdbc.Driver");
		Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/dks_test","root","");
		Statement stmt1=con1.createStatement();
		String query2="insert into `server_result_keyword5_10`(`Sr`,`block`,`stime`,`rtime`,`document`) value ('"+sr+"','"+bbb+"','"+xtime+"','"+aaa+"','"+ff+"')";
		xtime3=0;xtime2=0;xtime=0;
		stmt1.executeUpdate(query2);
//		System.out.println(query2);
		sr++;
		tm.clear();
		rm.clear();
	}	


	public static void main(String args[])throws Exception
	{
		String doc_result="";
		server_rank ob=new server_rank();
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
			Connection con11=DriverManager.getConnection("jdbc:mysql://localhost:3306/dks_test","root","");
			Statement stmt11=con11.createStatement();
		
			String query11="select * from `result_keyword5_10`";
			ResultSet rs11=stmt11.executeQuery(query11);
			while(rs11.next())
			{
				//Thread.sleep(1000);
				String str=rs11.getString("index");
				ob.search(str);
				
				ob.disp();
				
			}
		}catch(Exception e){System.out.println("Exception="+e);}
	}
}
