import java.io.*;
import java.security.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.*;
import java.util.*;
import java.net.*;

class CKS_level_index_gen
{
	public static void main(String args[])
	{
		try
		{
			downer_level ob=new downer_level();
			ob.cal_index();
		}catch(Exception e){System.out.println(e.toString());}
	}
}

class downer_level
{
public long startTime1 =0, endTime=0,x=0;

String rindex="1100110011100111000000101011001001111101110101011010111101111000110001111111101111101100011011101101111110110111111110111110101001111101101111101111111101100111001110110101101111111111101111101100110011100111000000101011001001111101110101011010111101111000110001111111101111101100011011101101111110110111111110111110101001111101101111101111111101100111001110110101101111111111101111101000100100011110011001101110110111111111011010110111110111111001";


public void cal_index()throws Exception
{
	String did="",c1="",final_keyword="",final_bid="";
	    		
	Class.forName("com.mysql.jdbc.Driver");
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/preprocess","root","111111");
	Statement stmt=con.createStatement();
	Statement stmt1=con.createStatement();

	Connection con111=DriverManager.getConnection("jdbc:mysql://localhost:3306/level_cengiz","root","111111");
	Statement stmt111=con111.createStatement();

	String cid="",keyword="",keyword1="";
			
	String query="select * from `doc_level` where `Doc_ID`<=200";
	ResultSet rs=stmt.executeQuery(query);
	while(rs.next())
	{
		x=0;
		did=rs.getString("Doc_ID");
		String query1="insert into `doc_level`(`Doc_ID`) values('"+did+"')";
		stmt111.executeUpdate(query1);
		String final_index="";
		final_keyword="";
		final_bid="";
		for(int i=1;i<=17;i++)
		{
			final_index="";
			final_keyword="";

			keyword1="L"+i;
			keyword=rs.getString(keyword1);
			if(keyword.isEmpty())
			{	
				startTime1 = System.currentTimeMillis();		final_keyword="1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";	
				\\query1="update `doc_level` set `"+ keyword1+"`='"+final_keyword+"' where `Doc_ID`='"+did+"'";
				\\stmt111.executeUpdate(query1);

				endTime = System.currentTimeMillis();	
				x=x+endTime-startTime1;
				\\System.out.println(x);
			if(i==5)
			{
			\\query1="update `doc_level` set `time1`='"+x+"' where `Doc_ID`='"+did+"'";
			\\stmt111.executeUpdate(query1);
			System.out.println(x);
			}
			if(i==10)
			{
			\\query1="update `doc_level` set `time2`='"+x+"' where `Doc_ID`='"+did+"'";
			\\stmt111.executeUpdate(query1);			
			System.out.print("\t"+x);
			}
			if(i==15)
			{
			\\query1="update `doc_level` set `time3`='"+x+"' where `Doc_ID`='"+did+"'";
			\\stmt111.executeUpdate(query1);			
			System.out.print("\t"+x);
			}
			if(i==17)
			{
			\\query1="update `doc_level` set `time4`='"+x+"' where `Doc_ID`='"+did+"'";
			\\stmt111.executeUpdate(query1);			
			System.out.print("\t"+x);
			} 
				continue;
			}
			startTime1 = System.currentTimeMillis();
			String[] key_array=keyword.split("\\,");
			endTime = System.currentTimeMillis();	
			x=x+endTime-startTime1;
			for(int cc=0;cc<key_array.length;cc++)
			{
				keyword=key_array[cc];
				String bucketid=MD5(keyword);
				int bid=cal_bucketid(bucketid);
				String key=getKey(bid);	
				String index=hash_calculate(keyword,key);
				String bindex=gen_index(index);	
				final_index=gen_final_index(bindex);
				if(cc==0)
				{
					final_keyword+=final_index;
					continue;
				}
				startTime1 = System.currentTimeMillis();
				final_keyword=calculate(final_index,final_keyword);
				endTime = System.currentTimeMillis();	
				x=x+endTime-startTime1;
			}

			final_keyword=calculate(final_keyword,rindex);
			query1="update `doc_level` set `"+ keyword1+"`='"+final_keyword+"' where `Doc_ID`='"+did+"'";
			stmt111.executeUpdate(query1);
			key_array=null;
			System.out.println(x);
			if(i==5)
			{
			\\query1="update `doc_level` set `time1`='"+x+"' where `Doc_ID`='"+did+"'";
			\\stmt111.executeUpdate(query1);
			System.out.println(x);
			}
			if(i==10)
			{
			\\query1="update `doc_level` set `time2`='"+x+"' where `Doc_ID`='"+did+"'";
			\\stmt111.executeUpdate(query1);			System.out.println(query1);
			System.out.print("\t"+x);
			}
			if(i==15)
			{
			\\query1="update `doc_level` set `time3`='"+x+"' where `Doc_ID`='"+did+"'";
			\\stmt111.executeUpdate(query1);			System.out.println(query1);
			System.out.print("\t"+x);
			}
			if(i==17)
			{
			\\query1="update `doc_level` set `time4`='"+x+"' where `Doc_ID`='"+did+"'";
			\\stmt111.executeUpdate(query1);			System.out.println(query1);
			System.out.print("\t"+x);
			} 
		}
	}
}




public String MD5(String passwordToHash)throws Exception
{
	String generatedPassword = null;
	MessageDigest md = MessageDigest.getInstance("MD5");
	md.update(passwordToHash.getBytes());
	byte[] bytes = md.digest();
	StringBuilder sb = new StringBuilder();
startTime1 = System.currentTimeMillis();
	for(int i=0; i< bytes.length ;i++)
	{
		sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
	}
	generatedPassword = sb.toString();

endTime = System.currentTimeMillis();	
x=x+endTime-startTime1;
	String key=generatedPassword.substring(30,32);
	return key;				
}

public int cal_sum_bid(String str)
{
int h=0,c=7;
for(int i=0;i<str.length();i++)
{
char ccc=str.charAt(i);
h=(c*h+ccc)%97;
}
return h;
}


public int cal_bucketid(String str)
{
	int b=0,mul=1,sum=0;
	char a[]=new char[640];
	a=str.toCharArray();
startTime1 = System.currentTimeMillis();
	for(int i=1;i>=0;i--)
	{
		switch(a[i])
		{
			case '0':b=0; break;
			case '1':b=1; break;
			case '2':b=2; break;
			case '3':b=3; break;
			case '4':b=4; break;
			case '5':b=5; break;
			case '6':b=6; break;
			case '7':b=7; break;
			case '8':b=8; break;
			case '9':b=9; break;
			case 'a':b=10; break;
			case 'b':b=11; break;
			case 'c':b=12; break;
			case 'd':b=13; break;
			case 'e':b=14; break;
			case 'f':b=15; break;
		}
		sum=sum+b*mul;
		mul=16*mul;
	}

endTime = System.currentTimeMillis();	
x=x+endTime-startTime1;
	return(sum%100);
}

public String getKey(int bid)throws Exception
{
    	String secret="";
	Class.forName("com.mysql.jdbc.Driver");
	Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/Conjunctive_Bucket","root","111111");
	Statement stmt=con.createStatement();
			
	String query="select * from `bucket` where `Bucket_ID`="+bid;
	ResultSet rs=stmt.executeQuery(query);
	while(rs.next())
	{
startTime1 = System.currentTimeMillis();
		secret=rs.getString("Secret_Key");
endTime = System.currentTimeMillis();	
x=x+endTime-startTime1;
		break;
	}
con.close();
	return secret;
}

public String hash_calculate(String passwordToHash,String salt)throws Exception
{
	String securePassword = get_SHA_256_SecurePassword(passwordToHash, salt);
 	String securePassword1 = get_SHA_384_SecurePassword(passwordToHash, salt);
      	String securePassword2 = get_SHA_512_SecurePassword(passwordToHash, salt);
startTime1 = System.currentTimeMillis();
	String Index=securePassword+securePassword1+securePassword2+securePassword+securePassword1+securePassword2+securePassword1;
endTime = System.currentTimeMillis();	
x=x+endTime-startTime1;

	return Index;
}	


private static String get_SHA_256_SecurePassword(String passwordToHash, String salt)
{
	String generatedPassword = null;
	try 
	{
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(salt.getBytes());
		byte[] bytes = md.digest(passwordToHash.getBytes());
		StringBuilder sb = new StringBuilder();
		for(int i=0; i< bytes.length ;i++)
		{
			sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		generatedPassword = sb.toString();
	}
	catch (NoSuchAlgorithmException e)
	{
		e.printStackTrace();
	}
	return generatedPassword;
}
     
private static String get_SHA_384_SecurePassword(String passwordToHash, String salt)
{
	String generatedPassword = null;
	try 
	{
		MessageDigest md = MessageDigest.getInstance("SHA-384");
		md.update(salt.getBytes());
		byte[] bytes = md.digest(passwordToHash.getBytes());
		StringBuilder sb = new StringBuilder();
		for(int i=0; i< bytes.length ;i++)
		{
			sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
		}
	generatedPassword = sb.toString();
	}
        	catch (NoSuchAlgorithmException e)
        	{
            		e.printStackTrace();
        	}
	return generatedPassword;
}
     
private static String get_SHA_512_SecurePassword(String passwordToHash, String salt)
{
	String generatedPassword = null;
        	try 
	{
            		MessageDigest md = MessageDigest.getInstance("SHA-512");
		md.update(salt.getBytes());
            		byte[] bytes = md.digest(passwordToHash.getBytes());
            		StringBuilder sb = new StringBuilder();
            		for(int i=0; i< bytes.length ;i++)
            		{
                		sb.append(Integer.toString((bytes[i] & 0xff) + 0x100, 16).substring(1));
            		}
            		generatedPassword = sb.toString();
        	}
        	catch (NoSuchAlgorithmException e)
        	{
            		e.printStackTrace();
        	}
        	return generatedPassword;
}

public String gen_index(String str)
{
	int len=str.length();
	Vector vec=new Vector(len);
	String b="";
	char a[]=new char[672];
	a=str.toCharArray();
startTime1 = System.currentTimeMillis();
	for(int i=0;i<len;i++)
	{
		switch(a[i])
		{
			case '0':b="0000"; break;
			case '1':b="0001"; break;
			case '2':b="0010"; break;
			case '3':b="0011"; break;
			case '4':b="0100"; break;
			case '5':b="0101"; break;
			case '6':b="0110"; break;
			case '7':b="0111"; break;
			case '8':b="1000"; break;
			case '9':b="1001"; break;
			case 'a':b="1010"; break;
			case 'b':b="1011"; break;
			case 'c':b="1100"; break;
			case 'd':b="1101"; break;
			case 'e':b="1110"; break;
			case 'f':b="1111"; break;
		}
		vec.addElement(b);
	}
	String res="";
	for(int i=0;i<len;i++)
      	{
	        res=res+vec.get(i);
      	}
endTime = System.currentTimeMillis();	
x=x+endTime-startTime1;
	return res;
}
      	
public String gen_final_index(String str)
{
	int j=0;
	int length=str.length();
	char array[]=new char[length];
	char array1[]=new char[448];
	array=str.toCharArray();
startTime1 = System.currentTimeMillis();
	for(int i=0;i<2688;i=i+6)
	{
		if((array[i]=='0')&&(array[i+1]=='0')&&(array[i+2]=='0')&&(array[i+3]=='0')&&(array[i+4]=='0')&&(array[i+5]=='0'))
		{
			array1[j]='0';
		}
		else
		{
			array1[j]='1';
		}
		j++;
	}
endTime = System.currentTimeMillis();	
x=x+endTime-startTime1;
	String newString2 = String.valueOf(array1);
	return newString2;
}

public String calculate(String final_index,String final_keyword)
{
	char array[]=new char[448];
	char array1[]=new char[448];
	array=final_index.toCharArray();
	array1=final_keyword.toCharArray();
	char array3[]=new char[448];
startTime1 = System.currentTimeMillis();
	for(int i=0;i<448;i++)
	{
		if(array1[i]=='0'||array[i]=='0')
			array3[i]='0';
		else
			array3[i]='1';
	}
endTime = System.currentTimeMillis();	
x=x+endTime-startTime1;
	String newString2 = String.valueOf(array3);
	return newString2;
}
}
