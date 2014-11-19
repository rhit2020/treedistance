import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;



public class DB {
//	private Connection labstudyConn;
//	private boolean isConnLabstudyValid;
	
	private Connection webex21Conn;
	private boolean isWebex21ConnValid;
	
	public void connectToWebex21()
	{
		  String url = api.Constants.DB.WEBEX_URL;
		  String driver = api.Constants.DB.DRIVER;
		  String userName = api.Constants.DB.USER;
		  String password = api.Constants.DB.PASSWORD;
		  
		  try {
		  Class.forName(driver).newInstance();
		  webex21Conn = DriverManager.getConnection(url,userName,password);
		  isWebex21ConnValid = true;
		  System.out.println("Connected to the database webex");
		  } catch (Exception e) {
		  e.printStackTrace();
		  }	
	}
	
	
//	public void connectToLabstudy()
//	{
//		  String url = api.Constants.DB.LABSTUDY_URL;
//		  String driver = api.Constants.DB.DRIVER;
//		  String userName = api.Constants.DB.USER;
//		  String password = api.Constants.DB.PASSWORD;
//		  
//		  try {
//		  Class.forName(driver).newInstance();
//		  labstudyConn = DriverManager.getConnection(url,userName,password);
//		  isConnLabstudyValid = true;
//		  System.out.println("Connected to the database labstudy");
//		  } catch (Exception e) {
//		  e.printStackTrace();
//		  }	
//	}
//		
//	public boolean isConnectedToLabstudy()
//	{
//		if (labstudyConn != null) {
//			try {
//				if (labstudyConn.isClosed() == false & isConnLabstudyValid)
//					return true;
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
//		return false;
//	}
//	
//		
//	public void disconnectFromLabstudy()
//	{
//		if (labstudyConn != null)
//			try {
//				labstudyConn.close();
//			    System.out.println("Database labstudy Connection Closed");
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//	}
	
	public boolean isConnectedToWebex21()
	{
		if (webex21Conn != null) {
			try {
				if (webex21Conn.isClosed() == false & isWebex21ConnValid )
					return true;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	public void disconnectFromWebex()
	{
		if (webex21Conn != null)
			try {
				webex21Conn.close();
			    System.out.println("Database webex Connection Closed");
			} catch (SQLException e) {
				e.printStackTrace();
		}
	}
	
	public List<String> getContentsRdfs() {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> rdfs = new ArrayList<String>();
		try
		{
			sqlCommand = "(select distinct rdfid from ent_jquiz) union (select distinct d.rdfid from ent_dissection d,rel_scope_dissection sd where sd.DissectionID = d.DissectionID and sd.scopeid = 12)";
			ps = webex21Conn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				rdfs.add(rs.getString(1));
		}catch (SQLException e) {
			 e.printStackTrace();
		}
		releaseStatement(ps,rs);
		return rdfs;	
	}

	public void insertContentTree(String c, String tree) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		try
		{
			sqlCommand = "insert into ent_content_tree (content_name,tree) values ('"+c+"','"+tree+"')";
			ps = webex21Conn.prepareStatement(sqlCommand);
			ps.executeUpdate();			
		}catch (SQLException e) {
			 e.printStackTrace();
		}	
		releaseStatement(ps,null);
	}

	public List<String> getAdjacentConcept(String content, String concept, int sLine) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = getContentType(content);
		int eline = -1;		
		
		String conceptTable = "ent_jquiz_concept";
		if (isExample)
			conceptTable = "ent_jexample_concept";				

		try
		{		
			sqlCommand = "select eline from "+conceptTable+" where title ='"+content+"' and sline = "+sLine+" and concept = '"+concept +"'" ;
			ps = webex21Conn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
			{
				eline = rs.getInt(1);
			}
			sqlCommand = "select distinct concept from "+conceptTable+" where title ='"+content+"' and sline >= "+sLine+" and eline <= "+eline +" and concept != '"+concept+"'" ;
			ps = webex21Conn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
			{
				if (conceptList.contains(rs.getString(1)) == false)
					conceptList.add(rs.getString(1));
			}
		}catch (SQLException e) {
			 e.printStackTrace();
		}
		releaseStatement(ps,rs);
		return conceptList;	
	}

	public String getStartEndLine(String content) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		String lines = "";
		boolean isExample = getContentType(content);	
		
		String conceptTable = "ent_jquiz_concept";
		if (isExample)				
			conceptTable = "ent_jexample_concept";
		try
		{
			int s=-1,e=-1 ;
			sqlCommand = "select min(sline),max(eline) from "+conceptTable+" where title ='"+content+"'";
			ps = webex21Conn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
			{
				s = rs.getInt(1);
				e = rs.getInt(2);
				lines = s+","+e;				
			}
			
		}catch (SQLException e) {
			 e.printStackTrace();
		}	
		releaseStatement(ps,rs);
		return lines;	
	}

	public List<String> getConceptsInSameStartLine(String content, int sline) {
		String sqlCommand = "";
		ResultSet rs = null;
		PreparedStatement ps = null;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = getContentType(content);

		String conceptTable = "ent_jquiz_concept";
		if (isExample)
			conceptTable = "ent_jexample_concept";

		try {
			sqlCommand = "select distinct concept from " + conceptTable
					+ " where title ='" + content + "' and sline = " + sline;
			ps = webex21Conn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				conceptList.add(rs.getString(1));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		releaseStatement(ps,rs);
		return conceptList;
	}

	public List<String> getConceptsInDifferentStartEndLine(String content, int sline)
	{
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = getContentType(content);	
		
		String conceptTable = "ent_jquiz_concept";
		if (isExample)
			conceptTable = "ent_jexample_concept";

		try
		{								
			sqlCommand = "select distinct concept from "+conceptTable+" where title ='"+content+"' and sline != eline and sline = "+sline;
			ps = webex21Conn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				conceptList.add(rs.getString(1));
		}catch (SQLException e) {
			 e.printStackTrace();
		}
		releaseStatement(ps,rs);
		return conceptList;		
	}
	
	private boolean getContentType(String rdfid)
	{
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		boolean isExample = false;
		boolean isQuestion = false;
		try
		{
			sqlCommand = "select * from ent_dissection where rdfid = '"+rdfid+"'";
			ps = webex21Conn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			if (rs.next())
			{
				isExample = true;
			}	
			sqlCommand = "select * from ent_jquiz where rdfid = '"+rdfid+"'";
			ps = webex21Conn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			if (rs.next())
			{
				isQuestion = true;
			}	
		}catch (SQLException e) {
			 e.printStackTrace();
		}	
		if (isExample & isQuestion)
			System.out.println("Error occured! a content rdf: "+rdfid+" was in both ent_dissection and ent_jquiz");
		releaseStatement(ps,rs);
		return isExample;	
	}
	
	private void releaseStatement(PreparedStatement p, ResultSet r)
    {
		try {
			if (r != null) {
				r.close();
			}
			if (p != null) {
				p.close();
			}			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
