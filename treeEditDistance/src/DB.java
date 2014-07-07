import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;



public class DB {
	private Connection labstudyConn;
	private boolean isConnLabstudyValid;
		
	public void connectToLabstudy()
	{
		  String url = api.Constants.DB.LABSTUDY_URL;
		  String driver = api.Constants.DB.DRIVER;
		  String userName = api.Constants.DB.USER;
		  String password = api.Constants.DB.PASSWORD;
		  
		  try {
		  Class.forName(driver).newInstance();
		  labstudyConn = DriverManager.getConnection(url,userName,password);
		  isConnLabstudyValid = true;
		  System.out.println("Connected to the database labstudy");
		  } catch (Exception e) {
		  e.printStackTrace();
		  }	
	}
		
	public boolean isConnectedToLabstudy()
	{
		if (labstudyConn != null) {
			try {
				if (labstudyConn.isClosed() == false & isConnLabstudyValid)
					return true;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
		
	public void disconnectFromLabstudy()
	{
		if (labstudyConn != null)
			try {
				labstudyConn.close();
			    System.out.println("Database labstudy Connection Closed");
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
			sqlCommand = "SELECT content_name FROM ent_content where domain = 'java'";
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				rdfs.add(rs.getString(1));
		}catch (SQLException e) {
			 e.printStackTrace();
		}	
		return rdfs;	
	}

	public void insertContentTree(String c, String tree) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		try
		{
			sqlCommand = "insert into ent_content_tree (content_name,tree) values ('"+c+"','"+tree+"')";
			ps = labstudyConn.prepareStatement(sqlCommand);
			ps.executeUpdate();			
		}catch (SQLException e) {
			 e.printStackTrace();
		}				
	}

	public List<String> getAdjacentConcept(String content, String concept, int sLine) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = false;

		int eline = -1;
		sqlCommand = "select content_type from ent_content where content_name ='"+content+"'";
		try {
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
			{
				 if (rs.getString(1).equals("example"))
					 isExample = true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String conceptTable = "ent_jquiz_concept";
		if (isExample)
			conceptTable = "ent_jexample_concept";				

		try
		{		
			sqlCommand = "select eline from "+conceptTable+" where title ='"+content+"' and sline = "+sLine+" and concept = '"+concept +"'" ;
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
			{
				eline = rs.getInt(1);
			}
			sqlCommand = "select distinct concept from "+conceptTable+" where title ='"+content+"' and sline >= "+sLine+" and eline <= "+eline +" and concept != '"+concept+"'" ;
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
			{
				if (conceptList.contains(rs.getString(1)) == false)
					conceptList.add(rs.getString(1));
			}
		}catch (SQLException e) {
			 e.printStackTrace();
		}
				
		return conceptList;	
	}

	public String getStartEndLine(String content) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		String lines = "";
		boolean isExample = false;	
		sqlCommand = "select content_type from ent_content where content_name ='"+content+"'";
		try {
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
			{
				 if (rs.getString(1).equals("example"))
					 isExample = true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String conceptTable = "ent_jquiz_concept";
		if (isExample)				
			conceptTable = "ent_jexample_concept";
		try
		{
			int s=-1,e=-1 ;
			sqlCommand = "select min(sline),max(eline) from "+conceptTable+" where title ='"+content+"'";
			ps = labstudyConn.prepareStatement(sqlCommand);
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
		return lines;	
	}

	public List<String> getConceptsInSameStartLine(String content, int sline) {
		String sqlCommand = "";
		ResultSet rs = null;
		PreparedStatement ps;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = false;
		sqlCommand = "select content_type from ent_content where content_name ='"
				+ content + "'";
		try {
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getString(1).equals("example"))
					isExample = true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String conceptTable = "ent_jquiz_concept";
		if (isExample)
			conceptTable = "ent_jexample_concept";

		try {
			sqlCommand = "select distinct concept from " + conceptTable
					+ " where title ='" + content + "' and sline = " + sline;
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				conceptList.add(rs.getString(1));
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return conceptList;

	}

	public List<String> getConceptsInDifferentStartEndLine(String content, int sline) {

		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = false;
		sqlCommand = "select content_type from ent_content where content_name ='"+content+"'";
		try {
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
			{
				 if (rs.getString(1).equals("example"))
					 isExample = true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String conceptTable = "ent_jquiz_concept";
		if (isExample)
			conceptTable = "ent_jexample_concept";

		try
		{								
			sqlCommand = "select distinct concept from "+conceptTable+" where title ='"+content+"' and sline != eline and sline = "+sline;
			ps = labstudyConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				conceptList.add(rs.getString(1));
		}catch (SQLException e) {
			 e.printStackTrace();
		}
			
		return conceptList;		
	}
}
