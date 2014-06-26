import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;



public class DB {

	private Connection connWebex21;
	private boolean isConnWebex21Valid;

	private Connection guanjieConn;
	private boolean isConnGuanjieValid;

	private Connection um2Conn;
	private boolean isConnum2Valid;
	
	public DB()
	{
		connectToUM2();
	}
	
	public void connectToUM2()
	{
		  String url = api.Constants.DB.UM2_URL;
		  String driver = api.Constants.DB.DRIVER;
		  String userName = api.Constants.DB.USER;
		  String password = api.Constants.DB.PASSWORD;
		  
		  try {
		  Class.forName(driver).newInstance();
		  um2Conn = DriverManager.getConnection(url,userName,password);
		  isConnum2Valid = true;
		  System.out.println("Connected to the database um2");
		  } catch (Exception e) {
		  e.printStackTrace();
		  }	
	}
	public boolean isConnectedToUM2()
	{
		if (um2Conn != null) {
			try {
				if (um2Conn.isClosed() == false & isConnum2Valid)
					return true;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	
	public void disconnectFromUM2()
	{
		if (um2Conn != null)
			try {
				um2Conn.close();
			    System.out.println("Database um2 Connection Closed");
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	public void connectToWebex21()
	{
		  String url = api.Constants.DB.WEBEX_URL;
		  String driver = api.Constants.DB.DRIVER;
		  String userName = api.Constants.DB.USER;
		  String password = api.Constants.DB.PASSWORD;
		  try {
		  Class.forName(driver);
		  connWebex21 = DriverManager.getConnection(url,userName,password);
		  isConnWebex21Valid = true;
		  System.out.println("Connected to the database webex21");
		  } catch (Exception e) {
		  e.printStackTrace();
		  }	
	}	
	
	public void connectToGuangie()
	{
		  String url = api.Constants.DB.GUANGIE_URL;
		  String driver = api.Constants.DB.DRIVER;
		  String userName = api.Constants.DB.USER;
		  String password = api.Constants.DB.PASSWORD;
		  
		  try {
		  Class.forName(driver).newInstance();
		  guanjieConn = DriverManager.getConnection(url,userName,password);
		  isConnGuanjieValid = true;
		  System.out.println("Connected to the database guangie");
		  } catch (Exception e) {
		  e.printStackTrace();
		  }	
	}
	
	public boolean isConnectedToWebex21()
	{
		if (connWebex21 != null) {
			try {
				if (connWebex21.isClosed() == false & isConnWebex21Valid)
					return true;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	
	public boolean isConnectedToGuanjie()
	{
		if (guanjieConn != null) {
			try {
				if (guanjieConn.isClosed() == false & isConnGuanjieValid)
					return true;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
		
	public void disconnectFromWebex21()
	{
		if (connWebex21 != null)
			try {
				connWebex21.close();
			    System.out.println("Database webex21 Connection Closed");
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	
	public void disconnectFromGuanjie()
	{
		if (guanjieConn != null)
			try {
				guanjieConn.close();
			    System.out.println("Database guangie Connection Closed");
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
			sqlCommand = "SELECT content_name FROM guanjie.ent_content";
			ps = guanjieConn.prepareStatement(sqlCommand);
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
			ps = guanjieConn.prepareStatement(sqlCommand);
			ps.executeUpdate();			
		}catch (SQLException e) {
			 e.printStackTrace();
		}				
	}
	
	public String[] getExamplesName() {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		String[] list = null;
		int count = -1;
		sqlCommand = "SELECT count(distinct content_name) FROM ent_content where content_type = 'example'";
		try {
			ps = guanjieConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				count = rs.getInt(1);
			list = new String[count];
			sqlCommand = "SELECT content_name FROM ent_content where content_type = 'example'";
			ps = guanjieConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			int i = 0;
			while (rs.next())
			{
				list[i] = rs.getString(1);
				i++;
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	
		return list;
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
			ps = guanjieConn.prepareStatement(sqlCommand);
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
		connectToWebex21();
		if (isConnectedToWebex21())
		{
			try
			{		
				sqlCommand = "select eline from "+conceptTable+" where title ='"+content+"' and sline = "+sLine+" and concept = '"+concept +"'" ;
				ps = connWebex21.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while (rs.next())
				{
					eline = rs.getInt(1);
				}
				sqlCommand = "select distinct concept from "+conceptTable+" where title ='"+content+"' and sline >= "+sLine+" and eline <= "+eline +" and concept != '"+concept+"'" ;
				ps = connWebex21.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while (rs.next())
				{
					if (conceptList.contains(rs.getString(1)) == false)
						conceptList.add(rs.getString(1));
				}
			}catch (SQLException e) {
				 e.printStackTrace();
			}
			disconnectFromWebex21();
		}			
		return conceptList;	
	}
	
	public List<String> getAdjacentConcept(String content, String concept, String seLine) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = false;
		String[] lines = seLine.split(",");
		String startLine = lines[0];
		String endLine = lines[1];
		sqlCommand = "select content_type from ent_content where content_name ='"+content+"'";
		try {
			ps = guanjieConn.prepareStatement(sqlCommand);
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
		connectToWebex21();
		if (isConnectedToWebex21())
		{
			try
			{					
				sqlCommand = "select concept from "+conceptTable+" where title ='"+content+"' and sline >= "+startLine+" and eline <= "+endLine +" and concept != '"+concept+"'" ;
				ps = connWebex21.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while (rs.next())
				{
					if (conceptList.contains(rs.getString(1)) == false)
						conceptList.add(rs.getString(1));
				}
			}catch (SQLException e) {
				 e.printStackTrace();
			}
			disconnectFromWebex21();
		}			
		return conceptList;	
	}

	public List<String> getConcepts(String content) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = false;
		sqlCommand = "select content_type from ent_content where content_name ='"+content+"'";
		try {
			ps = guanjieConn.prepareStatement(sqlCommand);
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

		connectToWebex21();
		if (isConnectedToWebex21())
		{
			try
			{								
				sqlCommand = "select distinct concept from "+conceptTable+" where title ='"+content+"'";
				ps = connWebex21.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while (rs.next())
					conceptList.add(rs.getString(1));
			}catch (SQLException e) {
				 e.printStackTrace();
			}
			disconnectFromWebex21();
		}			
		return conceptList;	
	}

	public String[] getQuestionNames() {

		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		String[] list = null;
		int count = -1;
		sqlCommand = "SELECT count(distinct content_name) FROM ent_content where content_type = 'question'";
		try {
			ps = guanjieConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				count = rs.getInt(1);
			list = new String[count];
			sqlCommand = "SELECT content_name FROM ent_content where content_type = 'question' ";
			ps = guanjieConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			int i = 0;
			while (rs.next())
			{
				list[i] = rs.getString(1);
				i++;
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	
		return list;
	
	}
	
	public String getTree(String c) {
        PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		String tree = "";
		
		try {
			sqlCommand = "SELECT tree  FROM ent_content_tree where content_name = '"+c+"'";
			ps = guanjieConn.prepareStatement(sqlCommand);
			rs = ps.executeQuery();
			while (rs.next())
				 tree = rs.getString(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		return tree;
	}

	public void insertContentSim(String question, String example, double sim, String method) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		try
		{
			sqlCommand = "insert into rel_con_con_sim (question_content_name,example_content_name,sim,method) values ('"+question+"','"+example+"',"+sim+",'"+method+"')";
			ps = guanjieConn.prepareStatement(sqlCommand);
			ps.executeUpdate();			
		}catch (SQLException e) {
			 e.printStackTrace();
		}			
	}

	public int getConceptID(String c) {
        PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		int id = -1;
		//connectToUM2();
		//if (isConnectedToUM2())
		//{
			try {
				sqlCommand = "SELECT ConceptID  FROM ent_concept where Title = '"+c+"'";
				ps = um2Conn.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while (rs.next())
					 id = rs.getInt(1);
			} catch (SQLException e) {
				e.printStackTrace();
			}			
		//	disconnectFromUM2();
		//}
		
		return id;
	}

	public List<String> getConceptLines(String content, String con) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> lines = new ArrayList<String>();
		boolean isExample = false;	
		sqlCommand = "select content_type from ent_content where content_name ='"+content+"'";
		try {
			ps = guanjieConn.prepareStatement(sqlCommand);
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
		connectToWebex21();
		if (isConnectedToWebex21())
		{
			try
			{
				int s=-1,e=-1 ;
				sqlCommand = "select sline,eline from "+conceptTable+" where title ='"+content+"' and concept = '"+con+"'";
				ps = connWebex21.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while (rs.next())
				{
					s = rs.getInt(1);
					e = rs.getInt(2);
					lines.add(s+","+e);				
				}
				
			}catch (SQLException e) {
				 e.printStackTrace();
			}
			disconnectFromWebex21();
		}			
		return lines;	
	}

	public boolean isOutcomeConcept(String content, String concept) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		String topic = "";
		boolean isOutcome = false;
		List<String> topicOutcomeConceptList = new ArrayList<String>();		
		try
		{
				sqlCommand = "SELECT topic_name FROM guanjie.rel_topic_content where content_name = '"+content+"'";
				ps = guanjieConn.prepareStatement(sqlCommand);
				ResultSet rs = ps.executeQuery();
				while(rs.next())
				{
					topic = rs.getString(1);
				}
				sqlCommand = " SELECT distinct concept_name" +
						     " FROM guanjie.rel_topic_concept_agg"+
						     " where topic_name = '"+topic+"' and direction = 'outcome'";						
				ps = guanjieConn.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while(rs.next())
				{
					topicOutcomeConceptList.add(rs.getString(1));
				}
				if (topicOutcomeConceptList.contains(concept))
					isOutcome=  true;
		}catch (SQLException e) {
			 e.printStackTrace();
		}
		return isOutcome;
	}

	public String getStartEndLine(String content) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		String lines = "";
		boolean isExample = false;	
		sqlCommand = "select content_type from ent_content where content_name ='"+content+"'";
		try {
			ps = guanjieConn.prepareStatement(sqlCommand);
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
		connectToWebex21();
		if (isConnectedToWebex21())
		{
			try
			{
				int s=-1,e=-1 ;
				sqlCommand = "select min(sline),max(eline) from "+conceptTable+" where title ='"+content+"'";
				ps = connWebex21.prepareStatement(sqlCommand);
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
			disconnectFromWebex21();
		}			
		return lines;	
	}

	public List<String> getConceptsInSameStartEndLine(String content, int sline) {
		PreparedStatement ps = null;
		String sqlCommand = "";
		ResultSet rs = null;
		List<String> conceptList = new ArrayList<String>();
		boolean isExample = false;
		sqlCommand = "select content_type from ent_content where content_name ='"+content+"'";
		try {
			ps = guanjieConn.prepareStatement(sqlCommand);
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

		connectToWebex21();
		if (isConnectedToWebex21())
		{
			try
			{								
				sqlCommand = "select distinct concept from "+conceptTable+" where title ='"+content+"' and sline = eline and sline = "+sline;
				ps = connWebex21.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while (rs.next())
					conceptList.add(rs.getString(1));
			}catch (SQLException e) {
				 e.printStackTrace();
			}
			disconnectFromWebex21();
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
			ps = guanjieConn.prepareStatement(sqlCommand);
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

		connectToWebex21();
		if (isConnectedToWebex21())
		{
			try
			{								
				sqlCommand = "select distinct concept from "+conceptTable+" where title ='"+content+"' and sline != eline and sline = "+sline;
				ps = connWebex21.prepareStatement(sqlCommand);
				rs = ps.executeQuery();
				while (rs.next())
					conceptList.add(rs.getString(1));
			}catch (SQLException e) {
				 e.printStackTrace();
			}
			disconnectFromWebex21();
		}			
		return conceptList;		
	}
	
	public double getTFIDF(String q, String c) {
		double val = 0.0;
		if (isConnectedToWebex21() == false)
		{
			connectToWebex21();
			if (isConnWebex21Valid)
			{
				try
				{				
					String sqlCommand = "SELECT `log(tf+1)idf` FROM webex21.temp2_ent_jcontent_tfidf where title = '"+q+"' and concept = '"+c+"';";
					PreparedStatement ps = connWebex21.prepareStatement(sqlCommand);
					ResultSet rs = ps.executeQuery();
					while (rs.next())
						val = rs.getDouble(1);
				}catch (SQLException e) {
					 e.printStackTrace();
				}
				disconnectFromWebex21();
			}
			
		}		
		return val;
	}
}
