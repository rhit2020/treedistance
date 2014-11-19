import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class createTED {
	
//	private String[] ignoreConceptList = {"ClassDefinition","FormalMethodParameter","MethodDefinition",
//			       "PublicClassSpecifier","PublicMethodSpecifier","StaticMethodSpecifier","VoidDataType"};
//
	public static void main (String[] args)
	{
		createTED ted = new createTED();
		DB db  = new DB();
		db.connectToWebex21();
		if (db.isConnectedToWebex21())
		{
		    List<String> rdfs = db.getContentsRdfs(); 
//			String[] rdfs = {"inheritance_casting_1"};
			for (String c : rdfs)
			{
				String tree = ted.createTree(c,db);
				db.insertContentTree(c, tree);
			}
			//createTED.String tree = ted.createTree("TypeCasting_v2",db);

			
		//create files
//		String[] eList = db.getExamplesName();
//		String[] qList = db.getQuestionNames();
//		String qtree,etree;
//		
//		for (int i = 0 ; i < eList.length; i++)
//			for (int j = 0 ; j < qList.length; j++)
//			{
//				qtree = db.getTree(qList[j]);
//				etree = db.getTree(eList[i]);
//				writeToFile(qList[j],qtree,eList[i],etree);
//			}
			db.disconnectFromWebex();
		}
	}

	private static void writeToFile(String q, String qtree, String e, String etree) {
		try { 
			File file = new File("resource/"+q+"@"+e);
 			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(qtree);
			bw.newLine();
			bw.write(etree);
			bw.close(); 
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}		
	}

	private String createTree(String content, DB db) {
		List<String> subtreeList = new ArrayList<String>();
		List<String> conceptsInSameStartLine;
		List<String> conceptsInDifferentStartEndLine;
		List<String> adjucentConceptsList;
		String se = db.getStartEndLine(content);

		String[] lines = se.split(",");
		int start = Integer.parseInt(lines[0]);
		int end = Integer.parseInt(lines[1]);
		String subtree;
		for (int i = start; i <= end; i++)
		{
			subtree = "";
			conceptsInSameStartLine = db.getConceptsInSameStartLine(content,i);
			if (conceptsInSameStartLine.isEmpty() == false)
			{
				Collections.sort(conceptsInSameStartLine, new SortByName());
				for (String c : conceptsInSameStartLine)
				{
					subtree +="ROOT-"+c+";";		
				}
				if (subtreeList.contains(subtree) == false)
				{
					subtreeList.add(subtree);
				}
			}			

			conceptsInDifferentStartEndLine = db.getConceptsInDifferentStartEndLine(content,i);
			if (conceptsInDifferentStartEndLine.isEmpty() == false)
			{
				Collections.sort(conceptsInDifferentStartEndLine, new SortByName());
				for (String c: conceptsInDifferentStartEndLine)
				{
					subtree = "";
					adjucentConceptsList = db.getAdjacentConcept(content, c,i);
					Collections.sort(adjucentConceptsList, new SortByName());
					for (String adjcon : adjucentConceptsList)
						subtree += c+"-"+adjcon+";";
					if (subtree !="")
					{
						if (subtreeList.contains(subtree) == false)
						{
							subtreeList.add(subtree);
						}

					}
				}	
			}						
		}		
		String tree = "";
		//put subtrees into tree
		for (int i = 0; i < subtreeList.size(); i++)
		{
			tree+= subtreeList.get(i);
			if (i != subtreeList.size() -1)
				tree+="@";
		}
		return tree;		
	}
	
	public class SortByName implements Comparator<String> {
	    public int compare(String s1, String s2) {
	        return s1.compareTo(s2);
	    }
	}

}
