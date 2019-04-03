package com;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.sql.*;

public class App {
	
	
	public static void main (String[] args) throws SQLException{
		String csvFile = "/home/ubuntu/projet/projet-eseo/src/main/ressources/laposte_hexasmal.csv";
		BufferedReader br = null;
		String line = "";
		String csvSplitBy = ",";
		String cellSplitBy = ";";
		
		String myDriver = "com.mysql.jdbc.Driver";
	    String myUrl = "jdbc:mysql://localhost/projet?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	    
	    Connection conn = null;
		
		try {
			Class.forName(myDriver);
			conn = DriverManager.getConnection(myUrl, "root", "root");
			
			br = new BufferedReader(new FileReader(csvFile));
			int i = 0;
			br.readLine();
			while ((line = br.readLine()) != null) {
				String check = " SELECT * FROM villes WHERE ";
				String query = " INSERT INTO villes (code_commune, nom_commune, code_postal, libelle_acheminement, ligne_5, coordonnees_gps)"
				        + " VALUES (";
				String[] read = line.split(cellSplitBy);
				if(read.length>0) {
					query += "'" + read[0] + "',";
					check += "code_commune = '" + read[0]+ "' AND ";
				} else {
					query += "null,";
				}
				if(read.length>1) {
					query += "'" + read[1] + "',";
					check += "nom_commune = '" + read[1]+ "' AND ";
				} else {
					query += "null,";
				}
				if(read.length>2) {
					query += "'" + read[2] + "',";
					check += "code_postal = '" + read[2]+ "' AND ";
				} else {
					query += "null,";
				}
				if(read.length>3) {
					query += "'" + read[3] + "',";
					check += "libelle_acheminement = '" + read[3]+ "' AND ";
				} else {
					query += "null,";
				}
				if(read.length>4) {
					query += "'" + read[4] + "',";
					check += "ligne_5 = '" + read[4]+ "' AND ";
				} else {
					query += "null,";
				}	
				if(read.length>5) {
					query += "'" + read[5] + "'";
					check += "coordonnees_gps = '" + read[5]+ "';";
				} else {
					query += "null";
				}
						
				query += ");";
				if(read.length>5) {
					Statement stmt=conn.createStatement();  
					ResultSet rs = stmt.executeQuery(check);
					boolean empty = true;
					while(rs.next()) {
						empty = false;
					}
					if (empty) {
						stmt.executeUpdate(query);
					}
				}		
			}
		} catch (Exception e) {
			System.err.println("got an exception");
			System.err.println(e.getMessage());
			e.printStackTrace();
		} finally {
			conn.close();
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
