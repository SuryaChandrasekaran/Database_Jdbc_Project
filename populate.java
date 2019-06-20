/**
 * 
 */
package dbms;

/**
 * @author Surya
 *
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Populate {
	private static final String DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String CONNECTION = "jdbc:oracle:thin:@localhost:1521:XE";
	private static final String USER = "******";
	private static final String PASSWORD = "******";
	private static Connection dbConnection = null;
	private static PreparedStatement ps = null;
	private static BufferedReader br = null;
	private static FileReader fr = null;
	private static String FILENAME = "";
	private static String sqlStatement;
	private static String currentLine;

	private static Connection getDBConnection() {
		Connection dbConnection = null;
		try {
			Class.forName(DRIVER);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		}
		try {
			dbConnection = DriverManager.getConnection(CONNECTION, USER, PASSWORD);
			return dbConnection;
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return dbConnection;
	}

	public static void populateMoviesTable() {

		FILENAME = "C:\\DB workspace\\surya_a3\\movies.dat";
		try {
			fr = new FileReader(FILENAME);
			br = new BufferedReader(fr);

			int movieId;
			String title;
			String imdbID;
			String spanishTitle;
			String imdbPictureURL;
			int year;
			String rtID;
			float rtAllCriticsRating;
			float rtAllCriticsNumReviews;
			float rtAllCriticsNumFresh;
			float rtAllCriticsNumRotten;
			float rtAllCriticsScore;
			float rtTopCriticsRating;
			float rtTopCriticsNumReviews;
			float rtTopCriticsNumFresh;
			float rtTopCriticsNumRotten;
			float rtTopCriticsScore;
			float rtAudienceRating;
			float rtAudienceNumRatings;
			float rtAudienceScore;
			String rtPictureURL;

			br.readLine();
			while ((currentLine = br.readLine()) != null) {
				String[] tokens = currentLine.split("\t");
				if (tokens.length == 21) {
					movieId = Integer.parseInt(tokens[0]);
					title = tokens[1];
					imdbID = tokens[2];
					spanishTitle = tokens[3];
					imdbPictureURL = tokens[4];
					year = Integer.parseInt(tokens[5]);
					rtID = tokens[6];
					rtAllCriticsRating = tokens[7].equals("\\N") ? 0 : Float.parseFloat(tokens[7]);
					rtAllCriticsNumReviews = tokens[8].equals("\\N") ? 0 : Float.parseFloat(tokens[8]);
					rtAllCriticsNumFresh = tokens[9].equals("\\N") ? 0 : Float.parseFloat(tokens[9]);
					rtAllCriticsNumRotten = tokens[10].equals("\\N") ? 0 : Float.parseFloat(tokens[10]);
					rtAllCriticsScore = tokens[11].equals("\\N") ? 0 : Float.parseFloat(tokens[11]);
					rtTopCriticsRating = tokens[12].equals("\\N") ? 0 : Float.parseFloat(tokens[12]);
					rtTopCriticsNumReviews = tokens[13].equals("\\N") ? 0 : Float.parseFloat(tokens[13]);
					rtTopCriticsNumFresh = tokens[14].equals("\\N") ? 0 : Float.parseFloat(tokens[14]);
					rtTopCriticsNumRotten = tokens[15].equals("\\N") ? 0 : Float.parseFloat(tokens[15]);
					rtTopCriticsScore = tokens[16].equals("\\N") ? 0 : Float.parseFloat(tokens[16]);
					rtAudienceRating = tokens[17].equals("\\N") ? 0 : Float.parseFloat(tokens[17]);
					rtAudienceNumRatings = tokens[18].equals("\\N") ? 0 : Float.parseFloat(tokens[18]);
					rtAudienceScore = tokens[19].equals("\\N") ? 0 : Float.parseFloat(tokens[19]);
					rtPictureURL = tokens[20];

					sqlStatement = "INSERT INTO movies VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

					try {
						ps = dbConnection.prepareStatement(sqlStatement);
						ps.setInt(1, movieId);
						ps.setString(2, title);
						ps.setString(3, imdbID);
						ps.setString(4, spanishTitle);
						ps.setString(5, imdbPictureURL);
						ps.setInt(6, year);
						ps.setString(7, rtID);
						ps.setDouble(8, rtAllCriticsRating);
						ps.setDouble(9, rtAllCriticsNumReviews);
						ps.setDouble(10, rtAllCriticsNumFresh);
						ps.setDouble(11, rtAllCriticsNumRotten);
						ps.setDouble(12, rtAllCriticsScore);
						ps.setDouble(13, rtTopCriticsRating);
						ps.setDouble(14, rtTopCriticsNumReviews);
						ps.setDouble(15, rtTopCriticsNumFresh);
						ps.setDouble(16, rtTopCriticsNumRotten);
						ps.setDouble(17, rtTopCriticsScore);
						ps.setDouble(18, rtAudienceRating);
						ps.setDouble(19, rtAudienceNumRatings);
						ps.setDouble(20, rtAudienceScore);
						ps.setString(21, rtPictureURL);
						ps.executeUpdate();

					} catch (SQLException e) {
						System.out.println("failed prepared statement");
						e.printStackTrace();
						return;
					} finally {
						try {
							ps.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);
						}

					}

				} else {
					System.out.println("column mismatch for the movies table");
				}
			}

		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("finished inserting data into movies table");

	}

	public static void populateMovie_GenresTable() {

		try {
			fr = new FileReader("C:\\DB workspace\\surya_a3\\movie_genres.dat");
			br = new BufferedReader(fr);

			int movieID;
			String genre;

			br.readLine();
			while ((currentLine = br.readLine()) != null) {
				String[] tokens = currentLine.split("\t");
				if (tokens.length == 2) {
					movieID = Integer.parseInt(tokens[0]);
					genre = tokens[1];
					sqlStatement = "INSERT INTO movie_genres VALUES('" + movieID + "','" + genre + "')";

					try {
						ps = dbConnection.prepareStatement(sqlStatement);
						ps.executeUpdate(sqlStatement);

					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("failed to execute query for table genres");
						e.printStackTrace();
						return;
					} finally {
						if (ps != null) {
							try {
								ps.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);

							}

						}
					}
				} else {
					System.out.println("column mismatch for table movie_genres");
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("finished entering data into movie_genres table");
	}

	public static void populateMovie_ActorsTable() {

		try {
			fr = new FileReader("C:\\DB workspace\\surya_a3\\movie_actors.dat");
			br = new BufferedReader(fr);

			Long movieID;
			String actorID;
			String actorName;
			int ranking;

			br.readLine();
			while ((currentLine = br.readLine()) != null) {
				String[] tokens = currentLine.split("\t");
				
					movieID = (tokens.length>0)?Long.parseLong(tokens[0]):-1;
					actorID = (tokens.length>1)? tokens[1]:"";
					actorName = (tokens.length>2)?tokens[2]:"";
					ranking = (tokens.length>3)?Integer.parseInt(tokens[3]):-1;
					sqlStatement = "INSERT INTO movie_actors VALUES(?,?,?,?)";

					try {
						ps = dbConnection.prepareStatement(sqlStatement);
						ps.setLong(1, movieID);
						ps.setString(2, actorID);
						ps.setString(3, actorName);
						ps.setInt(4, ranking);

						ps.executeUpdate(sqlStatement);

					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("failed to execute query for table movie_actors");
						e.printStackTrace();
						return;
					} finally {
						if (ps != null) {
							try {
								ps.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);

							}

						}
					}
				
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("finished entering data into movie_actors table");
	}

	public static void populateMovie_DirectorsTable() {

		try {
			fr = new FileReader("C:\\DB workspace\\surya_a3\\movie_directors.dat");
			br = new BufferedReader(fr);
			int movieID;
			String directorID;
			String directorName;

			br.readLine();
			while ((currentLine = br.readLine()) != null) {
				String[] tokens = currentLine.split("\t");
				if (tokens.length == 3) {
					movieID = Integer.parseInt(tokens[0]);
					directorID = tokens[1];
					directorName = tokens[2];
					String sqlStatement = "INSERT INTO movie_directors VALUES(?,?,?)";

					try {
						ps = dbConnection.prepareStatement(sqlStatement);
						ps.setInt(1, movieID);
						ps.setString(2, directorID);
						ps.setString(3, directorName);
						ps.executeUpdate();

					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("failed to execute query for table movie_directors");
						e.printStackTrace();
						return;
					} finally {
						if (ps != null) {
							try {
								ps.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);

							}

						}
					}
				} else {
					System.out.println("column mismatch for table movie_directors");
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("finished entering data into movie_actors table");
	}

	public static void populateMovie_LocationsTable() {

		try {
			fr = new FileReader("C:\\DB workspace\\surya_a3\\movie_locations.dat");
			br = new BufferedReader(fr);
			int movieID;
			String location1;
			String location2;
			String location3;
			String location4;

			br.readLine();
			while ((currentLine = br.readLine()) != null) {
				String[] tokens = currentLine.split("\t");

				movieID = (tokens.length > 0) ? Integer.parseInt(tokens[0]) : -1;
				location1 = (tokens.length>1)?  tokens[1]: "";
				location2 = (tokens.length>2)?tokens[2]:"";
				location3 = (tokens.length>3)?  tokens[3]:"";
				location4 =  (tokens.length>4)? tokens[4]:"";

				String sqlStatement = "INSERT INTO movie_locations VALUES(?,?,?,?,?)";

				try {
					ps = dbConnection.prepareStatement(sqlStatement);
					ps.setInt(1, movieID);
					ps.setString(2, location1);
					ps.setString(3, location2);
					ps.setString(4, location3);
					ps.setString(5, location4);

					ps.executeUpdate();

				} catch (SQLException e) {
					// TODO Auto-generated catch block
					System.out.println("failed to execute query for table movie_locations");
					e.printStackTrace();
					return;
				} finally {
					if (ps != null) {
						try {
							ps.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);

						}

					}
				}

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("finished entering data into movie_locations table");
	}

	public static void populateMovie_TagsTable() {

		try {
			fr = new FileReader("C:\\DB workspace\\surya_a3\\movie_tags.dat");
			br = new BufferedReader(fr);
			int movieID;
			int tagID;
			int tagWeight;

			br.readLine();
			while ((currentLine = br.readLine()) != null) {
				String[] tokens = currentLine.split("\t");
				if (tokens.length == 3) {
					movieID = Integer.parseInt(tokens[0]);
					tagID = Integer.parseInt(tokens[1]);
					tagWeight = Integer.parseInt(tokens[2]);

					String sqlStatement = "INSERT INTO movie_tags VALUES(?,?,?)";

					try {
						ps = dbConnection.prepareStatement(sqlStatement);
						ps.setInt(1, movieID);
						ps.setInt(2, tagID);
						ps.setInt(3, tagWeight);
						ps.executeUpdate();

					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("failed to execute query for table movie_tags");
						e.printStackTrace();
						return;
					} finally {
						if (ps != null) {
							try {
								ps.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);

							}

						}
					}
				} else {
					System.out.println("column mismatch for table movie_tags");
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("finished entering data into movie_tags table");
	}

	public static void populateMovie_CountriesTable() {

		try {
			fr = new FileReader("C:\\DB workspace\\surya_a3\\movie_countries.dat");
			br = new BufferedReader(fr);
			int movieID;
			String country;

			br.readLine();
			while ((currentLine = br.readLine()) != null) {
				String[] tokens = currentLine.split("\t");
				
					movieID = (tokens.length>0)? Integer.parseInt(tokens[0]):-1;
					country = (tokens.length>1)?tokens[1]:"";

					String sqlStatement = "INSERT INTO movie_countries VALUES(?,?)";

					try {
						ps = dbConnection.prepareStatement(sqlStatement);
						ps.setInt(1, movieID);
						ps.setString(2, country);
						ps.executeUpdate();

					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("failed to execute query for table movie_countries");
						e.printStackTrace();
						return;
					} finally {
						if (ps != null) {
							try {
								ps.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);

							}

						}
					}
				
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("finished entering data into movie_counties table");
	}

	public static void populateTagsTable() {

		try {
			fr = new FileReader("C:\\DB workspace\\surya_a3\\tags.dat");
			br = new BufferedReader(fr);
			int id;
			String value;

			br.readLine();
			while ((currentLine = br.readLine()) != null) {
				String[] tokens = currentLine.split("\t");
				if (tokens.length == 2) {
					id = Integer.parseInt(tokens[0]);
					value = tokens[1];

					String sqlStatement = "INSERT INTO tags VALUES(?,?)";

					try {
						ps = dbConnection.prepareStatement(sqlStatement);
						ps.setInt(1, id);
						ps.setString(2, value);
						ps.executeUpdate();

					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("failed to execute query for table tags");
						e.printStackTrace();
						return;
					} finally {
						if (ps != null) {
							try {
								ps.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);

							}

						}
					}
				} else {
					System.out.println("column mismatch for table tags");
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				fr.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("finished entering data into tags table");
	}

	public static void dropTables() {

		System.out.println("dropping all existing tables");
		ArrayList<String> tableNames = new ArrayList<>();
		tableNames.addAll(Arrays.asList("movies", "movie_genres", "movie_directors", "movie_actors", "movie_locations",
				"movie_tags", "movie_countries", "tags"));
		PreparedStatement ps = null;
		for (int i = 0; i < tableNames.size(); i++) {
			String sqlStatement = "DROP TABLE " + tableNames.get(i);
			try {
				ps = dbConnection.prepareStatement(sqlStatement);
				System.out.println("dropping talbe " + tableNames.get(i));
				ps.executeUpdate(sqlStatement);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("error executing sql statement");
				return;
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, e);
					}
				}
			}

		}
		System.out.println("finished dropping all tables");
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		dbConnection = getDBConnection();
		// System.out.println(dbConnection);
		// create a function to drop all tables
		// dropTables();

		// populateMoviesTable();
		// populateMovie_GenresTable();
		// populateMovie_DirectorsTable();

//		 populateMovie_ActorsTable();
//		populateMovie_LocationsTable();
//		 populateMovie_TagsTable();
//		 populateMovie_CountriesTable();

	}

}
