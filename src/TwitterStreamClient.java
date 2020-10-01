import java.io.*; 
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.math.*;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.ObjectMapper;
import java.text.*;

import org.apache.http.client.*;
import org.apache.http.impl.client.*;
import org.apache.http.client.methods.*;
import org.apache.http.auth.*;
import org.apache.http.entity.*;
import org.apache.http.*;

import java.sql.*;


public class TwitterStreamClient {


	private static final String TWITTER_STREAM_URL = "https://stream.twitter.com/1/statuses/filter.json";
	private static final String DEFAULT_TRACK_KEYWORDS = "Cricket,Football,Soccer,BasketBall,Hockey";
	private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/tweetsdb";
	private static final String MYSQL_USER = "root" ;
	private static final String MYSQL_PASSWORD = "jackbe" ;


	public static void main(String args[]) throws Exception {
		TwitterStreamClient tstream = new TwitterStreamClient();
		String keywords  = null ;
		String locations = null ;

		int i = 0;
		while ( i < args.length ) {
			
			if ( "-track".equalsIgnoreCase( args[i]) ) {
				keywords = args[i+1];
				i += 2;
			}
			else if ( "-locations".equalsIgnoreCase( args[i] ) ) {
				locations = args[i+1];
				i += 2;
			}
			else {
				System.out.println(	"  Usage: run.sh -track \"keyword1,keyword2\" -locations \"lat1,long1\" ");
				return;
			}

		}

		if ( keywords == null && locations == null ) {
				System.out.println(	"Usage: run.sh -track \"keyword1,keyword2\" -locations \"lat1,long1\" ");
				return;
		}
		System.out.println(	"Tracking Keywords: " + ( keywords == null ? "<None>" : keywords) );
		System.out.println(	"Locations : " + ( locations == null ? "<None>" : locations ));
		tstream.sipFirehose(keywords,locations);
	}

	BlockingQueue<Map<String,Object>> tweetsQ = new LinkedBlockingQueue<Map<String,Object>>(500);

	public void sipFirehose(final String keywords, final String locations) throws Exception {

		new Thread(new TweetSpout(tweetsQ, keywords, locations) , "TweetsThread").start();

   		Class.forName("com.mysql.jdbc.Driver");

		Connection con = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD );

		PreparedStatement ps = con.prepareStatement("insert into tweets(tweet, ts, hashtags, username, geotype, lat1, long1, lat2, long2, lat3, long3, lat4, long4) value(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

		while ( true ) { 
			Map<String, Object> tweet = tweetsQ.poll(120, TimeUnit.SECONDS );
			if ( tweet == null ) {
				System.out.println(	"timedout polling TweetsQ -  exiting..." );
				break;
			}

			ps.setString(1, getTweetText(tweet));
			ps.setObject(2, toDate((String)tweet.get("created_at")));
			ps.setString(3, getHashTags(tweet));
			ps.setString(4, getUsername(tweet));
			GeoLocation geo = getGeoLocation(tweet);

			ps.setString(5, geo.geotype);
			ps.setDouble(6, geo.lat1);
			ps.setDouble(7, geo.long1);
			ps.setDouble(8, geo.lat2);
			ps.setDouble(9, geo.long2);
			ps.setDouble(10, geo.lat3);
			ps.setDouble(11, geo.long3);
			ps.setDouble(12, geo.lat4);
			ps.setDouble(13, geo.long4);


			try {
				ps.execute();
			}
			catch(java.sql.SQLException e) {
				e.printStackTrace();
				// continue
			}
		}
	}

	static class GeoLocation {
		public String 	geotype;
		public double 	lat1, long1, 
						lat2, long2, 
						lat3, long3, 
						lat4, long4;
	}

	static SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");                                          
	Object toDate(String dt) throws Exception {
		java.util.Date ts ;
		if ( dt == null )
			ts = Calendar.getInstance().getTime();
		else
			ts = sdf.parse(dt);

		java.sql.Timestamp d  = new java.sql.Timestamp(ts.getTime());
		return d;
	}

	static class TweetSpout implements Runnable {

		final BlockingQueue<Map<String,Object>> tweetsQ;
		final String keywords;
		final String locations;
		public TweetSpout(BlockingQueue<Map<String,Object>> q, final String keywords, final String locations ) {
			tweetsQ = q;
			this.keywords = keywords;
			this.locations = locations;
		}

		public void run() {

			int retryCount = 0;

			int tweetCnt = 0;
			while ( true ) {
				DefaultHttpClient httpClient = new DefaultHttpClient();
				try {
					UsernamePasswordCredentials creds = new UsernamePasswordCredentials("jackbedeveloper", "#winning#");
					httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, creds);
					HttpPost httpPost = new HttpPost(TWITTER_STREAM_URL);

					StringEntity postEntity;
					
					if ( locations == null )
						postEntity = new StringEntity("track="+keywords, "UTF-8");
					else if ( keywords == null ) 
						postEntity = new StringEntity("locations="+locations, "UTF-8");
					else
						postEntity = new StringEntity("track="+keywords+"locations="+locations, "UTF-8");

					postEntity.setContentType("application/x-www-form-urlencoded");
					httpPost.setEntity(postEntity);
				
					HttpResponse httpResponse = httpClient.execute(httpPost);
					System.out.println(	"connection established. tracking tweets... " );
					HttpEntity entity = httpResponse.getEntity();
					if (entity != null) {
						InputStream instream = entity.getContent();
						String jsonTweet;
						BufferedReader br = new BufferedReader(new InputStreamReader(instream));
						while( true ) {
							jsonTweet = br.readLine();
							if(jsonTweet != null) {                        
								Map<String,Object> tweet = getTweet(new StringReader(jsonTweet));
								if ( tweet == null ) continue;

								tweetsQ.put(tweet);
								tweetCnt++;

								log("tweet cnt : " + tweetCnt + " Q pending : " + tweetsQ.size() );
							}
						}
					}
				} catch (java.net.SocketException e) {
					if ( ++retryCount <= 5 ) {
						// increase delay between subsequent network retries
						try { Thread.sleep(retryCount * 5000 ); } catch (Exception t) {} 
						System.out.println(	e.getMessage() + " : retry connecting : " + retryCount );
						continue;
					}
					throw new RuntimeException ("Repeated network failure - sorry, terminating connection .: " + e.getMessage(), e);
				} catch (Exception ioe) {
					ioe.printStackTrace();
					throw new RuntimeException(ioe.getMessage(), ioe);
				}
				finally{
					httpClient.getConnectionManager().shutdown();
				}
			}

		}

	}

	static public Map<String,Object> getTweet(Reader reader) throws Exception {

		try {
			JsonFactory f = new JsonFactory();
			JsonParser jp = f.createJsonParser(reader);

			ObjectMapper om = new ObjectMapper();
			Map<String, Object> data = om.readValue(reader, Map.class );
			return data;
		}
		catch(Exception e) {
			e.printStackTrace();			
			System.out.println(	"Error parsing tweet. Skipping to next one ");
			return null;
		}
	}

	String getHashTags(Map<String,Object> data) {
		Map entities = (Map)data.get("entities" );
		if ( entities == null ) return "";
		List hashtags = (List)entities.get("hashtags");
		StringBuilder sb = new StringBuilder(50);
		for ( Object o : hashtags ) {
			Map hashtag = (Map)o;
			sb.append("#").append( hashtag.get("text") ).append(" ");
		}
		return sb.toString().trim();
	}

	String getUsername(Map<String,Object> data) {
			Map user = (Map)data.get("user");
			if ( user == null )
				return null;
			return ((String)user.get("name")).trim();
	}

	String getTweetText(Map<String, Object> data) {
			String txt = (String)data.get("text");
			if ( txt != null )
				return txt.replaceAll("0x1C","");	// unicode char cleanup
			return "";
	}

	GeoLocation getGeoLocation(Map<String, Object>data) {
		GeoLocation geo = new GeoLocation();

		boolean location = false;
		if ( data.containsKey("place") ) {
			Map place  = (Map)data.get("place");
			if ( place != null ) {
				Map boundingBox = (Map)place.get("bounding_box");
				if ( boundingBox != null ) {
					geo.geotype = (String)boundingBox.get("type");	
					List<List<List<Double>>> cords = (List<List<List<Double>>>) boundingBox.get("coordinates");
					geo.lat1  = cords.get(0).get(0).get(0);
					geo.long1 = cords.get(0).get(0).get(1);
					geo.lat2  = cords.get(0).get(1).get(0);
					geo.long2 = cords.get(0).get(1).get(1);
					geo.lat3  = cords.get(0).get(2).get(0);
					geo.long3 = cords.get(0).get(2).get(1);
					geo.lat4  = cords.get(0).get(3).get(0);
					geo.long4 = cords.get(0).get(3).get(1);
					location = true;
				}
			}
		}
		if ( !location ) {
			if ( data.containsKey("coordinates") ) {
				Map cords = (Map)data.get("coordinates");
				if ( cords != null ) {
					geo.geotype = (String)cords.get("type");
					List<Double> ll = (List<Double>)cords.get("coordinates");
					geo.lat1 = ll.get(0);
					geo.long1 = ll.get(1);
				}
			}
		}
		return geo;
	}

	static void log(String msg) {
		System.out.println(	msg );
	}
}
