import org.apache.spark.api.java.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Weblog {

	public static void main(String[] args) {

		HiveContext sqlContext = setupSpark();
		// sessions limit set to 30 minutes = 18000000 miliseconds
		// get all visits, and group by the same IP, and its previous visit. Calculate the difference of the 2 timestamps. If it's greater than 30 min, it's a new session.
		String innerSQL = "SELECT IP,timestamp, date, url,CASE WHEN ((timestamp - LAG(timestamp) OVER (PARTITION BY IP ORDER BY timestamp)) > 18000000) OR (LAG(timestamp) OVER (PARTITION BY IP ORDER BY timestamp) IS NULL) "
				+ "THEN 1 ELSE 0 END as is_new_session FROM websessions";
		// use the sum of is_new_session get a unique session ID per Session.
		String outerSQL = "SELECT SUM(is_new_session) OVER (PARTITION BY IP ORDER BY timestamp) AS IPSessionID, SUM(is_new_session) OVER (ORDER BY IP, timestamp) AS SessionID,IP,date,timestamp,url FROM ("
				+ innerSQL + ") as Inner ";
		// Sessionization: group all the sessions that is in the same ip and IPSessionID, get the session time by substracting the max and min of the timestamp in the window
		String sessionSQL = "SELECT (MAX(timestamp) - MIN(timestamp))/1000 AS SessionTimeInSec,IPSessionID, IP FROM ("
				+ outerSQL + " ) AS OUTER GROUP BY IPSessionID, IP ";

		DataFrame sessionized = sqlContext.sql(sessionSQL);
		System.out.println("Sessionized: ");
		sessionized.show(100);

		getAverageSessionTime(sessionized);

		getUniqueURLPerSession(sqlContext, outerSQL);

		getMostEnagedUsers(sessionized);
	}

	private static void getMostEnagedUsers(DataFrame sessionized) {
		DataFrame mostEnagedUsers = sessionized.groupBy("IP")
				.agg(org.apache.spark.sql.functions.sum("SessionTimeInSec"))
				.sort(org.apache.spark.sql.functions.desc("sum(SessionTimeInSec)"));
		System.out.println("Top Most enaged users are: ");
		mostEnagedUsers.show(100);
	}

	private static void getUniqueURLPerSession(HiveContext sqlContext, String outerSQL) {
		// get the unique URL per IP
		String uniqueURLPerSession = "SELECT COUNT(DISTINCT url) AS uniqueVisits,IPSessionID, IP FROM (" + outerSQL
				+ " ) AS OUTER GROUP BY IPSessionID, IP ";
		DataFrame uniqueURLdf = sqlContext.sql(uniqueURLPerSession);
		uniqueURLdf.show(100);
	}

	private static void getAverageSessionTime(DataFrame sessionized) {
		DataFrame averageSessionTime = sessionized
				.agg(org.apache.spark.sql.functions.avg(sessionized.col("SessionTimeInSec")));
		System.out.println("Average Session Time is: ");
		averageSessionTime.show(100);
	}

	private static HiveContext setupSpark() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		String logFile = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz";
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext sqlContext = new HiveContext(sc.sc());
		JavaRDD<String> logData = sc.textFile(logFile);

		JavaRDD<WebSession> websessions = logData.map(new Function<String, WebSession>() {
			public WebSession call(String line) throws Exception {
				String[] parts = line.split(" ");

				WebSession session = new WebSession();
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
				Date dateStr = formatter.parse(parts[0]);
				session.setTimeStamp(dateStr.getTime());
				String ip = parts[2];
				ip = removePortFromIP(ip);
				session.setIP(ip);
				session.setUrl(parts[11]);
				session.setDate(parts[0]);
				return session;
			}

		});
		DataFrame schemaSessions = sqlContext.createDataFrame(websessions, WebSession.class);
		schemaSessions.registerTempTable("websessions");
		return sqlContext;
	}
	private static String removePortFromIP(String ip) {
		if (ip != null && ip.indexOf(":") > 0) {
			ip = ip.substring(0, ip.indexOf(":"));
		}
		return ip;
	}

}
