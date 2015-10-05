import java.io.Serializable;

public class WebSession implements Serializable {
	private String IP;
	private long timestamp;
	private String date;
	private String url;
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getIP() {
		return IP;
	}

	public void setIP(String iP) {
		IP = iP;
	}

	public long getTimeStamp() {
		return timestamp;
	}

	public void setTimeStamp(long time) {
		this.timestamp = time;
	}

}
