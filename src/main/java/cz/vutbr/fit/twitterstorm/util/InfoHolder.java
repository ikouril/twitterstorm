package cz.vutbr.fit.twitterstorm.util;


import com.google.common.collect.Multiset;

/**
 * InfoHolder is a class designed for holding data of tweets, which arrive in random order,
 * after every variable is filled with value, tweet can be indexed
 * @author ikouril
 */
public class InfoHolder {
	private Tweet tweet;
	private String gender;
	private String sentiment;
	private String persons;
	private Multiset<String> keywords;
	
	
	/**
	* Creates an InfoHolder object
	*/
	public InfoHolder(){
		tweet=null;
		gender=null;
		sentiment=null;
		persons=null;
		keywords=null;
	}
	
	/**
	 * Checks whether tweet is filled with complete information
	 */
	public boolean isComplete(){
		return tweet!=null && gender!=null && sentiment!=null && persons!=null && keywords!=null;
	}
	
	/**
	 * Constructs result
	 * @return Tweet the resulting object constructed from complete data
	 */
	public Tweet getResult(){
		tweet.setGender(gender);
		tweet.setSentiment(sentiment);
		tweet.setPerson(persons);
		tweet.setKeywords(keywords);
		return tweet;
	}
	
	public Tweet getTweet() {
		return tweet;
	}
	public void setTweet(Tweet tweet) {
		this.tweet = tweet;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getSentiment() {
		return sentiment;
	}
	public void setSentiment(String sentiment) {
		this.sentiment = sentiment;
	}
	public String getPersons() {
		return persons;
	}
	public void setPersons(String persons) {
		this.persons = persons;
	}
	public Multiset<String> getKeywords() {
		return keywords;
	}
	public void setKeywords(Multiset<String> keywords) {
		this.keywords = keywords;
	}

}
