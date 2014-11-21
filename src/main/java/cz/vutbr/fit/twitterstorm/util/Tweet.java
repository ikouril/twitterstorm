package cz.vutbr.fit.twitterstorm.util;

import java.io.Serializable;
import java.util.Date;

import com.google.common.collect.Multiset;

/**
 * Tweet is a class for holding tweet data
 * @author ikouril
 */
public class Tweet implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7953035489460083302L;
	
	private String text;
	private String author;
	private String person;
	private String game;
	private String sentiment;
	private String gender;
	private Multiset<String> keywords;
	private float score;
	private Date date;
	
	/**
	* Creates empty tweet object, which later formed as a result from a query to index
	*/
	public Tweet(){
		score=0.0F;
	}
	
	/**
	 * Creates new tweet object, which is after additional processing sent to index
	 * @param text the Tweet body
	 * @param author the username of tweet creator
	 * @param date the date, when tweet was created
	 */
	public Tweet(String text,String author,Date date){
		this.setText(text);
		this.setAuthor(author);
		this.setDate(date);
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}


	public String getPerson() {
		return person;
	}


	public void setPerson(String person) {
		this.person = person;
	}

	public String getGame() {
		return game;
	}

	public void setGame(String game) {
		this.game = game;
	}

	public float getScore() {
		return score;
	}

	public void setScore(float score) {
		this.score = score;
	}

	public Multiset<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(Multiset<String> keywords) {
		this.keywords = keywords;
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
	
	

}
