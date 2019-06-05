/**
 * 
 */
package com.shtick.apps.sh.driver.sqlite;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.shtick.apps.sh.core.Answer;
import com.shtick.apps.sh.core.AnswerID;
import com.shtick.apps.sh.core.Driver;
import com.shtick.apps.sh.core.Question;
import com.shtick.apps.sh.core.QuestionID;
import com.shtick.apps.sh.core.Quiz;
import com.shtick.apps.sh.core.QuizDesign;
import com.shtick.apps.sh.core.QuizDesignID;
import com.shtick.apps.sh.core.QuizDesignSubject;
import com.shtick.apps.sh.core.QuizID;
import com.shtick.apps.sh.core.Subject;
import com.shtick.apps.sh.core.SubjectQuestionGenerator;
import com.shtick.apps.sh.core.User;
import com.shtick.apps.sh.core.UserID;
import com.shtick.apps.sh.core.content.Marshal;
import com.shtick.apps.sh.driver.sqlite.bundle.DriverActivator;
import com.shtick.util.tokenizers.TokenTree;
import com.shtick.util.tokenizers.json.JSONToken;
import com.shtick.util.tokenizers.json.JSONTokenizer;
import com.shtick.util.tokenizers.json.NumberToken;
import com.shtick.util.tokenizers.json.ObjectPropertyToken;
import com.shtick.util.tokenizers.json.ObjectToken;
import com.shtick.utils.data.json.JSONDecoder;
import com.shtick.utils.data.json.JSONEncoder;

/**
 * @author sean.cox
 *
 */
public class SQLiteDriver implements Driver {
	private static File WORKING_DIRECTORY;
	private static final String OS = (System.getProperty("os.name")).toUpperCase();
	private static final String DB_URL;
	private static final Object DB_LOCK = new Object();
	private static final Random RANDOM = new Random();
	private static final JSONTokenizer jsonTokenizer = new JSONTokenizer();
	
	static{
		if (OS.contains("WIN")){
		    String workingDirectory = System.getenv("AppData");
		    WORKING_DIRECTORY = new File(workingDirectory);
		}
		else{ // Try Linux or related.
		    String workingDirectory = System.getProperty("user.home");
		    WORKING_DIRECTORY = new File(workingDirectory+"/Library/Application Support");
		    if(!WORKING_DIRECTORY.exists())
			    WORKING_DIRECTORY = new File(workingDirectory+"/.local/share");
		    if(!WORKING_DIRECTORY.exists())
			    WORKING_DIRECTORY = null;
		}
		if((WORKING_DIRECTORY==null)||(!WORKING_DIRECTORY.canWrite())){
			// TODO Find the current application folder.
		}
		
		DB_URL = "jdbc:sqlite:"+WORKING_DIRECTORY.toString()+"/sh.quiz.db";
	}
	
	/**
	 * 
	 */
	public SQLiteDriver() {
		try{
			Class.forName("org.sqlite.JDBC");
		}
		catch(ClassNotFoundException t){
			throw new RuntimeException(t);
		}
		
		synchronized(DB_LOCK){
			System.out.println("DB_URL:"+DB_URL);
			try (Connection connection = DriverManager.getConnection(DB_URL)) {
				// Build the database if necessary.
				if(connection==null)
					throw new RuntimeException("Connection to database cannot be established.");
				Statement statement = connection.createStatement();
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS users" +
		                   "(id TEXT PRIMARY KEY     NOT NULL," +
		                   " name           TEXT    NOT NULL," +
		                   " time_added     TEXT    NOT NULL" +
		                   ")");
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS quiz_designs" +
		                   "(id            TEXT PRIMARY KEY NOT NULL," +
		                   " user_id       TEXT    NOT NULL," +
		                   " title         TEXT    NOT NULL," +
		                   " subjects      TEXT   NOT NULL," +
		                   " min_questions INT   NOT NULL," +
		                   " max_questions INT   NOT NULL," +
		                   " time_added    TEXT    NOT NULL" +
		                   ")");
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS quizes" +
		                   "(id            TEXT PRIMARY KEY NOT NULL," +
		                   " user_id       TEXT    NOT NULL," +
		                   " time_added    TEXT    NOT NULL" +
		                   ")");
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS questions" +
		                   "(id            TEXT PRIMARY KEY NOT NULL," +
		                   " quiz_id       TEXT    NOT NULL," +
		                   " q_order       INT     NOT NULL," +
		                   " subject       TEXT    NOT NULL," +
		                   " prompt        TEXT    NOT NULL," +
		                   " prompt_format TEXT    NOT NULL," +
		                   " answer        TEXT    NOT NULL," +
		                   " answer_format TEXT    NOT NULL," +
		                   " answer_value  TEXT    NOT NULL," +
		                   " dimensions    TEXT    NOT NULL," +
		                   " points        INT     NOT NULL," +
		                   " time_added    TEXT    NOT NULL" +
		                   ")");
				statement.executeUpdate("CREATE TABLE IF NOT EXISTS answers" +
		                   "(id            TEXT PRIMARY KEY NOT NULL," +
		                   " question_id   TEXT    NOT NULL," +
		                   " answer_value  TEXT    NOT NULL," +
		                   " points        INT     NOT NULL," +
		                   " time_asked    TEXT    NOT NULL," +
		                   " time_answered TEXT    NOT NULL" +
		                   ")");
				connection.close();
			}
			catch(SQLException t){
				throw new RuntimeException(t);
			}
		}
	}

	@Override
	public UserID createUser(String username) throws IOException {
		String sql = "INSERT INTO users " +
                "(id, name, time_added) " +
				"VALUES (?,?,?)";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				String id = UUID.randomUUID().toString();
				statement.setString(1, id);
				statement.setString(2, username);
				statement.setString(3, ZonedDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
				statement.executeUpdate();
				return new UserID(id);
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	@Override
	public void updateUser(UserID userID, String username) throws IOException {
		String sql = "UPDATE users " +
                "SET name = ? " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, username);
				statement.setString(2, userID.toString());
				statement.executeUpdate();
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#deleteUser(com.shtick.apps.sh.core.UserID)
	 */
	@Override
	public void deleteUser(UserID userID) throws IOException {
		String sql = "DELETE FROM users " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, userID.toString());
				statement.executeUpdate();
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getUser(com.shtick.apps.sh.core.UserID)
	 */
	@Override
	public User getUser(UserID userID) throws IOException {
		String sql = "SELECT id, name, time_added " +
				"FROM users " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (
					Connection connection = DriverManager.getConnection(DB_URL);
					PreparedStatement statement = connection.prepareStatement(sql);
			) {
				statement.setString(1, userID.toString());
				ResultSet resultSet = statement.executeQuery();
				if(!resultSet.next())
					return null;
				return getUserFromResultSetRow(resultSet);
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getUsers()
	 */
	@Override
	public Collection<User> getUsers() throws IOException {
		String sql = "SELECT id, name, time_added " +
				"FROM users";
		synchronized(DB_LOCK){
			try (
					Connection connection = DriverManager.getConnection(DB_URL);
					PreparedStatement statement = connection.prepareStatement(sql);
			) {
				ResultSet resultSet = statement.executeQuery();
				LinkedList<User> retval = new LinkedList<>();
				while(resultSet.next())
					retval.add(getUserFromResultSetRow(resultSet));
				return retval;
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#createQuizDesign(com.shtick.apps.sh.core.QuizDesign)
	 */
	@Override
	public QuizDesignID createQuizDesign(QuizDesign quizDesign) throws IOException {
		String sql = "INSERT INTO quiz_designs " +
                "(id, user_id, title, subjects, min_questions, max_questions, time_added) " +
				"VALUES (?,?,?,?,?,?,?)";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				String id = UUID.randomUUID().toString();
				statement.setString(1, id);
				statement.setString(2, quizDesign.getUserID().toString());
				statement.setString(3, quizDesign.getTitle());
				{
					ArrayList<Map<String,Object>> subjects=new ArrayList<>(quizDesign.getSubjects().size());
					for(QuizDesignSubject subject:quizDesign.getSubjects())
						subjects.add(Marshal.marshal(subject));
					statement.setString(4, JSONEncoder.encode(subjects.toArray()));
				}
				statement.setInt(5, quizDesign.getMinQuestions());
				statement.setInt(6, quizDesign.getMaxQuestions());
				statement.setString(7, ZonedDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
				statement.executeUpdate();
				return new QuizDesignID(id);
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#saveQuizDesign(com.shtick.apps.sh.core.QuizDesign)
	 */
	@Override
	public void updateQuizDesign(QuizDesign quizDesign) throws IOException {
		String sql = "UPDATE quiz_designs " +
                "SET user_id = ?, title = ?, subjects = ?, min_questions = ?, max_questions = ? " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(6, quizDesign.getQuizDesignID().toString());
				statement.setString(1, quizDesign.getUserID().toString());
				statement.setString(2, quizDesign.getTitle());
				{
					ArrayList<Map<String,Object>> subjects=new ArrayList<>(quizDesign.getSubjects().size());
					for(QuizDesignSubject subject:quizDesign.getSubjects())
						subjects.add(Marshal.marshal(subject));
					statement.setString(3, JSONEncoder.encode(subjects.toArray()));
				}
				statement.setInt(4, quizDesign.getMinQuestions());
				statement.setInt(5, quizDesign.getMaxQuestions());
				statement.executeUpdate();
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getQuizDesigns(com.shtick.apps.sh.core.UserID)
	 */
	@Override
	public Collection<QuizDesign> getQuizDesigns(UserID userID) throws IOException {
		String sql = "SELECT id, user_id, title, subjects, min_questions, max_questions, time_added " +
				"FROM quiz_designs " +
				"WHERE user_id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, userID.toString());
				ResultSet resultSet = statement.executeQuery();
				LinkedList<QuizDesign> retval = new LinkedList<>();
				while(resultSet.next())
					retval.add(getQuizDesignFromResultSetRow(resultSet));
				return retval;
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getQuizDesign(com.shtick.apps.sh.core.QuizDesignID)
	 */
	@Override
	public QuizDesign getQuizDesign(QuizDesignID designID) throws IOException {
		String sql = "SELECT id, user_id, title, subjects, min_questions, max_questions, time_added " +
				"FROM quiz_designs " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, designID.toString());
				ResultSet resultSet = statement.executeQuery();
				if(!resultSet.next())
					return null;
				return getQuizDesignFromResultSetRow(resultSet);
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#deleteQuizDesign(com.shtick.apps.sh.core.QuizDesignID)
	 */
	@Override
	public void deleteQuizDesign(QuizDesignID designID) throws IOException {
		String sql = "DELETE FROM quiz_designs " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, designID.toString());
				statement.executeUpdate();
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getAllSubjects()
	 */
	@Override
	public Collection<Subject> getAllSubjects() throws IOException {
		Set<SubjectQuestionGenerator> generators = DriverActivator.SUBJECT_QUESTION_GENERATOR_TRACKER.getSubjectQuestionGenerators();
		ArrayList<Subject> retval = new ArrayList<>(generators.size());
		for(SubjectQuestionGenerator generator:generators) 
			retval.add(generator.getSubject());
		return retval;
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#generateQuiz(com.shtick.apps.sh.core.QuizDesignID)
	 */
	@Override
	public Quiz generateQuiz(QuizDesignID quizDesignID) throws IOException {
		QuizDesign quizDesign = getQuizDesign(quizDesignID);

		// Check inputs
		if((quizDesign.getMinQuestions()<=0)||(quizDesign.getMinQuestions()>quizDesign.getMaxQuestions()))
			throw new IllegalArgumentException("Quiz design has invalid min/max question count specified.");
		Set<QuizDesignSubject> subjects = quizDesign.getSubjects();
		if(subjects.size()==0)
			throw new IllegalArgumentException("Quiz design has no subjects specified.");
		
		// Set up question count for each subject.
		int questionCount=0;
		QuizDesignSubject[] subjectArray = subjects.toArray(new QuizDesignSubject[subjects.size()]);
		int[] subjectQuestionCounts = new int[subjectArray.length];
		LinkedList<Integer> optionalSubjects = new LinkedList<>();
		{
			if(quizDesign.getMinQuestions()==quizDesign.getMaxQuestions())
				questionCount=quizDesign.getMinQuestions();
			else
				questionCount=RANDOM.nextInt(quizDesign.getMaxQuestions()-quizDesign.getMinQuestions()+1)+quizDesign.getMinQuestions();
			if(subjectArray.length==1){
				subjectQuestionCounts[0]=questionCount;
			}
			else{
				int questionTally = 0;
				for(int i=0;i<subjectQuestionCounts.length;i++) {
					subjectQuestionCounts[i]=subjectArray[i].getMinQuestions();
					questionTally+=subjectQuestionCounts[i];
					for(int o=0;o<subjectArray[i].getMaxQuestions()-subjectArray[i].getMinQuestions();o++)
						optionalSubjects.add(i);
				}
				
				while((optionalSubjects.size()>0)&&(questionTally<questionCount)) {
					Integer optionalQuestion = optionalSubjects.remove(RANDOM.nextInt(optionalSubjects.size()));
					subjectQuestionCounts[optionalQuestion]++;
					questionTally++;
				}
			}
		}
		
		QuizID quizID=new QuizID(UUID.randomUUID().toString());
		Quiz quiz = new Quiz(quizID, quizDesign.getUserID(), ZonedDateTime.now());
		
		ArrayList<Question> questions = new ArrayList<>(questionCount);
		for(int i=0;i<subjectArray.length;i++){
			if(subjectQuestionCounts[i]<=0)
				continue;
			SubjectQuestionGenerator questionGenerator = DriverActivator.SUBJECT_QUESTION_GENERATOR_TRACKER.getSubjectQuestionGenerator(subjectArray[i].getSubject());
			if(questionGenerator == null)
				throw new RuntimeException("Unknown subject: "+subjectArray[i].toString());
			Collection<Question> generatedQuestions = questionGenerator.generateQuestions(this, quizDesign.getUserID(), subjectQuestionCounts[i]);
			if(generatedQuestions.size()!=subjectQuestionCounts[i])
				throw new RuntimeException("Question generator did not return the specified number of questions: Subject="+subjectArray[i]+", Expected="+subjectQuestionCounts[i]+", Returned="+generatedQuestions.size());
			for(Question generatedQuestion:generatedQuestions)
				questions.add(new Question(new QuestionID(UUID.randomUUID().toString()), quizID, subjectArray[i].getSubject(), generatedQuestion,ZonedDateTime.now()));
		}
		
		Collections.shuffle(questions);
		
		// Save generated quiz
		saveQuiz(quiz);
		saveQuizQuestions(questions);
		
		return quiz;
	}
	
	private static void saveQuiz(Quiz quiz) throws IOException{
		String sql = "INSERT INTO quizes " +
                "(id, user_id, time_added) " +
				"VALUES (?,?,?)";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, quiz.getQuizID().toString());
				statement.setString(2, quiz.getUserID().toString());
				statement.setString(3, quiz.getTimeAdded().format(DateTimeFormatter.ISO_DATE_TIME));
				statement.executeUpdate();
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}
	
	private static void saveQuizQuestions(Collection<Question> questions) throws IOException{
		int i=0;
		for(Question question:questions){
			saveQuizQuestion(question,i);
			i++;
		}
	}
	
	private static void saveQuizQuestion(Question question, int order) throws IOException{
		String sql = "INSERT INTO questions " +
                "(id, quiz_id, q_order, subject, prompt, prompt_format, answer, answer_format, answer_value, dimensions, points, time_added) " +
				"VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, question.getQuestionID().toString());
				statement.setString(2, question.getQuizID().toString());
				statement.setInt(3, order);
				statement.setString(4, question.getSubject().toString());
				statement.setString(5, question.getPrompt());
				statement.setString(6, question.getPromptType());
				statement.setString(7, question.getAnswerPrompt());
				statement.setString(8, question.getAnswerPromptType());
				statement.setString(9, question.getAnswerValue());
				statement.setString(10, JSONEncoder.encode(question.getQuestionDimensions()));
				statement.setInt(11, question.getPoints());
				statement.setString(12, question.getTimeAdded().format(DateTimeFormatter.ISO_DATE_TIME));
				statement.executeUpdate();
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getQuiz(com.shtick.apps.sh.core.QuizID)
	 */
	@Override
	public Quiz getQuiz(QuizID quizID) throws IOException {
		String sql = "SELECT id, user_id, time_added " +
				"FROM quizes " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, quizID.toString());
				ResultSet resultSet = statement.executeQuery();
				if(!resultSet.next())
					return null;
				return getQuizFromResultSetRow(resultSet);
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getUserQuizes(com.shtick.apps.sh.core.UserID)
	 */
	@Override
	public Collection<Quiz> getUserQuizes(UserID userID) throws IOException {
		String sql = "SELECT id, user_id, time_added " +
				"FROM quizes " +
				"WHERE user_id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, userID.toString());
				ResultSet resultSet = statement.executeQuery();
				LinkedList<Quiz> retval = new LinkedList<>();
				while(resultSet.next())
					retval.add(getQuizFromResultSetRow(resultSet));
				return retval;
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#deleteQuiz(com.shtick.apps.sh.core.QuizID)
	 */
	@Override
	public void deleteQuiz(QuizID quizID) throws IOException {
		String sql = "DELETE FROM quizes " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, quizID.toString());
				statement.executeUpdate();
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
		Collection<Question> questions = getQuizQuestions(quizID);
		// Delete answers.
		for(Question question:questions){
			sql = "DELETE FROM answers " +
					"WHERE question_id = ?";
			synchronized(DB_LOCK){
				try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
					statement.setString(1, question.getQuestionID().toString());
					statement.executeUpdate();
				}
				catch(SQLException t){
					throw new IOException(t);
				}
			}
		}
		// Delete questions.
		sql = "DELETE FROM questions " +
				"WHERE quiz_id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, quizID.toString());
				statement.executeUpdate();
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getQuizQuestion(com.shtick.apps.sh.core.QuestionID)
	 */
	@Override
	public Question getQuizQuestion(QuestionID questionID) throws IOException {
		String sql = "SELECT id, quiz_id, q_order, subject, prompt, prompt_format, answer, answer_format, answer_value, dimensions, points, time_added " +
				"FROM questions " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, questionID.toString());
				ResultSet resultSet = statement.executeQuery();
				if(!resultSet.next())
					return null;
				return getQuestionFromResultSetRow(resultSet);
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getQuizQuestions(com.shtick.apps.sh.core.QuizID)
	 */
	@Override
	public Collection<Question> getQuizQuestions(QuizID quizID) throws IOException {
		String sql = "SELECT id, quiz_id, q_order, subject, prompt, prompt_format, answer, answer_format, answer_value, dimensions, points, time_added " +
				"FROM questions " +
				"WHERE quiz_id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, quizID.toString());
				ResultSet resultSet = statement.executeQuery();
				LinkedList<Question> retval = new LinkedList<>();
				while(resultSet.next())
					retval.add(getQuestionFromResultSetRow(resultSet));
				return retval;
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getUserSubjectQuestions(com.shtick.apps.sh.core.UserID, com.shtick.apps.sh.core.Subject)
	 */
	@Override
	public Collection<Question> getUserSubjectQuestions(UserID userID, Subject subject) throws IOException {
		String sql = "SELECT qs.id, qs.quiz_id, qs.q_order, qs.subject, qs.prompt, qs.prompt_format, qs.answer, qs.answer_format, qs.answer_value, qs.dimensions, qs.points, qs.time_added " +
				"FROM questions qs, quizes qz" +
				"WHERE qz.user_id = ? AND qs.subject = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, userID.toString());
				statement.setString(2, subject.toString());
				ResultSet resultSet = statement.executeQuery();
				LinkedList<Question> retval = new LinkedList<>();
				while(resultSet.next())
					retval.add(getQuestionFromResultSetRow(resultSet));
				return retval;
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#saveAnswer(com.shtick.apps.sh.core.QuestionID, java.lang.Stringt, java.time.ZonedDateTime)
	 */
	@Override
	public AnswerID saveAnswer(Question question, String answer, ZonedDateTime timeAsked)
			throws IOException {
		SubjectQuestionGenerator questionGenerator = DriverActivator.SUBJECT_QUESTION_GENERATOR_TRACKER.getSubjectQuestionGenerator(question.getSubject());
		if(questionGenerator==null)
			throw new RuntimeException("Question generator for question not found.");
		int points = questionGenerator.getAnswerScore(question, answer);
		ZonedDateTime timeAnswered = ZonedDateTime.now();

		String sql = "INSERT INTO answers " +
                "(id, question_id, answer_value, points, time_asked, time_answered) " +
				"VALUES (?,?,?,?,?,?)";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				String id = UUID.randomUUID().toString();
				statement.setString(1, id);
				statement.setString(2, question.getQuestionID().toString());
				statement.setString(3, answer);
				statement.setInt(4, points);
				statement.setString(5, timeAsked.format(DateTimeFormatter.ISO_DATE_TIME));
				statement.setString(6, timeAnswered.format(DateTimeFormatter.ISO_DATE_TIME));
				statement.executeUpdate();
				return new AnswerID(id);
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getAnswer(com.shtick.apps.sh.core.AnswerID)
	 */
	@Override
	public Answer getAnswer(AnswerID answerID) throws IOException {
		String sql = "SELECT id, question_id, answer_value, points, time_asked, time_answered " +
				"FROM answers " +
				"WHERE id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, answerID.toString());
				ResultSet resultSet = statement.executeQuery();
				if(!resultSet.next())
					return null;
				return getAnswerFromResultSetRow(resultSet);
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getQuestionAnswers(com.shtick.apps.sh.core.QuestionID)
	 */
	@Override
	public Collection<Answer> getQuestionAnswers(QuestionID questionID) throws IOException {
		String sql = "SELECT id, question_id, answer_value, points, time_asked, time_answered " +
				"FROM answers " +
				"WHERE question_id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, questionID.toString());
				ResultSet resultSet = statement.executeQuery();
				LinkedList<Answer> retval = new LinkedList<>();
				while(resultSet.next())
					retval.add(getAnswerFromResultSetRow(resultSet));
				return retval;
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.shtick.apps.sh.core.Driver#getLatestAnswer(com.shtick.apps.sh.core.QuestionID)
	 */
	@Override
	public Answer getLatestAnswer(QuestionID questionID) throws IOException {
		String sql = "SELECT id, question_id, answer_value, points, time_asked, time_answered " +
				"FROM answers " +
				"WHERE question_id = ?";
		synchronized(DB_LOCK){
			try (Connection connection = DriverManager.getConnection(DB_URL);PreparedStatement statement = connection.prepareStatement(sql);) {
				statement.setString(1, questionID.toString());
				ResultSet resultSet = statement.executeQuery();
				Answer retval = null;
				Answer answer = null;
				while(resultSet.next()){
					answer=getAnswerFromResultSetRow(resultSet);
					if((retval==null)||(answer.getTimeAnswered().isAfter(retval.getTimeAnswered())))
						retval = answer;
				}
				return retval;
			}
			catch(SQLException t){
				throw new IOException(t);
			}
		}
	}

	private static QuizDesign getQuizDesignFromResultSetRow(ResultSet resultSet) throws SQLException{
		Object decodedJSON = JSONDecoder.decode(resultSet.getString(4), null);
		if(!(decodedJSON instanceof List))
			throw new IllegalArgumentException("Decoded JSON was not a list.");
		List<Object> dbDesignSubjects = (List<Object>)decodedJSON;
		HashSet<QuizDesignSubject> quizDesignSubjects = new HashSet<>(dbDesignSubjects.size());
		Map<String,Object> mapDesignSubject;
		for(Object dbDesignSubject:dbDesignSubjects) {
			if(!(dbDesignSubject instanceof Map))
				throw new IllegalArgumentException("Decoded design subject was not a Map.");
			quizDesignSubjects.add(Marshal.unmarshalQuizDesignSubject((Map<String,Object>)dbDesignSubject));
		}
		return new QuizDesign(
				new QuizDesignID(resultSet.getString(1)),
				new UserID(resultSet.getString(2)),
				resultSet.getString(3),
				quizDesignSubjects,
				resultSet.getInt(5),
				resultSet.getInt(6),
				ZonedDateTime.parse(resultSet.getString(7), DateTimeFormatter.ISO_DATE_TIME)
				);
	}

	private static User getUserFromResultSetRow(ResultSet resultSet) throws SQLException{
		return new User(
				new UserID(resultSet.getString(1)),
				resultSet.getString(2),
				ZonedDateTime.parse(resultSet.getString(3), DateTimeFormatter.ISO_DATE_TIME)
				);
	}

	private static Quiz getQuizFromResultSetRow(ResultSet resultSet) throws SQLException{
		return new Quiz(
				new QuizID(resultSet.getString(1)),
				new UserID(resultSet.getString(2)),
				ZonedDateTime.parse(resultSet.getString(3), DateTimeFormatter.ISO_DATE_TIME)
				);
	}

	/**
	 * ResultSet rows are expected to have the following ordered columns:
	 * id, quiz_id, subject, prompt, prompt_format, answer, answer_format, answer_value, dimensions, points, time_added
	 * 
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 */
	private static Question getQuestionFromResultSetRow(ResultSet resultSet) throws SQLException{
		Map<String,Float> dimensions = parseJSONMap(resultSet.getString(10));
		return new Question(
				new QuestionID(resultSet.getString(1)),
				new QuizID(resultSet.getString(2)),
				resultSet.getInt(3),
				new Subject(resultSet.getString(4)),
				resultSet.getString(5),
				resultSet.getString(6),
				resultSet.getString(7),
				resultSet.getString(8),
				resultSet.getString(9),
				dimensions,
				resultSet.getInt(11),
				ZonedDateTime.parse(resultSet.getString(12), DateTimeFormatter.ISO_DATE_TIME)
				);
	}

	/**
	 * ResultSet rows are expected to have the following ordered columns:
	 * id, question_id, answer_value, points, time_asked, time_answered
	 * 
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 */
	private static Answer getAnswerFromResultSetRow(ResultSet resultSet) throws SQLException{
		return new Answer(
				new AnswerID(resultSet.getString(1)),
				new QuestionID(resultSet.getString(2)),
				resultSet.getString(3),
				resultSet.getInt(4),
				ZonedDateTime.parse(resultSet.getString(5), DateTimeFormatter.ISO_DATE_TIME),
				ZonedDateTime.parse(resultSet.getString(6), DateTimeFormatter.ISO_DATE_TIME)
				);
	}
	
	private static Map<String,Float> parseJSONMap(String jsonMap) throws IllegalArgumentException{
		TokenTree<JSONToken> tokenTree;
		jsonMap=jsonMap.trim();
		if(jsonMap.length()==0)
			return new HashMap<>();
		try{
			tokenTree = jsonTokenizer.tokenize(new StringReader(jsonMap));
		}
		catch(IOException t){
			throw new RuntimeException(t);
		}
		
		Iterator<JSONToken> iter = tokenTree.iterator();
		if(!iter.hasNext())
			return null;
		JSONToken token = iter.next();
		if(!(token instanceof ObjectToken))
			throw new IllegalArgumentException("Input was not a valid JSON encoded object.");
		if(iter.hasNext())
			throw new IllegalArgumentException("Extra data was found in the input.");
		if(tokenTree.getAllIssues().size()>0)
			throw new IllegalArgumentException("Syntax errors in JSON.");
		
		// Turn tokens into map.
		ObjectToken objectToken = (ObjectToken)token;
		List<ObjectPropertyToken> properties = objectToken.getObjectPropertyTokens();
		HashMap<String,Float> retval = new HashMap<>(properties.size());
		String label;
		JSONToken value;
		Float numericValue;
		for(ObjectPropertyToken property:properties){
			label=property.getLabel().getRepresentedString();
			value=property.getValue();
			if(!(value instanceof NumberToken))
				throw new IllegalArgumentException("Object contains non-numeric properties.");
			numericValue = Float.valueOf(((NumberToken)value).toString());
			retval.put(label,numericValue);
		}
		return retval;
	}
}
