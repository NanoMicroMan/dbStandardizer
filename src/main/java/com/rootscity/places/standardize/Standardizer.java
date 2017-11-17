/*
 * Copyright 2012 Foundation for On-Line Genealogy, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rootscity.places.standardize;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.TreeMultimap;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import com.rootscity.common.DataBase;
import com.rootscity.common.Util;
import com.rootscity.common.stats;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import javax.sql.DataSource;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * User: dallan
 * Date: 1/10/12
 */
public class Standardizer {
	/**
	 * Standardization mode:
	 * BEST=get the closest matching place
	 * REQUIRED=match must include the left-most level or not at all,
	 * NEW=BEST+1 -- if you can't include the next level to the left, return a fake place with it as the name
	 */
	public static enum Mode {
		BEST, REQUIRED, NEW
	}

	;

	public static final int MAX_LEVELS = 4;
	public static final int TOP_LEVEL = 1;
	public static final int PLACE_CACHE_MAX_SIZE = 50000;
	public static final int PLACE_CACHE_MAX_SECONDS = 3600;
	public static final int WORD_CACHE_MAX_SIZE = 50000;
	public static final int WORD_CACHE_MAX_SECONDS = 3600;
	public static final String DB_DRIVER_CLASS = "com.mysql.jdbc.Driver";

	private static Logger logger = Logger.getLogger("places.standardize");
	private static int USA_ID = 1500;
	private static Standardizer standardizer = new Standardizer();

	public static Standardizer getInstance() {
		return standardizer;
	}

	@XmlRootElement
	public static class PlaceScore {
		private Place place;
		private double score;

		public PlaceScore(Place place, double score) {
			this.place = place;
			this.score = score;
		}

		public Place getPlace() {
			return place;
		}

		public double getScore() {
			return score;
		}
	}

	private static ComboPooledDataSource staticDS = null;

	private placeNormalizer pn = null;
	private Set<String> typeWords = null;
	private Map<String, String> abbreviations = null;
	private Set<String> noiseWords = null;

	private DB diskDB;
	private DataBase db;

	private Map<Integer, Place> placeIndex = null;
	private Map<String, int[]> wordIndex = null;

	private DataSource dataSource = null;
	private Set<Integer> largeCountries = null;
	private Set<Integer> mediumCountries = null;
	private double primaryMatchWeight = 0;
	private Double[] largeCountryLevelWeights = null;
	private Double[] mediumCountryLevelWeights = null;
	private Double[] smallCountryLevelWeights = null;
	private ErrorHandler errorHandler = null;

	LoadingCache<Integer, Place> placeCache = CacheBuilder.newBuilder()
			.maximumSize(PLACE_CACHE_MAX_SIZE)
			.expireAfterWrite(PLACE_CACHE_MAX_SECONDS, TimeUnit.SECONDS)
			.build(
					new CacheLoader<Integer, Place>() {
						public Place load(Integer id) {
							Connection conn = null;
							PreparedStatement ps = null;
							ResultSet rs = null;
							Place p = null;
							try {
								conn = dataSource.getConnection();
								ps = conn.prepareStatement("SELECT * FROM places WHERE id = ?");
								ps.setInt(1, id);
								rs = ps.executeQuery();
								if (rs.next()) {
									p = constructPlace(rs.getInt("id"), rs.getString("name"), rs.getString("alt_names"),
											rs.getString("types"), rs.getInt("located_in_id"), rs.getString("also_located_in_ids"),
											rs.getInt("level"), rs.getInt("country_id"), rs.getDouble("latitude"), rs.getDouble("longitude"),
											rs.getString("sources"));
								}
							}
							catch (SQLException e) {
								logger.severe("Error reading places: " + e);
							}
							finally {
								try {
									if (rs != null) {
										rs.close();
									}
									if (ps != null) {
										ps.close();
									}
									if (conn != null) {
										conn.close();
									}
								}
								catch (Exception e) {
									// ignore
								}
							}
							return p;
						}
					});

	LoadingCache<String, int[]> wordCache = CacheBuilder.newBuilder()
			.maximumSize(WORD_CACHE_MAX_SIZE)
			.expireAfterWrite(WORD_CACHE_MAX_SECONDS, TimeUnit.SECONDS)
			.build(
					new CacheLoader<String, int[]>() {
						public int[] load(String word) {
							Connection conn = null;
							PreparedStatement ps = null;
							ResultSet rs = null;
							int[] ids = null;
							try {
								conn = dataSource.getConnection();
								ps = conn.prepareStatement("SELECT ids FROM place_words WHERE word = ?");
								ps.setString(1, word);
								rs = ps.executeQuery();
								if (rs.next()) {
									ids = constructPlaceWords(rs.getString("ids"));
								} else {
									ids = new int[0];
								}
							}
							catch (SQLException e) {
								logger.severe("Error reading place_words: " + e);
							}
							finally {
								try {
									if (rs != null) {
										rs.close();
									}
									if (ps != null) {
										ps.close();
									}
									if (conn != null) {
										conn.close();
									}
								}
								catch (Exception e) {
									// ignore
								}
							}
							return ids;
						}
					});

	private Standardizer() {
		pn = placeNormalizer.getInstance();

		Reader indexReader = null;
		Reader matchCountsReader = null;

		try {
			// read properties
			Properties props = new Properties();
			props.load(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("standardizer.properties"), "UTF8"));

			// read type words
			typeWords = new HashSet<>(Arrays.asList(props.getProperty("typeWords").split(",")));

			// read abbreviations
			abbreviations = new HashMap<>();
			for (String abbrMap : props.getProperty("abbreviations").split(",")) {
				String[] fields = abbrMap.split("=");
				abbreviations.put(fields[0], fields[1]);
			}

			// read noise words
			noiseWords = new HashSet<>(Arrays.asList(props.getProperty("noiseWords").split(",")));

			// read large countries
			largeCountries = toIntegerSet(props.getProperty("largeCountries"));

			// read medium countries
			mediumCountries = toIntegerSet(props.getProperty("mediumCountries"));

			// read large country level weights
			largeCountryLevelWeights = toDoubleArray(props.getProperty("largeCountryLevelWeights"));

			// read large country level weights
			mediumCountryLevelWeights = toDoubleArray(props.getProperty("mediumCountryLevelWeights"));

			// read large country level weights
			smallCountryLevelWeights = toDoubleArray(props.getProperty("smallCountryLevelWeights"));

			primaryMatchWeight = Double.parseDouble(props.getProperty("primaryMatchWeight"));

			// initialize db
			String databaseUrl = System.getenv("DATABASE_URL");
			if (databaseUrl != null) {
				dataSource = getDataSource(databaseUrl);
			} else {
				//db = new DataBase(true);
				File dbFile = new File(getClass().getClassLoader().getResource("").toURI());
				dbFile = new File(dbFile, "places.db");
				Boolean create = !Util.decommpress(getClass().getClassLoader().getResourceAsStream("places.db.gz"), dbFile);

				if (create) {
					stats st = new stats("Creating map DB", -1L, 10000L);
					initDB(dbFile, false);
					indexReader = new InputStreamReader(getClass().getClassLoader().getResourceAsStream("place_words.tsv"), "UTF8");
					readWordIndex(indexReader, "\t", st);
					indexReader.close();
					indexReader = new InputStreamReader(getClass().getClassLoader().getResourceAsStream("places.tsv"), "UTF8");
					readPlaceIndex(indexReader, "\t", st);
					diskDB.close();
					File resFile = new File(dbFile.getPath().replaceAll("target.classes", "src/main/resources") + ".gz");
					Util.commpress(dbFile, resFile);
					st.cancel();
				}
				initDB(dbFile, true);
			}
		}
		catch (Exception e) {
			throw new RuntimeException("Error reading file:" + e.getMessage());
		}
		finally {
			try {
				if (matchCountsReader != null) {
					matchCountsReader.close();
				}
				if (indexReader != null) {
					indexReader.close();
				}
			}
			catch (IOException e) {
				// ignore
			}
		}
	}

	private void initDB(File dbFile, Boolean readonly) {
		if (readonly) {
			diskDB = DBMaker.fileDB(dbFile).fileMmapEnable().closeOnJvmShutdown().readOnly().make();
		} else {
			diskDB = DBMaker.fileDB(dbFile).fileMmapEnable().concurrencyDisable().closeOnJvmShutdown().make();
		}
		wordIndex = diskDB.hashMap("word")
				.keySerializer(Serializer.STRING)
				.valueSerializer(Serializer.INT_ARRAY).createOrOpen(); //new HashMap<String, Integer[]>();
		placeIndex = diskDB.hashMap("src/main/com/rootscity/places")
				.keySerializer(Serializer.INTEGER)
				.valueSerializer(Serializer.JAVA).createOrOpen(); // new HashMap<Integer, Place>();
	}

	private synchronized DataSource getDataSource(String url) {
		if (staticDS == null) {
			staticDS = new ComboPooledDataSource();
			try {
				Class.forName(DB_DRIVER_CLASS).newInstance();
				staticDS.setDriverClass(DB_DRIVER_CLASS);
			}
			catch (Exception e) {
				throw new RuntimeException("Error loading database driver: " + e.getMessage());
			}
			staticDS.setJdbcUrl(url);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						DataSources.destroy(staticDS);
					}
					catch (SQLException e) {
						// ignore
					}
				}
			});
		}
		return staticDS;
	}

	private Set<Integer> toIntegerSet(String value) {
		Set<Integer> result = new HashSet<>();
		for (String field : value.split(",")) {
			result.add(Integer.parseInt(field));
		}
		return result;
	}

	private Double[] toDoubleArray(String value) {
		String[] fields = value.split(",");
		Double[] result = new Double[fields.length];
		for (int i = 0; i < fields.length; i++) {
			result[i] = Double.parseDouble(fields[i]);
		}
		return result;
	}

	private int[] constructPlaceWords(String idString) {
		String[] idStrings = idString.split(",");
		int[] ids = new int[idStrings.length];
		for (int i = 0; i < idStrings.length; i++) {
			ids[i] = Integer.parseInt(idStrings[i]);
		}
		return ids;
	}

	/**
	 * Read the word index
	 * You would not normally call this function. Used in testing
	 */
	public void readWordIndex(Reader reader)
			throws IOException {
		readWordIndex(reader, "\\|", null);
	}

	public void readWordIndex(Reader reader, String sep, stats st)
			throws IOException {
		if (wordIndex == null) {
			wordIndex = new HashMap<>();
		}
		BufferedReader r = new BufferedReader(reader);
		String line;
		while ((line = r.readLine()) != null) {
			st.tick();
			String[] fields = line.split(sep);
			int[] ids = constructPlaceWords(fields[1]);
			wordIndex.put(fields[0], ids);
		}
	}

	private void setAltNames(Place p, String[] altNameStrings) {
		Place.AltName[] altNames = new Place.AltName[altNameStrings.length];
		for (int i = 0; i < altNameStrings.length; i++) {
			String altNameString = altNameStrings[i];
			Place.AltName altName;
			int pos = altNameString.indexOf(':');
			if (pos > 0) {
				altName = new Place.AltName(altNameString.substring(0, pos), altNameString.substring(pos + 1));
			} else {
				altName = new Place.AltName(altNameString, null);
			}
			altNames[i] = altName;
		}
		p.setAltNames(altNames);
	}

	private void setSources(Place p, String[] sourceStrings) {
		Place.Source[] sources = new Place.Source[sourceStrings.length];
		for (int i = 0; i < sourceStrings.length; i++) {
			String sourceString = sourceStrings[i];
			Place.Source source;
			int pos = sourceString.indexOf(':');
			if (pos > 0) {
				source = new Place.Source(sourceString.substring(0, pos), sourceString.substring(pos + 1));
			} else {
				source = new Place.Source(sourceString, null);
			}
			sources[i] = source;
		}
		p.setSources(sources);
	}

	private Place constructPlace(int id, String name, String altNames, String types, int locatedInId, String alsoLocatedInIds,
	                             int level, int countryId, double latitude, double longitude, String sources) {
		Place p = new Place();
//      p.setStandardizer(this);
		p.setId(id);
		p.setName(name);
		if (altNames.length() > 0) {
			setAltNames(p, altNames.split("~"));
		}
		if (types.length() > 0) {
			p.setTypes(types.split("~"));
		}
		p.setLocatedInId(locatedInId);
		if (alsoLocatedInIds.length() > 0) {
			String[] idStrings = alsoLocatedInIds.split("~");
			int[] ids = new int[idStrings.length];
			for (int i = 0; i < idStrings.length; i++) {
				ids[i] = Integer.parseInt(idStrings[i]);
			}
			p.setAlsoLocatedInIds(ids);
		}
		p.setLevel(level);
		p.setCountryId(countryId);
		p.setLatitude(latitude);
		p.setLongitude(longitude);
		if (sources.length() > 0) {
			setSources(p, sources.split("~"));
		}
		return p;
	}

	/**
	 * Read the place index
	 * You would not normally call this function. Used in testing
	 */
	public void readPlaceIndex(Reader reader)
			throws IOException {
		readPlaceIndex(reader, "\\|", null);
	}

	public void readPlaceIndex(Reader reader, String sep, stats st)
			throws IOException {
		if (placeIndex ==null) {
			placeIndex = new HashMap<>();
		}
		BufferedReader r = new BufferedReader(reader);
		String line;
		while ((line = r.readLine()) != null) {
			st.tick();
			String[] fields = line.split(sep);
			Place p = constructPlace(
					Integer.parseInt(fields[0]), // id
					fields[1], // name
					fields[2], // altNames
					fields[3], // types
					Integer.parseInt(fields[4]), // located in id
					fields[5], // also located in ids
					Integer.parseInt(fields[6]), // level
					Integer.parseInt(fields[7]), // country id
					fields.length > 8 && fields[8].length() > 0 ? Double.parseDouble(fields[8]) : 0.0,
					fields.length > 9 && fields[9].length() > 0 ? Double.parseDouble(fields[9]) : 0.0,
					fields.length > 10 && fields[10].length() > 0 ? fields[10] : "");
			placeIndex.put(p.getId(), p);
		}
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	// return null if word not found
	private List<Integer> lookupWord(String word) {
		int[] ids = null;
		if (db != null) {
			Statement s = null;
			try {
				List<Integer> res = new ArrayList<>();
				s = db.getConnection().createStatement();
				ResultSet rs = s.executeQuery("select ids FROM place_words WHERE word='" + word + "'");
				while (rs.next()) {
					String val = rs.getString(0);
					for (String id : val.split(",")) {
						res.add(new Integer(id));
					}
				}
				return res.isEmpty() ? null : res;
			}
			catch (Exception e) {
			}
			finally {
				try {
					s.close();
				}
				catch (SQLException e) {
				}
			}
		} else if (wordIndex != null) {
			ids = wordIndex.get(word);
		} else {
			try {
				ids = wordCache.get(word);
			}
			catch (ExecutionException e) {
				logger.severe("Error loading place words: " + e);
			}
		}
		if (ids != null && ids.length > 0) {
			return Arrays.stream(ids).boxed().collect(Collectors.toList());
		}
		return null;
	}

	public Place getPlace(int id) {
		Place p = null;
		if (db != null) {
			Statement s = null;
			try {
				s = db.getConnection().createStatement();
				ResultSet rs = s.executeQuery("select * FROM places WHERE id=" + id);
				while (rs.next()) {
					p = new Place();
					p.setId(rs.getInt("id"));
					p.setName(rs.getString("name"));
					String str = rs.getString("alt_names");
					if (!StringUtils.isEmpty(str)) {
						for (String ss : str.split("~")) {
							String sss[] = ss.split(":");
							p.addAltName(sss[1], sss[2]);
						}

					}
					p.setLocatedInId(rs.getInt("located_in_id"));
					String alii = rs.getString("also_located_in_ids");
					if (!alii.isEmpty()) {
						int a[] = new int[alii.split("~").length];
						int x = 0;
						for (String xx : alii.split("~")) {
							a[x++] = new Integer(xx);
						}
						p.setAlsoLocatedInIds(a);

					}
					p.setCountryId(rs.getInt("country_id"));
					p.setLatitude(rs.getDouble("latitude"));
					p.setLongitude(rs.getDouble("longitude"));
					p.setLevel(rs.getInt("level"));
					str = rs.getString("types");
					if (!StringUtils.isEmpty(str)) {
						p.setTypes(str.split("~"));
					}

					return p;
				}
				return null;
			}
			catch (Exception e) {
			}
			finally {
				try {
					s.close();
				}
				catch (SQLException e) {
				}
			}
		} else if (placeIndex != null) {
			p = placeIndex.get(id);
		} else {
			try {
				p = placeCache.get(id);
			}
			catch (ExecutionException e) {
				logger.severe("Error loading place: " + e);
			}
		}
		if (p == null) {
			logger.severe("Place not found: " + id);
		}
		return p;
	}

	public String generatePlaceName(List<String> words) {
		int len = words.size() - 1;

		// ignore type words at the end
		// keep Cemetery as part of the full name (it's an exception; if there are others I'll create a property list)
		while (len >= 0 && isTypeWord(words.get(len)) && !"cemetery".equals(words.get(len))) {
			len--;
		}
		// if all words are type words, keep them all
		if (len < 0) {
			len = words.size() - 1;
		}

		// join and capitalize
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i <= len; i++) {
			if (buf.length() > 0) {
				buf.append(" ");
			}
			String word = words.get(i);
			buf.append(word.substring(0, 1).toUpperCase() + (word.length() > 1 ? word.substring(1).toLowerCase() : ""));
		}
		return buf.toString();
	}

	private boolean checkAncestorMatch(int id, Collection<Integer> ids) {
		Place p = getPlace(id);
		int locatedInId = p.getLocatedInId();
		if (locatedInId > 0) {
			if (ids.contains(locatedInId) || checkAncestorMatch(locatedInId, ids)) {
				return true;
			}
		}
		if (p.getAlsoLocatedInIds() != null) {
			for (int alii : p.getAlsoLocatedInIds()) {
				if (ids.contains(alii) || checkAncestorMatch(alii, ids)) {
					return true;
				}
			}
		}
		return false;
	}

	private List<Integer> filterSubplaceMatches(List<Integer> children, List<Integer> parents) {
		List<Integer> result = new ArrayList<Integer>();

		for (int child : children) {
			if (checkAncestorMatch(child, parents)) {
				result.add(child);
			}
		}

		return result;
	}

	private List<Integer> filterTypeMatches(List<Integer> ids, String typeToken) {
		List<Integer> result = new ArrayList<Integer>();

		for (int id : ids) {
			Place p = getPlace(id);
			String normalizedName = pn.normalize(p.getName());
			// does primary name contain the type words?
			if (normalizedName.indexOf(typeToken) >= 0) {
				result.add(id);
			} else if (p.getTypes() != null) {
				for (String type : p.getTypes()) {
					String normalizedType = pn.normalize(type);
					// does one of the types contain the type words?
					if (normalizedType.indexOf(typeToken) >= 0) {
						result.add(id);
						break;
					}
				}
			}
		}

		return result;
	}

	private boolean isLocatedIn(int pId, int parentId) {
		if (pId == parentId) {
			return true;
		}
		Place p = getPlace(pId);
		if (p.getLocatedInId() > 0 && isLocatedIn(p.getLocatedInId(), parentId)) {
			return true;
		}
		if (p.getAlsoLocatedInIds() != null) {
			for (int id : p.getAlsoLocatedInIds()) {
				if (isLocatedIn(id, parentId)) {
					return true;
				}
			}
		}
		return false;
	}

	// remove non top-level places that are outside of the default country
	private List<Integer> filterDefaultCountry(List<Integer> ids, String defaultCountry) {
		List<Integer> matchingIds = new ArrayList<Integer>();
		Place country = standardize(defaultCountry);
		if (country != null) {
			for (Integer id : ids) {
				Place p = getPlace(id);
				// allow top-level places or places in the country or places also-located-in the country
				// the last condition allows "defaultCountry" to be a state or county or whatever level you want
				if (p.getLevel() == TOP_LEVEL || p.getCountryId() == country.getId() || isLocatedIn(id, country.getId())) {
					matchingIds.add(id);
				}
			}
		}
		return matchingIds;
	}

	private double scoreMatch(String nameToken, Place p) {
		String normalizedName = pn.normalize(p.getName());
		boolean isPrimaryNameMatch = normalizedName.indexOf(nameToken) >= 0;
		int level = p.getLevel();
		int countryId = p.getCountryId();
		Double[] weights;

		if (largeCountries.contains(countryId)) {
			weights = largeCountryLevelWeights;
		} else if (mediumCountries.contains(countryId)) {
			weights = mediumCountryLevelWeights;
		} else {
			weights = smallCountryLevelWeights;
		}

		double score = weights[Math.min(MAX_LEVELS, level) - 1];

		if (isPrimaryNameMatch) {
			score += primaryMatchWeight;
		}

		score += 1.0 / p.getName().length();
		return score;
	}

	public boolean isTypeWord(String word) {
		String expansion = abbreviations.get(word);
		if (expansion != null) {
			word = expansion;
		}
		return typeWords.contains(word);
	}

	// catenate all of the words together into one token, with ending type words in a second token
	private String[] getNameTypeToken(List<String> words, int wordsToSkip) {
		StringBuilder buf = new StringBuilder();
		String[] result = new String[2];
		result[0] = null; // name token
		result[1] = null; // type token (optional)
		boolean foundNameWord = false;
		for (int i = words.size() - 1; i >= wordsToSkip; i--) {
			String word = words.get(i);
			if (word.length() > 0) {
				// skip everything before or or now
				if (i > wordsToSkip && buf.length() > 0 && "or".equals(word) || "now".equals(word)) {
					break;
				}
				// expand abbreviations only if there is >1 word in the phrase
				// keeps from expanding places like No, Niigata, Japan into North
				if (words.size() - wordsToSkip > 1) {
					String expansion = abbreviations.get(word);
					if (expansion != null) {
						word = expansion;
					}
				}
				if (!typeWords.contains(word)) {
					// type words after a name word go into the type token position
					if (!foundNameWord && buf.length() > 0) {
						result[1] = buf.toString();
						buf.setLength(0);
					}
					foundNameWord = true;
				}
				buf.insert(0, word);
			}
		}
		if (buf.length() > 0) {
			result[0] = buf.toString();
		}
		return result;
	}

	private boolean containsNonNoiseWords(List<String> words) {
		for (String word : words) {
			if (!noiseWords.contains(word)) {
				return true;
			}
		}
		return false;
	}

	private boolean containsNonNoiseLevels(List<List<String>> levelWords) {
		for (List<String> words : levelWords) {
			if (containsNonNoiseWords(words)) {
				return true;
			}
		}
		return false;
	}

	// once you've matched a country or a US state, you can't skip over it
	private boolean isSkippable(Collection<Integer> ids) {
		for (int id : ids) {
			Place p = getPlace(id);
			if (p.getLevel() == 1 ||
					(p.getLevel() == 2 && p.getCountryId() == USA_ID)) {
				return false;
			}
		}
		return true;
	}

	private Collection<Integer> removeChildIds(Collection<Integer> currentIds) {
		if (currentIds != null) {
			List<Integer> ids = new ArrayList<Integer>();
			for (int id : currentIds) {
				if (!checkAncestorMatch(id, currentIds)) {
					ids.add(id);
				}
			}
			currentIds = ids;
		}
		return currentIds;
	}

	public List<PlaceScore> standardize(String text, String defaultCountry, Mode mode, int numResults) {
		List<List<String>> levelWords = pn.tokenize(text);
		List<Integer> currentIds = null;
		List<Integer> previousIds = null;
		String currentNameToken = null;
		String l4Token = null;
		int lastFoundLevel = -1;
		// log only the first error per place -- skipping words can result in multiple errors, but we want to log the whole phrase
		boolean errorLogged = false;
		TreeMultimap<Integer, Place> levelMatches = TreeMultimap.create();
		int rlevel = 0;
		for (int level = levelWords.size() - 1; level >= 0; level--) {
			List<String> words = levelWords.get(level);
			// if all words don't match, back off and insert left-hand words as a new level
			// (for people who don't use commas)
			int wordsToSkip = 0;
			List<Integer> ids = null;
			String[] nameType = null;
			while (wordsToSkip < words.size()) {
				nameType = getNameTypeToken(words, wordsToSkip);

				// lookup name token
				ids = lookupWord(nameType[0]);
				if (ids != null) {
					if ((nameType[0].matches("de|la") && words.size()>1)) {
						ids = null;
					} else {
						break;
					}
				}
				wordsToSkip++;
			}
			if (ids != null && wordsToSkip > 0) {
				List<String> newLevel = new ArrayList<>();
				for (int i = 0; i < wordsToSkip; i++) {
					String word = words.get(i);
					// don't push noise words or type words down to the lower level
					// (does it hurt not to push type words down?)
					if (!noiseWords.contains(word) && !isTypeWord(word)) {
						newLevel.add(word);
					}
				}
				if (newLevel.size() > 0) {
					levelWords.add(level, newLevel);
					level++;
				}
			}

			// didn't find any matches; log and ignore
			if (ids == null) {
				if (errorHandler != null && !errorLogged && containsNonNoiseWords(words)) {
					errorHandler.tokenNotFound(text, levelWords, level, removeChildIds(currentIds));
					errorLogged = true;
				}
			} else {
				// if we found previous matches, filter subplaces
				boolean ignoreTypeToken = false;
				if (currentIds != null) {
					List<Integer> matchingIds = filterSubplaceMatches(ids, currentIds);
					// didn't find any children, try skipping over the previous level
					if (matchingIds.size() == 0 && isSkippable(currentIds)) {
						// try attaching to the grandparent level if there is one
						if (previousIds != null && previousIds.size() > 0) {
							matchingIds = filterSubplaceMatches(ids, previousIds);
							if (matchingIds.size() > 0) {
								currentIds = previousIds;
								if (errorHandler != null && !errorLogged) {
									errorHandler.skippingParentLevel(text, levelWords, level, removeChildIds(matchingIds));
									errorLogged = true;
								}
							}
						}
						// else if there is no grandparent level and we matched non-skippable places, go with what we just found
						else if (!isSkippable(ids)) {
							matchingIds = ids;
							currentIds = null;
							if (errorHandler != null && !errorLogged) {
								errorHandler.skippingParentLevel(text, levelWords, level, removeChildIds(matchingIds));
								errorLogged = true;
							}
						}
					}

					// still didn't find any children; log and ignore
					if (matchingIds.size() == 0) {
						ignoreTypeToken = true; // no sense matching the type if we couldn't match the name
						if (errorHandler != null && !errorLogged && containsNonNoiseWords(words)) {
							errorHandler.tokenNotFound(text, levelWords, level, removeChildIds(currentIds));
							errorLogged = true;
						}
						ids = currentIds;
						currentIds = previousIds;
					} else {
						lastFoundLevel = level;
						ids = matchingIds;
					}
				} else {
					// if we have multiple matches and a default country, filter non-top-level places outside the default country
					if (ids.size() > 1 && defaultCountry != null && defaultCountry.length() > 0) {
						List<Integer> matchingIds = filterDefaultCountry(ids, defaultCountry);
						if (matchingIds.size() > 0) {
							ids = matchingIds;
						}
					}
					lastFoundLevel = level;
				}

				// if we still have multiple matches, filter on type
				if (ids.size() > 1 && nameType[1] != null && !ignoreTypeToken) {
					List<Integer> matchingIds = filterTypeMatches(ids, nameType[1]);
					// didn't find a type match; log and ignore
					if (matchingIds.size() == 0) {
						if (errorHandler != null && !errorLogged) {
							errorHandler.typeNotFound(text, levelWords, level, removeChildIds(ids));
							errorLogged = true;
						}
					} else {
						ids = matchingIds;
					}
				}

				for (Integer id : ids) {
					Place p = getPlace(id);
					levelMatches.put(p.getLevel(), p);
				}

				previousIds = currentIds;
				currentIds = ids;
				currentNameToken = nameType[0];
			}
		}

		List<PlaceScore> results = new ArrayList<>();
		if (levelMatches.size()>0) {
			for (int lvl: levelMatches.keySet()){
				levelMatches.get(lvl);
			}
		}

		// if we have no matches, return empty
		if (currentIds == null) {
			// log this even if we've logged another error earlier
			if (errorHandler != null && containsNonNoiseLevels(levelWords)) {
				errorHandler.placeNotFound(text, levelWords);
			}
		} else if (mode == mode.REQUIRED && lastFoundLevel != 0) {
			// don't return any results if we didn't match the last level in this mode
		} else {
			// remove children if we have the parents
			if (currentIds.size() > 1) {
				currentIds = (List<Integer>) removeChildIds(currentIds);
			}

			// if we have still have multiple matches, score them and return the highest-scoring
			if (currentIds.size() > 1) {
				for (int id : currentIds) {
					Place p = getPlace(id);
					results.add(new PlaceScore(p, scoreMatch(currentNameToken, p)));
				}
				Collections.sort(results, (ps1, ps2) -> {
					// make sort order deterministic
					if (ps2.getScore() == ps1.getScore()) {
						return ps1.getPlace().getId() < ps2.getPlace().getId() ? -1 : 1;
					}
					return Double.compare(ps2.getScore(), ps1.getScore());
				});

				// remove lowest-scoring results
				while (results.size() > numResults) {
					results.remove(results.size() - 1);
				}

				if (errorHandler != null && !errorLogged) {
					errorHandler.ambiguous(text, levelWords, currentIds, results.get(0).getPlace());
					errorLogged = true;
				}
			} else if (currentIds.size() > 0) {
				Place p = getPlace(currentIds.get(0));
				results.add(new PlaceScore(p, scoreMatch(currentNameToken, p)));
			}
		}

		// in NEW mode, return "next-to-last-level-found, best match" if we didn't match the last level
		if (results.size() > 0 && mode == Mode.NEW && lastFoundLevel > 0) {
			Place p = new Place();
			//p.setStandardizer(this);
			p.setName(generatePlaceName(levelWords.get(lastFoundLevel - 1)));
			p.setLocatedInId(results.get(0).getPlace().getId());
			results.clear();
			results.add(new PlaceScore(p, 0));
		}

		return results;
	}

	public List<PlaceScore> standardize(String text, int numResults) {
		return standardize(text, null, Mode.BEST, numResults);
	}

	public Place standardize(String text) {
		return standardize(text, null);
	}

	public Place standardize(String text, String defaultCountry) {
		List<PlaceScore> results = standardize(text, defaultCountry, Mode.BEST, 1);
		return results.size() > 0 ? results.get(0).getPlace() : null;
	}
}
