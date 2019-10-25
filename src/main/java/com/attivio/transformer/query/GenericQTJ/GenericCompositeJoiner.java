package com.attivio.transformer.query.GenericQTJ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.search.QueryFeedback;
import com.attivio.sdk.search.QueryLanguages;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.facet.FacetFilter;
import com.attivio.sdk.search.query.BooleanAndQuery;
import com.attivio.sdk.search.query.BooleanNotQuery;
import com.attivio.sdk.search.query.BooleanOrQuery;
import com.attivio.sdk.search.query.CompositeJoinQuery;
import com.attivio.sdk.search.query.JoinClause;
import com.attivio.sdk.search.query.CompositeJoinQuery.Clause;
import com.attivio.sdk.search.query.JoinMode;
import com.attivio.sdk.search.query.JoinQuery;
import com.attivio.sdk.search.query.PhraseQuery;
import com.attivio.sdk.search.query.Query;
import com.attivio.sdk.search.query.QueryString;
import com.attivio.sdk.search.query.SubQuery;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOption.OptionLevel;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.component.query.QueryTransformer;
import com.attivio.sdk.util.BaseTypesList;

/**
 * @author brandon.bogan
 *
 */
@ConfigurationOptionInfo(displayName = "Generic Composite Joiner", description = "Transforms query into a Composite Join with a specified tables", groups = {
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.PLATFORM_COMPONENT, propertyNames = {
				"primaryTables", "nonPrimaryTables", "childTables", "joinField", "maxChildDocs",
				"childTableFacetFields", "tablesToIncludeInFacetCounts" }),
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.ADVANCED, propertyNames = {
				"ignoreAdvancedQueries", "provideFeedback", "collectionFieldName", "strictChildMatching",
				"allowChildDocOnlySearch", "tableBoosts" }) })
public class GenericCompositeJoiner implements QueryTransformer {

	private Logger log = LoggerFactory.getLogger(this.getClass());

	protected List<String> primaryTables;
	protected List<String> nonPrimaryTables;
	protected Map<String, String> childTables;
	protected String joinField;
	protected Map<String, Integer> maxChildDocs;
	protected boolean provideFeedback;
	protected Map<String, List<String>> childTableFacetFields = new HashMap<String, List<String>>();
	protected String collectionFieldName = FieldNames.TABLE;
	protected boolean ignoreAdvancedQueries;
	private List<String> tablesToIncludeInFacetCounts = new ArrayList<String>();
	private Map<String, Integer> tableBoosts;
	private boolean strictChildMatching;
	private boolean allowChildDocOnlySearch;

	public static String STRICT_QUERY_PROPERTY_NAME = "wasStrictJoin";
	public static String CHILD_DOC_MATCH_MESSAGE_NAME = "matchInChildDocument";

	private static String FACET_FILTER_PROPERTY_NAME = "facetFilters";
	private static String FILTER_PROPERTY_NAME = "filters";
	private static String ORIGINAL_QUERY_PROPERTY_NAME = "original_query";
	private static String ORIGINAL_QUERY_LANGUAGE_NAME = "original_language";

	/**
	 * @return the primaryTable
	 */
	@ConfigurationOption(displayName = "Primary Tables", description = "Tables containing primary data, to which child tables will be joined. Either primary tables or non-primary tables must be provided, but not both.", formEntryClass = ConfigurationOption.STRING_LIST)
	public List<String> getPrimaryTables() {
		return primaryTables;
	}

	/**
	 * @param primaryTables
	 */
	public void setPrimaryTables(List<String> primaryTables) {
		this.primaryTables = primaryTables;
	}

	@ConfigurationOption(displayName = "Non-Primary Tables", description = "Tables to exlude from being parent records. Either primary tables or non-primary tables must be provided, but not both.", formEntryClass = ConfigurationOption.STRING_LIST)
	public List<String> getNonPrimaryTables() {
		return nonPrimaryTables;
	}

	public void setNonPrimaryTables(List<String> nonPrimaryTables) {
		this.nonPrimaryTables = nonPrimaryTables;
	}

	/**
	 * @return the metadataTable
	 */
	@ConfigurationOption(displayName = "Child Tables", description = "Map of Tables containing metadata or related non-primary date to leverage in JOIN to the Join Mode for that table (either INNER or OUTER)", optionLevel = OptionLevel.Required, formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
	public Map<String, String> getChildTables() {
		return childTables;
	}

	/**
	 * @param metadataTable the metadataTable to set
	 */
	public void setChildTables(Map<String, String> childTables) {
		this.childTables = childTables;
	}

	/**
	 * @return the joinField
	 */
	@ConfigurationOption(displayName = "Join Field", description = "Field to join on (must be the same field across both sources)", optionLevel = OptionLevel.Required)
	public String getJoinField() {
		return joinField;
	}

	/**
	 * @param joinField the joinField to set
	 */
	public void setJoinField(String joinField) {
		this.joinField = joinField;
	}

	/**
	 * @return the maxChildDocs
	 */
	@ConfigurationOption(displayName = "Max Child Docs", description = "Max number of child docs to relate to parent doc. Set to -1 for no limit. Defaults to 0 (no children) if unset", optionLevel = OptionLevel.Required, formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
	public Map<String, Integer> getMaxChildDocs() {
		return maxChildDocs;
	}

	/**
	 * @param maxChildDocs the maxChildDocs to set
	 */
	public void setMaxChildDocs(Map<String, Integer> maxChildDocs) {
		this.maxChildDocs = maxChildDocs;
	}

	/**
	 * @return the metadataFacetFields
	 */
	@ConfigurationOption(displayName = "Child Table Facet Fields", description = "Map of table to Fields from that table that are used as facets", formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
	public Map<String, String> getChildTableFacetFields() {
		Map<String, String> response = new HashMap<String, String>();
		for (String key : this.childTableFacetFields.keySet()) {
			List<String> facetFields = this.childTableFacetFields.get(key);
			response.put(key, String.join(",", facetFields));
		}
		return response;
	}

	/**
	 * @param metadataFacetFields the metadataFacetFields to set
	 */
	public void setChildTableFacetFields(Map<String, String> metadataFacetFields) {
		Map<String, List<String>> tableFacets = new HashMap<String, List<String>>();
		for (String key : metadataFacetFields.keySet()) {
			key = key.trim();
			String[] facetFields = metadataFacetFields.get(key).replace(" ", "").split(",");
			tableFacets.put(key, Arrays.asList(facetFields));
			log.trace("Adding facet fields " + Arrays.toString(facetFields) + " for table " + key);
		}
		this.childTableFacetFields = tableFacets;
	}

	@ConfigurationOption(displayName = "Collection Field Name", description = "Name of field to use to differentiate collections/tables of content (this will be normally be the 'table' field")
	public String getCollectionFieldName() {
		return collectionFieldName;
	}

	public void setCollectionFieldName(String collectionFieldName) {
		this.collectionFieldName = collectionFieldName;
	}

	@ConfigurationOption(displayName = "Provide Query Feedback", description = "Should detailed query feedback be provided", formEntryClass = ConfigurationOption.FALSE_SWITCH_VALUE)
	public boolean isProvideFeedback() {
		return provideFeedback;
	}

	public void setProvideFeedback(boolean provideFeedback) {
		this.provideFeedback = provideFeedback;
	}

	@ConfigurationOption(displayName = "Ignore Advanced Queries", description = "Should qeries in the advanced query language be ignored", formEntryClass = ConfigurationOption.FALSE_SWITCH_VALUE)
	public boolean isIgnoreAdvancedQueries() {
		return ignoreAdvancedQueries;
	}

	public void setIgnoreAdvancedQueries(boolean ignoreAdvancedQueries) {
		this.ignoreAdvancedQueries = ignoreAdvancedQueries;
	}

	@ConfigurationOption(displayName = "Facetable Tables", description = "Tables to include in facet counts", formEntryClass = ConfigurationOption.STRING_LIST)
	public List<String> getTablesToIncludeInFacetCounts() {
		return tablesToIncludeInFacetCounts;
	}

	public void setTablesToIncludeInFacetCounts(List<String> tablesToIncludeInFacetCounts) {
		this.tablesToIncludeInFacetCounts = tablesToIncludeInFacetCounts;
	}

	@ConfigurationOption(displayName = "Table Boost Amounts", description = "Static boosts to be applied to a table's clause (the default for ommited tables is 0)", formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
	public Map<String, Integer> getTableBoosts() {
		return tableBoosts;
	}

	public void setTableBoosts(Map<String, Integer> tableBoosts) {
		this.tableBoosts = tableBoosts;
	}

	@ConfigurationOption(displayName = "Use Strict Join", description = "If there is no match on the parent, should only child documents matching the query term be returned", formEntryClass = ConfigurationOption.FALSE_SWITCH_VALUE)
	public boolean isStrictChildMatching() {
		return strictChildMatching;
	}

	public void setStrictChildMatching(boolean strictChildMatching) {
		this.strictChildMatching = strictChildMatching;
	}

	@ConfigurationOption(displayName = "Allow Child Doc Only Search", description = "If 'Use Strict Join' is set to true, this will allow the initial pass to only search child documents. If you want to subsequently search parent documents when there's no results on the children, use the 'Resubmit Strict Join' response transformer to resubmit so all docs can be searched.", formEntryClass = ConfigurationOption.FALSE_SWITCH_VALUE)
	public boolean isAllowChildDocOnlySearch() {
		return allowChildDocOnlySearch;
	}

	public void setAllowChildDocOnlySearch(boolean allowChildDocOnlySearch) {
		this.allowChildDocOnlySearch = allowChildDocOnlySearch;
	}

	@Override
	public List<QueryFeedback> processQuery(QueryRequest qr) throws AttivioException {
		List<QueryFeedback> feedback = new ArrayList<QueryFeedback>();

		if (qr.getQueryLanguage().equalsIgnoreCase(QueryLanguages.ADVANCED) && this.ignoreAdvancedQueries) {
			if (this.provideFeedback) {
				String message = "Advanced Query Language in use, skipping stage.";
				feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
				log.trace(message);
			}
		} else {
			// qr.setProperty("join.facet", "FULL");
			Map<String, List<Query>> facetFilters = this.extractMetadataFacetFilterQueries(qr, feedback);
			Query joinQuery = this.buildCompositeJoinQuery(facetFilters, qr, feedback);
			if (this.provideFeedback) {
				String message = "Final Join Query: " + joinQuery;
				feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
				log.debug(message);
			}
			qr.setQuery(joinQuery);
		}
		return feedback;
	}

	/**
	 * Strips both filter queries and facet filters that pertain to the table being
	 * joined to from the {@code QueryRequest} and converts them into a list
	 * of{@code Queries} which can be added into the {@code CompositeJoin} down the
	 * line.
	 * 
	 * @param qr       The {@code QueryRequest} to strip the specific filter queries
	 *                 and facet filters from
	 * @param feedback List of feedback to add additional feedback to
	 * @return A list of {@code Query} objects that represent the stripped filters
	 *         and facet filters
	 */
	private Map<String, List<Query>> extractMetadataFacetFilterQueries(QueryRequest qr, List<QueryFeedback> feedback) {
		Map<String, List<Query>> filterSubQueries = new HashMap<String, List<Query>>();
		for (String table : this.childTableFacetFields.keySet()) {
			filterSubQueries.put(table,
					this.extractMetadataFacetFilterQueriesHelper(qr, feedback, this.childTableFacetFields.get(table)));
		}
		return filterSubQueries;
	}

	private List<Query> extractMetadataFacetFilterQueriesHelper(QueryRequest qr, List<QueryFeedback> feedback,
			List<String> fieldNames) {

		List<Query> facetSubQueries = new ArrayList<Query>();

		// Collect both Filters and FacetFilters that have been applied to the query
		List<Query> originalFilters = qr.getFilters();

		if (qr.getResubmits() > 0 && qr.hasProperty(FILTER_PROPERTY_NAME)) {
			Object filterPropValue = qr.getProperty(FILTER_PROPERTY_NAME);
			if (filterPropValue instanceof List<?>) {
				originalFilters = new ArrayList<Query>();
				// It has to be a list because setting a List<Query> as a QR property isn't
				// supported
				List<String> filterStringList = (List<String>) filterPropValue;
				for (String s : filterStringList) {
					Query q = new QueryString(s);
					originalFilters.add(q);
				}
			}
		} else {
			// We do this so that if the query gets resubmitted we can access the original
			// filters
			List<String> originalFiltersAsStringList = new BaseTypesList<String>();
			for (Query q : originalFilters) {
				originalFiltersAsStringList.add(q.getQueryString());
			}
			qr.setProperty(FILTER_PROPERTY_NAME, originalFiltersAsStringList);
		}

		List<FacetFilter> originalFacetFilters = qr.getFacetFilters();

		if (qr.getResubmits() > 0 && qr.hasProperty(FACET_FILTER_PROPERTY_NAME)) {
			Object facetFilterPropValue = qr.getProperty(FACET_FILTER_PROPERTY_NAME);
			if (facetFilterPropValue instanceof List<?>) {
				List<String> facetFiltersAsString = new BaseTypesList<String>();
				originalFacetFilters = new ArrayList<FacetFilter>();
				for (String ff : facetFiltersAsString) {
					originalFacetFilters.add(FacetFilter.valueOf(ff));
				}
			}
		} else {
			// We do this so that if the query gets resubmitted we can access the original
			// filters
			List<String> originalFiltersAsStringList = new BaseTypesList<String>();
			for (FacetFilter ff : originalFacetFilters) {
				originalFiltersAsStringList.add(ff.toString());
			}
			qr.setProperty(FILTER_PROPERTY_NAME, originalFiltersAsStringList);
		}

		List<Query> newFilters = new ArrayList<Query>();
		List<FacetFilter> newFacetFilters = new ArrayList<FacetFilter>();

		// Handle the filter queries first. These are a little more complex to get the
		// field and value
		// from, so we use Regex.
		// NOTE: There is probably a better way that I don't know about that doesn't
		// require Regex.
		// The idea here is loop through the filters, and if any of them pertain to the
		// metadata facet
		// fields, extract them into specific queries that we can leverage within our
		// composite join.
		for (Query filterQuery : originalFilters) {
			String queryString = filterQuery.toString();
			boolean matchFound = false;
			for (String facetFieldName : fieldNames) {
				if (queryString.contains(facetFieldName)) {
					if (this.provideFeedback) {
						String message = "Found metadata facet filter in QueryFilter: " + queryString;
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.trace(message);
					}
					// String regex = facetFieldName + ":(.*)\"";
					// Pattern pattern = Pattern.compile(regex);
					// Matcher m = pattern.matcher(queryString);
					// if (m.find() && m.groupCount() >= 1) {
					matchFound = true;
					// String fieldValue = m.group(1);
					// // QueryRequest filterQueryRequest = new QueryRequest();
					// // filterQueryRequest.setQuery(facetFieldName + ":" + fieldValue, "simple");
					// // facetSubQueries.add(filterQueryRequest.getQuery());
					// QueryString newFilterQuery = new QueryString(facetFieldName + ":" +
					// fieldValue);
					// facetSubQueries.add(newFilterQuery);
					facetSubQueries.add(filterQuery);
					if (this.provideFeedback) {
						String message = "Found " + facetFieldName
								+ ", stripping from filter and building into query...";
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.trace(message);
					}
					// }
				}
			}
			if (!matchFound) {
				if (this.provideFeedback) {
					String message = "Not a filter of interest - ignoring...: " + filterQuery.toString();
					feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
							"Not a filter of interest - ignoring...: " + filterQuery.toString()));
					log.trace(message);
				}
				newFilters.add(filterQuery);
			}
		}
		qr.setFilters(newFilters);

		// Next handle the FacetFilters
		for (FacetFilter f : originalFacetFilters) {
			String queryString = f.toString();// .getFilter().getQueryString();
			boolean matchFound = false;
			for (String facetFieldName : fieldNames) {
				if (queryString.contains(facetFieldName)) {
					if (this.provideFeedback) {
						String message = "Found metadata facet filter in QueryFilter: " + queryString;
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.trace(message);
					}
					// Query newQuery = f.getFilter();
					// facetSubQueries.add(newQuery);
					// if (this.provideFeedback) {
					// String message = "Found " + facetFieldName
					// + ", stripping from filter and building into query..." + newQuery;
					// feedback.add(
					// new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
					// message));
					// log.trace(message);
					// }
					String regex = facetFieldName + ":FACET.{1}(.*).{1},{0,1}";
					System.out.println("MATCHING: " + regex + " ON " + queryString);
					Pattern pattern = Pattern.compile(regex);
					Matcher m = pattern.matcher(queryString);
					if (m.find() && m.groupCount() >= 1) {
						matchFound = true;
						String fieldValue = m.group(1);
						fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
						fieldValue = this.handleRangeFacetFilters(fieldValue);
						Query newQueryString = new QueryString(facetFieldName + ":" + fieldValue);
						SubQuery newQuery = new SubQuery(newQueryString);
						// This is so the facet filter queries don't show up in Search Analytics
						newQuery.setParameter("abc.userquery", false);
						facetSubQueries.add(newQuery);
						if (this.provideFeedback) {
							String message = "Found " + facetFieldName
									+ ", stripping from filter and building into query..." + newQuery;
							feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
									message));
							log.trace(message);
						}
						// QueryRequest filterQueryRequest = new QueryRequest();
						// filterQueryRequest.setQuery(facetFieldName + ":" + fieldValue, "simple");
						// facetSubQueries.add(filterQueryRequest.getQuery());

						// QueryString qs = new QueryString(facetFieldName + ":" + "[0 TO 2000]");
						// facetSubQueries.add(qs);
					}
					if (this.provideFeedback) {
						feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
								"Found " + facetFieldName + ", stripping from filter and building into query..."));
					}
				}
			}
			if (!matchFound) {
				if (this.provideFeedback) {
					String message = "Not a filter of interest - ignoring...: " + f.toString();
					feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
							"Not a filter of interest - ignoring...: " + f.toString()));
					log.trace(message);
				}
				newFacetFilters.add(f);
			}
		}
		qr.setFacetFilters(newFacetFilters);
		return facetSubQueries;
	}

	/**
	 * For facet filters coming from Range Facets, we need to extract the RANGE
	 * query and rewrite it to the standard range syntax from simple query language
	 * (like [], [}, etc.)
	 * 
	 * @param fieldValue The field value to conditionally rewrite into a proper
	 *                   range query if necessary
	 */
	private String handleRangeFacetFilters(String fieldValue) {
		String returnValue = fieldValue;
		String regexForNumericRanges = "RANGE\\((.*),\\s{0,1}(.*),\\s{0,1}upper=(\\w*)\\)";
		Pattern pForNumRanges = Pattern.compile(regexForNumericRanges);
		Matcher mForNumRanges = pForNumRanges.matcher(fieldValue);
		if (mForNumRanges.find() && mForNumRanges.groupCount() >= 3) {
			// We found a range
			String rangeStart = mForNumRanges.group(1);
			String rangeEnd = mForNumRanges.group(2);
			String upperBoundryType = mForNumRanges.group(3);
			if (upperBoundryType.equals("exclusive")) {
				returnValue = String.format("[%s TO %s}", rangeStart, rangeEnd);
			} else if (upperBoundryType.equals("inclusive")) {
				returnValue = String.format("[%s TO %s]", rangeStart, rangeEnd);
			}
		}
		log.trace(String.format("Rewrote field value from facet filter query from %s to %s", fieldValue, returnValue));
		return returnValue;
	}

	/**
	 * Constructs a {@code CompositeJoinQuery} based on the configurations and what
	 * facet filters were found that pertain to the metadata. <br>
	 * <br>
	 * If there are no metadata facet filters, it constructs an {@code OUTER JOIN}.
	 * However, if there are metadata facets, it constructs an {@code INNER JOIN},
	 * such that only the documents in the {@code FROM} clause that have the
	 * specified metadata attribute(s) are returned. <br>
	 * <br>
	 * If {@code strictChildMatching} is enabled, it will generate a regular
	 * {@code JOIN} query for each child table, and put them all in an {@code OR}
	 * query. It will also set a propert on the {@code QUeryRequest} noting that a
	 * strict join was used, incase looping back to relax the join is enabled.<br>
	 * <br>
	 * <b>Note:</b>This strict join will only match on hits in the child tables, not
	 * the parent table. Looping back to relax the query will be required for that.
	 * 
	 * @param facetFilters
	 * @param qr
	 * @param feedback
	 * @return the constructed CompositeJoinQuery.
	 * @throws AttivioException
	 */
	protected Query buildCompositeJoinQuery(Map<String, List<Query>> facetFiltersMap, QueryRequest qr,
			List<QueryFeedback> feedback) throws AttivioException {

		if (qr.getResubmits() > 0 && qr.hasProperty(ORIGINAL_QUERY_PROPERTY_NAME)) {
			String originalQueryAsString = qr.getProperty(ORIGINAL_QUERY_PROPERTY_NAME, "*:*");
			String queryLanguage = qr.getProperty(ORIGINAL_QUERY_LANGUAGE_NAME, "advanced");
			qr.setQuery(originalQueryAsString, queryLanguage);
		} else {
			// If the query gets resubmitted we'll need this, since we don't want to use the
			// JOIN query as the query in our new JOIN
			qr.setProperty(ORIGINAL_QUERY_PROPERTY_NAME, qr.getQueryString());
			qr.setProperty(ORIGINAL_QUERY_LANGUAGE_NAME, qr.getQueryLanguage());
		}

		// If we need to build a strict join we need to build the query differently than
		// using a normal composite join
		boolean shouldBeComposite = !this.strictChildMatching || qr.getQuery().toString().contains("*:*")
				|| qr.getQuery().toString().equals("*");
		if (!shouldBeComposite) {
			return this.buildStrictChildMatchingJoin(facetFiltersMap, qr, feedback);
		}
		CompositeJoinQuery compJoin = new CompositeJoinQuery(qr.getQuery());

		Query fromQuery = this.generateFromQuery();
		compJoin.setFromQuery(fromQuery);
		compJoin.setField(this.joinField);

		// Put all the metadata tables into a big OR query
		for (String table : this.childTables.keySet()) {
			JoinMode configuredModeForTable = JoinMode.fromExternal(this.childTables.get(table));
			int boost = this.tableBoosts.containsKey(table) ? tableBoosts.get(table) : 0;
			Clause c;
			if (facetFiltersMap.containsKey(table)) {
				List<Query> facetFilters = facetFiltersMap.get(table);
				PhraseQuery metadataTableQuery = new PhraseQuery(this.collectionFieldName, table);
				if (facetFilters.size() > 0) {
					BooleanAndQuery clause = new BooleanAndQuery(metadataTableQuery);
					clause.add(facetFilters);
					// Clauses will always use INNER joins when a relevant
					// facet filter has been rewritten into the clause's query
					c = compJoin.addClause(JoinMode.INNER, clause);
					if (this.provideFeedback) {
						String message = "Metadata FacetFilter Queries applied to join clause (ACTUAL CLAUSE): "
								+ c.prettyFormat() + "with facet set to: " + c.isFacet();
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.debug(message);
					}
				} else {
					c = compJoin.addClause(configuredModeForTable, metadataTableQuery);
					if (this.provideFeedback) {
						String message = "No metadata facet filters found. Adding clause (ACTUAL CLAUSE): "
								+ c.prettyFormat() + "with facet set to: " + c.isFacet();
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.debug(message);
					}
				}
			} else {
				PhraseQuery clause = new PhraseQuery(this.collectionFieldName, table);
				c = compJoin.addClause(configuredModeForTable, clause);

				if (this.provideFeedback) {
					String message = "No metadata facet filters found. Adding clause: " + clause;
					feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
					log.debug(message);
				}
			}
			Integer maxDocs = this.maxChildDocs.containsKey(table) ? this.maxChildDocs.get(table) : 10;
			if (maxDocs >= 0) {
				c.setRollupLimit(maxDocs);
			}
			if (!this.tablesToIncludeInFacetCounts.contains(table)) {
				c.setFacet(false);
			}
			c.setBoost(boost);
		}
		return compJoin;
	}

	/**
	 * Generates an {@code BooleanOrQuery} with multiple {@code JoinQueries} in it.
	 * There will be one {@code JoinQuery} for each child table, plus one for the
	 * parents. This query will result in matching on all parent documents where the
	 * match is found, but if the match is only found in the child documents, only
	 * those children will be returned. This is different from the normal
	 * {@code CompositeJoin} behavior where all matches from a child table are
	 * returned as long as a match is found on at least one of them.
	 * 
	 * @param facetFiltersMap A map of child table name to a list of queries to add
	 *                        to that child table's join clause
	 * @param qr
	 * @param feedback
	 * @return
	 */
	private Query buildStrictChildMatchingJoin(Map<String, List<Query>> facetFiltersMap, QueryRequest qr,
			List<QueryFeedback> feedback) {
		boolean firstTimeThrough = !qr.hasProperty(STRICT_QUERY_PROPERTY_NAME)
				|| !qr.getProperty(STRICT_QUERY_PROPERTY_NAME, false);
		boolean searchOnlyParentTable = firstTimeThrough && this.allowChildDocOnlySearch;
		boolean searchOnlyChildren = !firstTimeThrough && this.allowChildDocOnlySearch;
		if (searchOnlyParentTable) {
			return this.buildJoinForParentTableOnly(facetFiltersMap, qr, feedback, true);
		}
		BooleanOrQuery orQuery = new BooleanOrQuery();
		Query fromQuery = this.generateFromQuery();
		Set<String> tablesToLoopThrough = new HashSet<String>(this.childTables.keySet());
		for (String tableToSearchIn : tablesToLoopThrough) {
			JoinQuery join = new JoinQuery();
			join.setQuery(fromQuery);
			for (String childTable : this.childTables.keySet()) {
				JoinMode joinMode = JoinMode.fromExternal(this.childTables.get(childTable));
				PhraseQuery basicTableQuery = new PhraseQuery(this.collectionFieldName, childTable);
				Query clauseQuery;
				if (childTable.equals(tableToSearchIn)) {
					BooleanAndQuery andQuery = new BooleanAndQuery(basicTableQuery);
					SubQuery userQuery = new SubQuery(qr.getQuery());
					userQuery.setParameter("abc.userquery", true);
					andQuery.add(userQuery);
					if (facetFiltersMap.containsKey(childTable)) {
						andQuery.add(facetFiltersMap.get(childTable));
					}
					clauseQuery = andQuery;
					joinMode = JoinMode.INNER;
				} else if (facetFiltersMap.containsKey(childTable) && facetFiltersMap.get(childTable).size() > 0) {
					BooleanAndQuery andQuery = new BooleanAndQuery(basicTableQuery);
					andQuery.add(facetFiltersMap.get(childTable));
					clauseQuery = andQuery;
					joinMode = JoinMode.INNER;
				} else {
					clauseQuery = basicTableQuery;
				}
				JoinClause c = new JoinClause(clauseQuery, joinMode, this.joinField, this.joinField);
				if (this.tableBoosts.containsKey(childTable)) {
					c.setBoost(this.tableBoosts.get(childTable));
				}
				Integer maxDocs = this.maxChildDocs.containsKey(childTable) ? this.maxChildDocs.get(childTable) : 10;
				if (maxDocs >= 0) {
					c.setRollupLimit(maxDocs);
				}
				join.add(c);
			}
			String message = "Adding join query to strict or query: " + join;
			log.trace(message);
			if (this.provideFeedback) {
				feedback.add(new QueryFeedback(this.getClass().getCanonicalName(), "Generatd Join Query", message));
			}
			orQuery.add(join);
		}
		if (!searchOnlyChildren) {
			orQuery.add(this.buildJoinForParentTableOnly(facetFiltersMap, qr, feedback, false));
		} else {
			feedback.add(new QueryFeedback(this.getClass().getCanonicalName(), CHILD_DOC_MATCH_MESSAGE_NAME,
					"The match is in the child documents"));
		}
		qr.setProperty(STRICT_QUERY_PROPERTY_NAME, false);
		return orQuery;
	}

	private Query buildJoinForParentTableOnly(Map<String, List<Query>> facetFiltersMap, QueryRequest qr,
			List<QueryFeedback> feedback, boolean modifyMessages) {
		Query fromQuery = this.generateFromQuery();
		BooleanAndQuery combinedFromQuery = new BooleanAndQuery(fromQuery);
		SubQuery userQuery = new SubQuery(qr.getQuery());
		userQuery.setParameter("abc.userquery", true);
		combinedFromQuery.add(userQuery);
		JoinQuery join = new JoinQuery();
		join.setQuery(combinedFromQuery);
		for (String childTable : this.childTables.keySet()) {
			JoinMode joinMode = JoinMode.fromExternal(this.childTables.get(childTable));
			PhraseQuery basicTableQuery = new PhraseQuery(this.collectionFieldName, childTable);
			Query clauseQuery;
			if (facetFiltersMap.containsKey(childTable) && facetFiltersMap.get(childTable).size() > 0) {
				BooleanAndQuery andQuery = new BooleanAndQuery(basicTableQuery);
				andQuery.add(facetFiltersMap.get(childTable));
				clauseQuery = andQuery;
				joinMode = JoinMode.INNER;
			} else {
				clauseQuery = basicTableQuery;
			}
			JoinClause c = new JoinClause(clauseQuery, joinMode, this.joinField, this.joinField);
			if (this.tableBoosts.containsKey(childTable)) {
				c.setBoost(this.tableBoosts.get(childTable));
			}
			Integer maxDocs = this.maxChildDocs.containsKey(childTable) ? this.maxChildDocs.get(childTable) : 10;
			if (maxDocs >= 0) {
				c.setRollupLimit(maxDocs);
			}
			join.add(c);
		}
		if (modifyMessages) {
			qr.setProperty(STRICT_QUERY_PROPERTY_NAME, true);
		}
		return join;
	}

	/**
	 * Depending on whether the primaryTables or nonPrimaryTables field is
	 * populated, generates a query that will only select records from the
	 * appropriate table(s), which can then be used as the query for the "FROM"
	 * table
	 * 
	 * @return the primary query to use in the join
	 */
	protected Query generateFromQuery() {
		if (this.primaryTables != null && this.primaryTables.size() > 0) {
			if (this.primaryTables.size() == 1) {
				return new PhraseQuery(this.collectionFieldName, this.primaryTables.get(0));
			}
			BooleanOrQuery orQ = new BooleanOrQuery();
			for (String tableName : primaryTables) {
				orQ.add(new PhraseQuery(this.collectionFieldName, tableName));
			}
			return orQ;
		} else {
			BooleanOrQuery orQ = new BooleanOrQuery();
			for (String tableName : this.nonPrimaryTables) {
				orQ.add(new PhraseQuery(this.collectionFieldName, tableName));
			}
			return new BooleanNotQuery(orQ);
		}
	}
}
