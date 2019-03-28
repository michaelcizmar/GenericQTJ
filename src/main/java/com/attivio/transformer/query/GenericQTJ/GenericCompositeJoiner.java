package com.attivio.transformer.query.GenericQTJ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.attivio.sdk.search.query.CompositeJoinQuery.Clause;
import com.attivio.sdk.search.query.JoinMode;
import com.attivio.sdk.search.query.PhraseQuery;
import com.attivio.sdk.search.query.Query;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOption.OptionLevel;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.component.query.QueryTransformer;

/**
 * @author brandon.bogan
 *
 */
@ConfigurationOptionInfo(displayName = "Generic Composite Joiner", description = "Transforms query into a Composite Join with a specified tables", groups = {
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.PLATFORM_COMPONENT, propertyNames = {
				"primaryTables", "nonPrimaryTables", "metadataTables", "joinField", "maxChildDocs",
				"metadataFacetFields", "tablesToIncludeInFacetCounts" }),
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.ADVANCED, propertyNames = {
				"ignoreAdvancedQueries", "provideFeedback" }) })
public class GenericCompositeJoiner implements QueryTransformer {

	private Logger log = LoggerFactory.getLogger(this.getClass());

	protected List<String> primaryTables;
	protected List<String> nonPrimaryTables;
	protected List<String> metadataTables;
	protected String joinField;
	protected int maxChildDocs;
	protected boolean provideFeedback;
	protected Map<String, List<String>> metadataFacetFields = new HashMap<String, List<String>>();
	protected boolean ignoreAdvancedQueries;
	private List<String> tablesToIncludeInFacetCounts = new ArrayList<String>();

	public GenericCompositeJoiner() {
		this.tablesToIncludeInFacetCounts.add("index_metadata");
		this.tablesToIncludeInFacetCounts.add("XML_Metadata");
	}

	/**
	 * @return the primaryTable
	 */
	@ConfigurationOption(displayName = "Primary Tables", description = "Tables containing primary data, to which metadata tables will be joined. Either primary tables or non-primary tables must be provided, but not both.", formEntryClass = ConfigurationOption.STRING_LIST)
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
	@ConfigurationOption(displayName = "Metadata Tables", description = "Tables containing metadata to leverage in JOIN", optionLevel = OptionLevel.Required, formEntryClass = ConfigurationOption.STRING_LIST)
	public List<String> getMetadataTables() {
		return metadataTables;
	}

	/**
	 * @param metadataTable
	 *            the metadataTable to set
	 */
	public void setMetadataTables(List<String> metadataTables) {
		this.metadataTables = metadataTables;
	}

	/**
	 * @return the joinField
	 */
	@ConfigurationOption(displayName = "Join Field", description = "Field to join on (must be the same field across both sources)", optionLevel = OptionLevel.Required)
	public String getJoinField() {
		return joinField;
	}

	/**
	 * @param joinField
	 *            the joinField to set
	 */
	public void setJoinField(String joinField) {
		this.joinField = joinField;
	}

	/**
	 * @return the maxChildDocs
	 */
	@ConfigurationOption(displayName = "Max Child Docs", description = "Max number of child docs to relate to parent doc. Set to -1 for no limit.", optionLevel = OptionLevel.Required)
	public String getMaxChildDocs() {
		return String.valueOf(maxChildDocs);
	}

	/**
	 * @param maxChildDocs
	 *            the maxChildDocs to set
	 */
	public void setMaxChildDocs(String maxChildDocs) {
		this.maxChildDocs = Integer.valueOf(maxChildDocs);
	}

	/**
	 * @return the metadataFacetFields
	 */
	@ConfigurationOption(displayName = "Metadata Facet Fields", description = "Map of table to Fields from that table that are used as facets", formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
	public Map<String, String> getMetadataFacetFields() {
		Map<String, String> response = new HashMap<String, String>();
		for (String key : this.metadataFacetFields.keySet()) {
			List<String> facetFields = this.metadataFacetFields.get(key);
			response.put(key, String.join(",", facetFields));
		}
		return response;
	}

	/**
	 * @param metadataFacetFields
	 *            the metadataFacetFields to set
	 */
	public void setMetadataFacetFields(Map<String, String> metadataFacetFields) {
		Map<String, List<String>> tableFacets = new HashMap<String, List<String>>();
		for (String key : metadataFacetFields.keySet()) {
			key = key.trim();
			String[] facetFields = metadataFacetFields.get(key).replace(" ", "").split(",");
			tableFacets.put(key, Arrays.asList(facetFields));
			log.trace("Adding facet fields " + Arrays.toString(facetFields) + " for table " + key);
		}
		this.metadataFacetFields = tableFacets;
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

	@ConfigurationOption(displayName = "Facet Metadata Tables", description = "Tables to include in facet counts", formEntryClass = ConfigurationOption.STRING_LIST)
	public List<String> getTablesToIncludeInFacetCounts() {
		return tablesToIncludeInFacetCounts;
	}

	public void setTablesToIncludeInFacetCounts(List<String> tablesToIncludeInFacetCounts) {
		this.tablesToIncludeInFacetCounts = tablesToIncludeInFacetCounts;
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
			qr.setProperty("join.facet", "FULL");
			Map<String, List<Query>> facetFilters = this.extractMetadataFacetFilterQueries(qr, feedback);
			Query joinQuery = this.buildCompositeJoinQuery(facetFilters, qr, feedback);
			if (this.provideFeedback) {
				String message = "Final Composite Join Query: " + joinQuery.prettyFormat();
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
	 * @param qr
	 *            The {@code QueryRequest} to strip the specific filter queries and
	 *            facet filters from
	 * @param feedback
	 *            List of feedback to add additional feedback to
	 * @return A list of {@code Query} objects that represent the stripped filters
	 *         and facet filters
	 */
	private Map<String, List<Query>> extractMetadataFacetFilterQueries(QueryRequest qr, List<QueryFeedback> feedback) {
		Map<String, List<Query>> filterSubQueries = new HashMap<String, List<Query>>();
		for (String table : this.metadataFacetFields.keySet()) {
			filterSubQueries.put(table,
					this.extractMetadataFacetFilterQueriesHelper(qr, feedback, this.metadataFacetFields.get(table)));
		}
		return filterSubQueries;
	}

	private List<Query> extractMetadataFacetFilterQueriesHelper(QueryRequest qr, List<QueryFeedback> feedback,
			List<String> fieldNames) {

		List<Query> facetSubQueries = new ArrayList<Query>();

		// Collect both Filters and FacetFilters that have been applied to the query
		List<Query> originalFilters = qr.getFilters();
		List<Query> newFilters = new ArrayList<Query>();

		List<FacetFilter> originalFacetFilters = qr.getFacetFilters();
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
			String queryString = filterQuery.getQueryString();
			boolean matchFound = false;
			for (String facetFieldName : fieldNames) {
				if (queryString.contains(facetFieldName)) {
					if (this.provideFeedback) {
						String message = "Found metadata facet filter in QueryFilter: " + queryString;
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.trace(message);
					}
					String regex = facetFieldName + ":(.*)";
					Pattern pattern = Pattern.compile(regex);
					Matcher m = pattern.matcher(queryString);
					if (m.find() && m.groupCount() >= 1) {
						matchFound = true;
						String fieldValue = m.group(1);
						QueryRequest filterQueryRequest = new QueryRequest();
						filterQueryRequest.setQuery(facetFieldName + ":" + fieldValue, "simple");
						facetSubQueries.add(filterQueryRequest.getQuery());
						if (this.provideFeedback) {
							String message = "Found " + facetFieldName
									+ ", stripping from filter and building into query...";
							feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
									message));
							log.trace(message);
						}
					}
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
					Query newQuery = f.getFilter();
					facetSubQueries.add(newQuery);
					if (this.provideFeedback) {
						String message = "Found " + facetFieldName
								+ ", stripping from filter and building into query..." + newQuery;
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.trace(message);
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
	 * Constructs a {@code CompositeJoinQuery} based on the configurations and what
	 * facet filters were found that pertain to the metadata. <br>
	 * <br>
	 * If there are no metadata facet filters, it constructs an {@code OUTER JOIN}.
	 * However, if there are metadata facets, it constructs an {@code INNER JOIN},
	 * such that only the documents in the {@code FROM} clause that have the
	 * specified metadata attribute(s) are returned.
	 * 
	 * @param facetFilters
	 * @param qr
	 * @param feedback
	 * @return the constructed CompositeJoinQuery.
	 * @throws AttivioException
	 */
	protected Query buildCompositeJoinQuery(Map<String, List<Query>> facetFiltersMap, QueryRequest qr,
			List<QueryFeedback> feedback) throws AttivioException {
		CompositeJoinQuery compJoin = new CompositeJoinQuery(qr.getQuery());

		Query fromQuery = this.generateFromQuery();
		compJoin.setFromQuery(fromQuery);
		compJoin.setField(this.joinField);

		// Put all the metadata tables into a big OR query
		for (String table : this.metadataTables) {
			if (facetFiltersMap.containsKey(table)) {
				List<Query> facetFilters = facetFiltersMap.get(table);
				BooleanOrQuery metadataTableQuery = new BooleanOrQuery();
				PhraseQuery tableQuery = new PhraseQuery(FieldNames.TABLE, table);
				metadataTableQuery.add(tableQuery);
				if (facetFilters.size() > 0) {
					BooleanAndQuery clause = new BooleanAndQuery(metadataTableQuery);
					clause.add(facetFilters);
					Clause c = compJoin.addClause(JoinMode.INNER, clause);
					if (!this.tablesToIncludeInFacetCounts.contains(table)) {
						c.setFacet(false);
					}
					if (this.provideFeedback) {
						String message = "Metadata FacetFilter Queries applied to join clause (ACTUAL CLAUSE): "
								+ c.prettyFormat() + "with facet set to: " + c.isFacet();
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.debug(message);
					}
				} else {
					Clause c = compJoin.addClause(JoinMode.OUTER, metadataTableQuery);
					if (!this.tablesToIncludeInFacetCounts.contains(table)) {
						c.setFacet(false);
					}
					if (this.provideFeedback) {
						String message = "No metadata facet filters found. Adding clause (ACTUAL CLAUSE): "
								+ c.prettyFormat() + "with facet set to: " + c.isFacet();
						feedback.add(
								new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
						log.debug(message);
					}
				}
			} else {
				PhraseQuery clause = new PhraseQuery(FieldNames.TABLE, table);
				compJoin.addClause(JoinMode.OUTER, clause);
				if (this.provideFeedback) {
					String message = "No metadata facet filters found. Adding clause: " + clause;
					feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner", message));
					log.debug(message);
				}
			}
		}
		return compJoin;
	}

	/**
	 * Depending on whether the primaryTables or nonPrimaryTables field is
	 * populated, generates a query that will only select records from the
	 * appropriate table(s), which can then be used as the query for the "FROM"
	 * table
	 * 
	 * @return
	 * @throws AttivioException
	 */
	protected Query generateFromQuery() throws AttivioException {
		if (this.primaryTables != null && this.primaryTables.size() > 0) {
			BooleanOrQuery orQ = new BooleanOrQuery();
			for (String tableName : primaryTables) {
				orQ.add(new PhraseQuery(FieldNames.TABLE, tableName));
			}
			return orQ;
		} else {
			BooleanOrQuery orQ = new BooleanOrQuery();
			for (String tableName : this.nonPrimaryTables) {
				orQ.add(new PhraseQuery(FieldNames.TABLE, tableName));
			}
			return new BooleanNotQuery(orQ);
		}
	}
}
