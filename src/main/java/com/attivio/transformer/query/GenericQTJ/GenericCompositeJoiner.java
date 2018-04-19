package com.attivio.transformer.query.GenericQTJ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import com.attivio.util.ObjectUtils;

/**
 * @author brandon.bogan
 *
 */
@ConfigurationOptionInfo(displayName = "Generic Composite Joiner",
    description = "Transforms query into a Composite Join with a specified tables",
    groups = {
        @ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.PLATFORM_COMPONENT,
            propertyNames = {"primaryTables", "nonPrimaryTables", "metadataTables", "joinField",
                "maxChildDocs", "propertiesToPreserve", "metadataFacetFields",
                "tablesToIncludeInFacetCounts"}),
        @ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.ADVANCED,
            propertyNames = {"ignoreAdvancedQueries", "provideFeedback"})})
public class GenericCompositeJoiner implements QueryTransformer {

  protected List<String> primaryTables;
  protected List<String> nonPrimaryTables;
  protected List<String> metadataTables;
  protected String joinField;
  protected int maxChildDocs;
  protected boolean provideFeedback;
  protected List<String> propertiesToPreserve = new ArrayList<String>();
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
  @ConfigurationOption(displayName = "Primary Tables",
      description = "Tables containing primary data, to which metadata tables will be joined. Either primary tables or non-primary tables must be provided, but not both.",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getPrimaryTables() {
    return primaryTables;
  }

  /**
   * @param primaryTables
   */
  public void setPrimaryTables(List<String> primaryTables) {
    this.primaryTables = primaryTables;
  }

  @ConfigurationOption(displayName = "Non-Primary Tables",
      description = "Tables to exlude from being parent records. Either primary tables or non-primary tables must be provided, but not both.",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getNonPrimaryTables() {
    return nonPrimaryTables;
  }

  public void setNonPrimaryTables(List<String> nonPrimaryTables) {
    this.nonPrimaryTables = nonPrimaryTables;
  }

  /**
   * @return the metadataTable
   */
  @ConfigurationOption(displayName = "Metadata Tables",
      description = "Tables containing metadata to leverage in JOIN",
      optionLevel = OptionLevel.Required, formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getMetadataTables() {
    return metadataTables;
  }

  /**
   * @param metadataTable the metadataTable to set
   */
  public void setMetadataTables(List<String> metadataTables) {
    this.metadataTables = metadataTables;
  }

  /**
   * @return the joinField
   */
  @ConfigurationOption(displayName = "Join Field",
      description = "Field to join on (must be the same field across both sources)",
      optionLevel = OptionLevel.Required)
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
  @ConfigurationOption(displayName = "Max Child Docs",
      description = "Max number of child docs to relate to parent doc. Set to -1 for no limit.",
      optionLevel = OptionLevel.Required)
  public String getMaxChildDocs() {
    return String.valueOf(maxChildDocs);
  }

  /**
   * @param maxChildDocs the maxChildDocs to set
   */
  public void setMaxChildDocs(String maxChildDocs) {
    this.maxChildDocs = Integer.valueOf(maxChildDocs);
  }

  /**
   * @return the propertiesToPreserve
   */
  @ConfigurationOption(displayName = "Properties To Preserve",
      description = "Properties on the QueryRequest that should be preserved if found",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getPropertiesToPreserve() {
    return propertiesToPreserve;
  }

  /**
   * @param propertiesToPreserve the propertiesToPreserve to set
   */
  public void setPropertiesToPreserve(List<String> propertiesToPreserve) {
    this.propertiesToPreserve = propertiesToPreserve;
  }

  /**
   * @return the metadataFacetFields
   */
  @ConfigurationOption(displayName = "Metadata Facet Fields",
      description = "Map of table to Fields from that table that are used as facets",
      formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
  public Map<String, String> getMetadataFacetFields() {
    Map<String, String> response = new HashMap<String, String>();
    for (String key : this.metadataFacetFields.keySet()) {
      List<String> facetFields = this.metadataFacetFields.get(key);
      for (String field : facetFields) {
        response.put(key, field);
      }
    }
    return response;
  }

  /**
   * @param metadataFacetFields the metadataFacetFields to set
   */
  public void setMetadataFacetFields(Map<String, String> metadataFacetFields) {
    Map<String, List<String>> tableFacets = new HashMap<String, List<String>>();
    for (String key : metadataFacetFields.keySet()) {
      key = key.trim();
      String[] facetFields = metadataFacetFields.get(key).replace(" ", "").split(",");
      tableFacets.put(key, Arrays.asList(facetFields));
      System.out
          .println("Adding facet fields " + Arrays.toString(facetFields) + " for table " + key);
    }
    this.metadataFacetFields = tableFacets;
  }

  @ConfigurationOption(displayName = "Provide Query Feedback",
      description = "Should detailed query feedback be provided",
      formEntryClass = ConfigurationOption.FALSE_SWITCH_VALUE)
  public boolean isProvideFeedback() {
    return provideFeedback;
  }

  public void setProvideFeedback(boolean provideFeedback) {
    this.provideFeedback = provideFeedback;
  }

  @ConfigurationOption(displayName = "Ignore Advanced Queries",
      description = "Should qeries in the advanced query language be ignored",
      formEntryClass = ConfigurationOption.FALSE_SWITCH_VALUE)
  public boolean isIgnoreAdvancedQueries() {
    return ignoreAdvancedQueries;
  }

  public void setIgnoreAdvancedQueries(boolean ignoreAdvancedQueries) {
    this.ignoreAdvancedQueries = ignoreAdvancedQueries;
  }

  // public boolean isExcludeFromFacets() {
  // return excludeFromFacets;
  // }
  //
  // public void setExcludeFromFacets(boolean excludeFromFacets) {
  // this.excludeFromFacets = excludeFromFacets;
  // }

  @ConfigurationOption(displayName = "Facet Metadata Tables",
      description = "Tables to include in facet counts",
      formEntryClass = ConfigurationOption.STRING_LIST)
  public List<String> getTablesToIncludeInFacetCounts() {
    return tablesToIncludeInFacetCounts;
  }

  public void setTablesToIncludeInFacetCounts(List<String> tablesToIncludeInFacetCounts) {
    this.tablesToIncludeInFacetCounts = tablesToIncludeInFacetCounts;
  }

  @Override
  public List<QueryFeedback> processQuery(QueryRequest qr) throws AttivioException {
    List<QueryFeedback> feedback = new ArrayList<QueryFeedback>();

    if (qr.getQueryLanguage().equalsIgnoreCase(QueryLanguages.ADVANCED)
        && this.ignoreAdvancedQueries) {
      if (this.provideFeedback) {
        feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
            "Advanced Query Language in use, skipping stage."));
      }
    } else {
      Map<String, Object> properties = this.extractProperties(qr, feedback);
      properties.put("join.facet", "FULL");
      Map<String, List<Query>> facetFilters = this.extractMetadataFacetFilterQueries(qr, feedback);
      Query joinQuery = this.buildCompositeJoinQuery(facetFilters, qr, feedback);
      if (this.provideFeedback) {
        feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
            "Final Composite Join Query: " + joinQuery.prettyFormat()));
      }
      qr.setQuery(joinQuery);
      this.addPropertiesToQueryRequest(properties, qr, feedback);
    }
    return feedback;
  }

  private Map<String, Object> extractProperties(QueryRequest qr, List<QueryFeedback> feedback) {
    Map<String, Object> properties = new HashMap<String, Object>();
    for (String key : this.propertiesToPreserve) {
      if (qr.hasProperty(key)) {
        properties.put(key, qr.getProperty(key));
        feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
            "Copied Property: " + key));
      }
    }
    return properties;
  }

  /**
   * Strips both filter queries and facet filters that pertain to the table being joined to from the
   * {@code QueryRequest} and converts them into a list of{@code Queries} which can be added into
   * the {@code CompositeJoin} down the line.
   * 
   * @param qr The {@code QueryRequest} to strip the specific filter queries and facet filters from
   * @param feedback List of feedback to add additional feedback to
   * @return A list of {@code Query} objects that represent the stripped filters and facet filters
   */
  private Map<String, List<Query>> extractMetadataFacetFilterQueries(QueryRequest qr,
      List<QueryFeedback> feedback) {

    Map<String, List<Query>> filterSubQueries = new HashMap<String, List<Query>>();
    for (String table : this.metadataFacetFields.keySet()) {
      filterSubQueries.put(table, this.extractMetadataFacetFilterQueriesHelper(qr, feedback,
          this.metadataFacetFields.get(table)));
    }

    return filterSubQueries;
  }

  private List<Query> extractMetadataFacetFilterQueriesHelper(QueryRequest qr,
      List<QueryFeedback> feedback, List<String> fieldNames) {

    List<Query> facetSubQueries = new ArrayList<Query>();

    // Collect both Filters and FacetFilters that have been applied to the query
    List<Query> originalFilters = qr.getFilters();
    List<Query> newFilters = new ArrayList<Query>();

    List<FacetFilter> originalFacetFilters = qr.getFacetFilters();
    List<FacetFilter> newFacetFilters = new ArrayList<FacetFilter>();


    // Handle the filter queries first. These are a little more complex to get the field and value
    // from, so we use Regex.
    // NOTE: There is probably a better way that I don't know about that doesn't require Regex.
    // The idea here is loop through the filters, and if any of them pertain to the metadata facet
    // fields, extract them into specific queries that we can leverage within out composite join.
    for (Query filterQuery : originalFilters) {
      String queryString = filterQuery.getQueryString();
      boolean matchFound = false;
      for (String facetFieldName : fieldNames) {
        if (queryString.contains(facetFieldName)) {
          if (this.provideFeedback) {
            feedback
                .add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
                    "Found metadata facet filter in QueryFilter: " + queryString));
          }
          String regex = facetFieldName + ":(.*)[,)]";
          Pattern pattern = Pattern.compile(regex);
          Matcher m = pattern.matcher(queryString);
          if (m.find() && m.groupCount() >= 1) {
            matchFound = true;
            String fieldValue = m.group(1);
            QueryRequest filterQueryRequest = new QueryRequest();
            filterQueryRequest.setQuery(facetFieldName + ":" + fieldValue, "simple");
            facetSubQueries.add(filterQueryRequest.getQuery());
            if (this.provideFeedback) {
              feedback.add(new QueryFeedback(this.getClass().getSimpleName(),
                  "GenericCompositeJoiner", "Found " + facetFieldName
                      + ", stripping from filter and building into query..."));
            }
          }
        }
      }
      if (!matchFound) {
        if (this.provideFeedback) {
          feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
              "Not a filter of interest - ignoring...: " + filterQuery.toString()));
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
            feedback
                .add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
                    "Found metadata facet filter in QueryFilter: " + queryString));
          }
          String regex = facetFieldName + ":FACET.{1}(.*).{1},{0,1}";
          System.out.println("MATCHING: " + regex + " ON " + queryString);
          Pattern pattern = Pattern.compile(regex);
          Matcher m = pattern.matcher(queryString);
          if (m.find() && m.groupCount() >= 1) {
            matchFound = true;
            String fieldValue = m.group(1);
            QueryRequest filterQueryRequest = new QueryRequest();
            filterQueryRequest.setQuery(facetFieldName + ":" + fieldValue, "simple");
            facetSubQueries.add(filterQueryRequest.getQuery());
            if (this.provideFeedback) {
              feedback.add(new QueryFeedback(this.getClass().getSimpleName(),
                  "GenericCompositeJoiner", "Found " + facetFieldName
                      + ", stripping from filter and building into query..."));
            }
          }
        }
      }
      if (!matchFound) {
        if (this.provideFeedback) {
          feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
              "Not a filter of interest - ignoring...: " + f.toString()));
        }
        newFacetFilters.add(f);
      }
    }
    qr.setFacetFilters(newFacetFilters);
    return facetSubQueries;
  }

  /**
   * Constructs a {@code CompositeJoinQuery} based on the configurations and what facet filters were
   * found that pertain to the metadata. <br>
   * <br>
   * If there are no metadata facet filters, it constructs an {@code OUTER JOIN}. However, if there
   * are metadata facets, it constructs an {@code INNER JOIN}, such that only the documents in the
   * {@code FROM} clause that have the specified metadata attribute(s) are returned.
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
    System.out.println("Composite Join Primary Query: " + compJoin.getQuery());

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
          // FLYNN START
          Clause c = compJoin.addClause(JoinMode.INNER, clause);
          if (!this.tablesToIncludeInFacetCounts.contains(table)) {
            c.setFacet(false);
          }
          // FLYNN END
          if (this.provideFeedback) {
            feedback
                .add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
                    "Metadata FacetFilter Queries applied to join clause (ACTUAL CLAUSE): "
                        + c.prettyFormat() + "with facet set to: " + c.isFacet()));
          }
        } else {
          // PhraseQuery clause = new PhraseQuery(FieldNames.TABLE,
          // this.metadataTables);
          Clause c = compJoin.addClause(JoinMode.OUTER, metadataTableQuery);
          if (!this.tablesToIncludeInFacetCounts.contains(table)) {
            c.setFacet(false);
          }
          if (this.provideFeedback) {
            feedback
                .add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
                    "No metadata facet filters found. Adding clause (ACTUAL CLAUSE): "
                        + c.prettyFormat() + "with facet set to: " + c.isFacet()));
          }
        }
      } else {
        PhraseQuery clause = new PhraseQuery(FieldNames.TABLE, table);
        compJoin.addClause(JoinMode.OUTER, clause);
        if (this.provideFeedback) {
          feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
              "No metadata facet filters found. Adding clause: " + clause));
        }
      }
    }
    return compJoin;
  }

  /**
   * Depending on whether the primaryTables or nonPrimaryTables field is populated, generates a
   * query that will only select records from the appropriate table(s), which can then be used as
   * the query for the "FROM" table
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
    } else if (this.nonPrimaryTables != null && this.nonPrimaryTables.size() > 0) {
      BooleanOrQuery orQ = new BooleanOrQuery();
      for (String tableName : this.nonPrimaryTables) {
        orQ.add(new PhraseQuery(FieldNames.TABLE, tableName));
      }
      return new BooleanNotQuery(orQ);
    } else {
      throw new AttivioException(null,
          "Neither primary tables nor non-primary tables were configured for GenericCompositeJoiner",
          null);
    }
  }

  /**
   * Copies a set of query properties onto a query object
   * 
   * @param properties A Map of property names and values to copy onto the query
   * @param qr the query request to copy the properties onto
   * @return The query with the properties added
   */
  private void addPropertiesToQueryRequest(Map<String, Object> properties, QueryRequest qr,
      List<QueryFeedback> feedback) {
    for (String key : properties.keySet()) {
      qr.setProperty(key, properties.get(key));
      if (this.provideFeedback) {
        feedback.add(new QueryFeedback(this.getClass().getSimpleName(), "GenericCompositeJoiner",
            "Added property " + key + " and value " + properties.get(key) + " to QueryRequest"));
      }
    }
  }

}
