package org.gbif.validator.api;

/**
 * Enumeration of all possible types for an evaluation. OccurrenceIssue are copied here to access
 * all evaluation types by a single enum.
 */
public enum EvaluationType {
  UNHANDLED_ERROR(EvaluationCategory.RESOURCE_INTEGRITY),
  UNREADABLE_SECTION_ERROR(EvaluationCategory.RESOURCE_INTEGRITY),

  DWCA_UNREADABLE(EvaluationCategory.RESOURCE_INTEGRITY),
  DWCA_META_XML_NOT_FOUND(EvaluationCategory.RESOURCE_INTEGRITY),
  DWCA_META_XML_SCHEMA(EvaluationCategory.RESOURCE_INTEGRITY),
  RECORD_IDENTIFIER_NOT_FOUND(EvaluationCategory.RESOURCE_INTEGRITY),
  CORE_ROWTYPE_UNDETERMINED(EvaluationCategory.RESOURCE_INTEGRITY),

  EML_GBIF_SCHEMA(EvaluationCategory.RESOURCE_STRUCTURE),
  EML_NOT_FOUND(EvaluationCategory.RESOURCE_STRUCTURE),
  UNKNOWN_ROWTYPE(EvaluationCategory.RESOURCE_STRUCTURE),
  REQUIRED_TERM_MISSING(EvaluationCategory.RESOURCE_STRUCTURE),
  UNKNOWN_TERM(EvaluationCategory.RESOURCE_STRUCTURE),
  DUPLICATED_TERM(EvaluationCategory.RESOURCE_STRUCTURE),

  LICENSE_MISSING_OR_UNKNOWN(EvaluationCategory.METADATA_CONTENT),
  TITLE_MISSING_OR_TOO_SHORT(EvaluationCategory.METADATA_CONTENT),
  DESCRIPTION_MISSING_OR_TOO_SHORT(EvaluationCategory.METADATA_CONTENT),
  RESOURCE_CONTACTS_MISSING_OR_INCOMPLETE(EvaluationCategory.METADATA_CONTENT),

  RECORD_NOT_UNIQUELY_IDENTIFIED(EvaluationCategory.RESOURCE_STRUCTURE),
  RECORD_REFERENTIAL_INTEGRITY_VIOLATION(EvaluationCategory.RESOURCE_STRUCTURE),
  OCCURRENCE_NOT_UNIQUELY_IDENTIFIED(EvaluationCategory.RESOURCE_STRUCTURE),

  COLUMN_MISMATCH(EvaluationCategory.RECORD_STRUCTURE),

  ZERO_COORDINATE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_OUT_OF_RANGE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_ROUNDED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  GEODETIC_DATUM_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  GEODETIC_DATUM_ASSUMED_WGS84(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_REPROJECTED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_REPROJECTION_FAILED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_REPROJECTION_SUSPICIOUS(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_PRECISION_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COORDINATE_UNCERTAINTY_METERS_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COUNTRY_COORDINATE_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COUNTRY_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COUNTRY_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  COUNTRY_DERIVED_FROM_COORDINATES(EvaluationCategory.OCC_INTERPRETATION_BASED),
  CONTINENT_COORDINATE_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  CONTINENT_COUNTRY_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  CONTINENT_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  CONTINENT_DERIVED_FROM_COUNTRY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  CONTINENT_DERIVED_FROM_COORDINATES(EvaluationCategory.OCC_INTERPRETATION_BASED),
  PRESUMED_SWAPPED_COORDINATE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  PRESUMED_NEGATED_LONGITUDE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  PRESUMED_NEGATED_LATITUDE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  RECORDED_DATE_MISMATCH(EvaluationCategory.OCC_INTERPRETATION_BASED),
  RECORDED_DATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  RECORDED_DATE_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  TAXON_MATCH_FUZZY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  TAXON_MATCH_HIGHERRANK(EvaluationCategory.OCC_INTERPRETATION_BASED),
  TAXON_MATCH_NONE(EvaluationCategory.OCC_INTERPRETATION_BASED),
  DEPTH_NOT_METRIC(EvaluationCategory.OCC_INTERPRETATION_BASED),
  DEPTH_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  DEPTH_MIN_MAX_SWAPPED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  DEPTH_NON_NUMERIC(EvaluationCategory.OCC_INTERPRETATION_BASED),
  ELEVATION_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  ELEVATION_MIN_MAX_SWAPPED(EvaluationCategory.OCC_INTERPRETATION_BASED),
  ELEVATION_NOT_METRIC(EvaluationCategory.OCC_INTERPRETATION_BASED),
  ELEVATION_NON_NUMERIC(EvaluationCategory.OCC_INTERPRETATION_BASED),
  MODIFIED_DATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  MODIFIED_DATE_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  IDENTIFIED_DATE_UNLIKELY(EvaluationCategory.OCC_INTERPRETATION_BASED),
  IDENTIFIED_DATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  BASIS_OF_RECORD_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  TYPE_STATUS_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  MULTIMEDIA_DATE_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  MULTIMEDIA_URI_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  REFERENCES_URI_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),
  INTERPRETATION_ERROR(EvaluationCategory.OCC_INTERPRETATION_BASED),
  INDIVIDUAL_COUNT_INVALID(EvaluationCategory.OCC_INTERPRETATION_BASED),

  PARENT_NAME_USAGE_ID_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  ACCEPTED_NAME_USAGE_ID_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  ORIGINAL_NAME_USAGE_ID_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  ACCEPTED_NAME_MISSING(EvaluationCategory.CLB_INTERPRETATION_BASED),
  RANK_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  NOMENCLATURAL_STATUS_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  TAXONOMIC_STATUS_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  SCIENTIFIC_NAME_ASSEMBLED(EvaluationCategory.CLB_INTERPRETATION_BASED),
  CHAINED_SYNOYM(EvaluationCategory.CLB_INTERPRETATION_BASED),
  BASIONYM_AUTHOR_MISMATCH(EvaluationCategory.CLB_INTERPRETATION_BASED),
  TAXONOMIC_STATUS_MISMATCH(EvaluationCategory.CLB_INTERPRETATION_BASED),
  PARENT_CYCLE(EvaluationCategory.CLB_INTERPRETATION_BASED),
  CLASSIFICATION_RANK_ORDER_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  CLASSIFICATION_NOT_APPLIED(EvaluationCategory.CLB_INTERPRETATION_BASED),
  VERNACULAR_NAME_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  DESCRIPTION_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  DISTRIBUTION_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  SPECIES_PROFILE_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  MULTIMEDIA_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  BIB_REFERENCE_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  ALT_IDENTIFIER_INVALID(EvaluationCategory.CLB_INTERPRETATION_BASED),
  BACKBONE_MATCH_NONE(EvaluationCategory.CLB_INTERPRETATION_BASED),

  // BACKBONE_MATCH_FUZZY is deprecated
  ACCEPTED_NAME_NOT_UNIQUE(EvaluationCategory.CLB_INTERPRETATION_BASED),
  PARENT_NAME_NOT_UNIQUE(EvaluationCategory.CLB_INTERPRETATION_BASED),
  ORIGINAL_NAME_NOT_UNIQUE(EvaluationCategory.CLB_INTERPRETATION_BASED),
  RELATIONSHIP_MISSING(EvaluationCategory.CLB_INTERPRETATION_BASED),
  ORIGINAL_NAME_DERIVED(EvaluationCategory.CLB_INTERPRETATION_BASED),
  CONFLICTING_BASIONYM_COMBINATION(EvaluationCategory.CLB_INTERPRETATION_BASED),
  NO_SPECIES(EvaluationCategory.CLB_INTERPRETATION_BASED),
  NAME_PARENT_MISMATCH(EvaluationCategory.CLB_INTERPRETATION_BASED),
  ORTHOGRAPHIC_VARIANT(EvaluationCategory.CLB_INTERPRETATION_BASED),
  HOMONYM(EvaluationCategory.CLB_INTERPRETATION_BASED),
  PUBLISHED_BEFORE_GENUS(EvaluationCategory.CLB_INTERPRETATION_BASED),
  UNPARSABLE(EvaluationCategory.CLB_INTERPRETATION_BASED),
  PARTIALLY_PARSABLE(EvaluationCategory.CLB_INTERPRETATION_BASED);

  private final EvaluationCategory category;

  EvaluationType(EvaluationCategory category) {
    this.category = category;
  }

  public EvaluationCategory getCategory() {
    return category;
  }
}
