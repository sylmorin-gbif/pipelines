package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.*;

@Slf4j
@Builder
public class ParentJsonConverter {

  private final MetadataRecord metadata;
  private final IdentifierRecord identifier;
  private final EventCoreRecord eventCore;
  private final TemporalRecord temporal;
  private final LocationRecord location;
  private final TaxonRecord taxon;
  private final GrscicollRecord grscicoll;
  private final MultimediaRecord multimedia;
  private final ExtendedRecord verbatim;
  private final MeasurementOrFactRecord measurementOrFact;
  private final DenormalisedEvent denormalisedEvent;

  public List<ParentJsonRecord> convertToParents() {

    Integer arraySize =
        Optional.of(verbatim.getExtensions())
            .map(exts -> exts.get(DwcTerm.Occurrence.qualifiedName()))
            .map(List::size)
            .map(size -> size + 1)
            .orElse(1);

    List<ParentJsonRecord> result = new ArrayList<>(arraySize);
    result.add(convertToParentEvent());

    Optional.of(verbatim.getExtensions())
        .map(exts -> exts.get(DwcTerm.Occurrence.qualifiedName()))
        .ifPresent(
            l ->
                l.stream()
                    .map(this::convertToOccurrence)
                    .map(this::convertToParentOccurrence)
                    .forEach(result::add));

    return result;
  }

  public List<String> toJsons() {
    return convertToParents().stream().map(ParentJsonRecord::toString).collect(Collectors.toList());
  }

  private ParentJsonRecord convertToParentEvent() {
    return convertToParent()
        .setType("event")
        .setJoinRecordBuilder(JoinRecord.newBuilder().setName("event"))
        .setEventBuilder(convertToEvent())
        .build();
  }

  private ParentJsonRecord convertToParentOccurrence(OccurrenceMapRecord occurrenceJsonRecord) {
    return convertToParent()
        .setType("occurrence")
        .setInternalId(
            HashConverter.getSha1(
                metadata.getDatasetKey(),
                verbatim.getId(),
                occurrenceJsonRecord.getCore().get(DwcTerm.occurrenceID.simpleName())))
        .setJoinRecordBuilder(
            JoinRecord.newBuilder().setName("occurrence").setParent(identifier.getInternalId()))
        .setOccurrence(occurrenceJsonRecord)
        .build();
  }

  private ParentJsonRecord.Builder convertToParent() {
    ParentJsonRecord.Builder builder =
        ParentJsonRecord.newBuilder()
            .setId(verbatim.getId())
            .setCrawlId(metadata.getCrawlId())
            .setInternalId(identifier.getInternalId())
            .setUniqueKey(identifier.getUniqueKey())
            .setHasCoordinate(location.getHasCoordinate())
            .setDecimalLatitude(location.getDecimalLatitude())
            .setDecimalLongitude(location.getDecimalLongitude())
            .setMetadataBuilder(mapMetadataJsonRecord());

    // Coordinates
    Double decimalLongitude = location.getDecimalLongitude();
    Double decimalLatitude = location.getDecimalLatitude();
    if (decimalLongitude != null && decimalLatitude != null) {
      builder
          .setHasCoordinate(true)
          .setDecimalLatitude(decimalLatitude)
          .setDecimalLongitude(decimalLongitude)
          // geo_point
          .setCoordinates(JsonConverter.convertCoordinates(decimalLongitude, decimalLatitude))
          // geo_shape
          .setScoordinates(JsonConverter.convertScoordinates(decimalLongitude, decimalLatitude));
    }

    mapCreated(builder);

    JsonConverter.convertToDate(identifier.getFirstLoaded()).ifPresent(builder::setFirstLoaded);
    JsonConverter.convertToDate(metadata.getLastCrawled()).ifPresent(builder::setLastCrawled);

    return builder;
  }

  private OccurrenceMapRecord convertToOccurrence(Map<String, String> occurrenceMap) {
    HashMap<String, String> map = new HashMap<>(occurrenceMap.size());
    for (Entry<String, String> entry : occurrenceMap.entrySet()) {
      map.put(TermFactory.instance().findTerm(entry.getKey()).simpleName(), entry.getValue());
    }
    return OccurrenceMapRecord.newBuilder().setCore(map).build();
  }

  private EventJsonRecord.Builder convertToEvent() {

    EventJsonRecord.Builder builder = EventJsonRecord.newBuilder();

    builder.setId(verbatim.getId());
    mapIssues(builder);

    mapEventCoreRecord(builder);
    mapTemporalRecord(builder);
    mapLocationRecord(builder);
    mapMultimediaRecord(builder);
    mapExtendedRecord(builder);

    mapMeasurementOrFactRecord(builder);

    mapDenormalisedEvent(builder);

    return builder;
  }

  private MetadataJsonRecord.Builder mapMetadataJsonRecord() {
    return MetadataJsonRecord.newBuilder()
        .setDatasetKey(metadata.getDatasetKey())
        .setDatasetTitle(metadata.getDatasetTitle())
        .setDatasetPublishingCountry(metadata.getDatasetPublishingCountry())
        .setEndorsingNodeKey(metadata.getEndorsingNodeKey())
        .setInstallationKey(metadata.getInstallationKey())
        .setHostingOrganizationKey(metadata.getHostingOrganizationKey())
        .setNetworkKeys(metadata.getNetworkKeys())
        .setProgrammeAcronym(metadata.getProgrammeAcronym())
        .setProjectId(metadata.getProjectId())
        .setProtocol(metadata.getProtocol())
        .setPublisherTitle(metadata.getPublisherTitle())
        .setPublishingOrganizationKey(metadata.getPublishingOrganizationKey());
  }

  private void mapDenormalisedEvent(EventJsonRecord.Builder builder) {

    if (denormalisedEvent.getParents() != null & !denormalisedEvent.getParents().isEmpty()) {

      List<String> eventTypes = new ArrayList<>();
      List<String> eventIDs = new ArrayList<>();

      boolean hasCoordsInfo = builder.getDecimalLatitude() != null;
      boolean hasCountryInfo = builder.getCountryCode() != null;
      boolean hasStateInfo = builder.getStateProvince() != null;
      boolean hasYearInfo = builder.getYear() != null;
      boolean hasMonthInfo = builder.getMonth() != null;
      boolean hasLocationID = builder.getLocationID() != null;

      // extract location & temporal information from
      denormalisedEvent
          .getParents()
          .forEach(
              parent -> {
                if (!hasYearInfo && parent.getYear() != null) {
                  builder.setYear(parent.getYear());
                }

                if (!hasMonthInfo && parent.getMonth() != null) {
                  builder.setMonth(parent.getMonth());
                }

                if (!hasCountryInfo && parent.getCountryCode() != null) {
                  builder.setCountryCode(parent.getCountryCode());
                }

                if (!hasStateInfo && parent.getStateProvince() != null) {
                  builder.setStateProvince(parent.getStateProvince());
                }

                if (!hasCoordsInfo
                    && parent.getDecimalLatitude() != null
                    && parent.getDecimalLongitude() != null) {
                  builder
                      .setHasCoordinate(true)
                      .setDecimalLatitude(parent.getDecimalLatitude())
                      .setDecimalLongitude(parent.getDecimalLongitude())
                      // geo_point
                      .setCoordinates(
                          JsonConverter.convertCoordinates(
                              parent.getDecimalLongitude(), parent.getDecimalLatitude()))
                      // geo_shape
                      .setScoordinates(
                          JsonConverter.convertScoordinates(
                              parent.getDecimalLongitude(), parent.getDecimalLatitude()));
                }

                if (!hasLocationID && parent.getLocationID() != null) {
                  builder.setLocationID(parent.getLocationID());
                }

                eventIDs.add(parent.getEventID());
                eventTypes.add(parent.getEventType());
              });

      builder.setEventHierarchy(eventIDs);
      builder.setEventTypeHierarchy(eventTypes);
      builder.setEventHierarchyJoined(String.join(" / ", eventIDs));
      builder.setEventTypeHierarchyJoined(String.join(" / ", eventTypes));
      builder.setEventHierarchyLevels(eventIDs.size());
    }
  }

  private void mapEventCoreRecord(EventJsonRecord.Builder builder) {

    // Simple
    builder
        .setSampleSizeValue(eventCore.getSampleSizeValue())
        .setSampleSizeUnit(eventCore.getSampleSizeUnit())
        .setReferences(eventCore.getReferences())
        .setDatasetID(eventCore.getDatasetID())
        .setDatasetName(eventCore.getDatasetName())
        .setSamplingProtocol(eventCore.getSamplingProtocol());

    // Vocabulary
    JsonConverter.convertVocabularyConcept(eventCore.getEventType())
        .ifPresent(builder::setEventType);

    // License
    JsonConverter.convertLicense(eventCore.getLicense()).ifPresent(builder::setLicense);

    // Multi-value fields
    JsonConverter.convertToMultivalue(eventCore.getSamplingProtocol())
        .ifPresent(builder::setSamplingProtocolJoined);
  }

  private void mapTemporalRecord(EventJsonRecord.Builder builder) {

    builder
        .setYear(temporal.getYear())
        .setMonth(temporal.getMonth())
        .setDay(temporal.getDay())
        .setStartDayOfYear(temporal.getStartDayOfYear())
        .setEndDayOfYear(temporal.getEndDayOfYear())
        .setModified(temporal.getModified());

    JsonConverter.convertEventDate(temporal.getEventDate()).ifPresent(builder::setEventDate);
    JsonConverter.convertEventDateSingle(temporal).ifPresent(builder::setEventDateSingle);
  }

  private void mapLocationRecord(EventJsonRecord.Builder builder) {

    builder
        .setContinent(location.getContinent())
        .setWaterBody(location.getWaterBody())
        .setCountry(location.getCountry())
        .setCountryCode(location.getCountryCode())
        .setPublishingCountry(location.getPublishingCountry())
        .setStateProvince(location.getStateProvince())
        .setMinimumElevationInMeters(location.getMinimumElevationInMeters())
        .setMaximumElevationInMeters(location.getMaximumElevationInMeters())
        .setMinimumDepthInMeters(location.getMinimumDepthInMeters())
        .setMaximumDepthInMeters(location.getMaximumDepthInMeters())
        .setMaximumDistanceAboveSurfaceInMeters(location.getMaximumDistanceAboveSurfaceInMeters())
        .setMinimumDistanceAboveSurfaceInMeters(location.getMinimumDistanceAboveSurfaceInMeters())
        .setCoordinateUncertaintyInMeters(location.getCoordinateUncertaintyInMeters())
        .setCoordinatePrecision(location.getCoordinatePrecision())
        .setHasCoordinate(location.getHasCoordinate())
        .setRepatriated(location.getRepatriated())
        .setHasGeospatialIssue(location.getHasGeospatialIssue())
        .setLocality(location.getLocality())
        .setFootprintWKT(location.getFootprintWKT());

    // Coordinates
    Double decimalLongitude = location.getDecimalLongitude();
    Double decimalLatitude = location.getDecimalLatitude();
    if (decimalLongitude != null && decimalLatitude != null) {
      builder
          .setDecimalLatitude(decimalLatitude)
          .setDecimalLongitude(decimalLongitude)
          // geo_point
          .setCoordinates(JsonConverter.convertCoordinates(decimalLongitude, decimalLatitude))
          // geo_shape
          .setScoordinates(JsonConverter.convertScoordinates(decimalLongitude, decimalLatitude));
    }

    JsonConverter.convertGadm(location.getGadm()).ifPresent(builder::setGadm);
  }

  private void mapMultimediaRecord(EventJsonRecord.Builder builder) {
    builder
        .setMultimediaItems(JsonConverter.convertMultimediaList(multimedia))
        .setMediaTypes(JsonConverter.convertMultimediaType(multimedia))
        .setMediaLicenses(JsonConverter.convertMultimediaLicense(multimedia));
  }

  private List<String> extractDistinctFromExtension(String extensionQualifiedName, String term) {
    return Optional.of(verbatim.getExtensions())
        .map(exts -> exts.get(extensionQualifiedName))
        .map(ext -> extractDistinct(term, ext))
        .orElse(new ArrayList<String>());
  }

  private List<String> extractDistinct(String term, List<Map<String, String>> extension) {
    return extension.stream()
        .map(r -> r.get(term))
        .distinct()
        .filter(x -> x != null)
        .limit(100)
        .collect(Collectors.toList());
  }

  private void mapExtendedRecord(EventJsonRecord.Builder builder) {

    // set occurrence count
    Integer occurrenceCount =
        Optional.of(verbatim.getExtensions())
            .map(exts -> exts.get(DwcTerm.Occurrence.qualifiedName()))
            .map(ext -> ext.size())
            .orElse(0);

    builder.setOccurrenceCount(occurrenceCount);

    List<String> kingdoms =
        extractDistinctFromExtension(
            DwcTerm.Occurrence.qualifiedName(), DwcTerm.kingdom.qualifiedName());
    List<String> phyla =
        extractDistinctFromExtension(
            DwcTerm.Occurrence.qualifiedName(), DwcTerm.phylum.qualifiedName());
    List<String> classes =
        extractDistinctFromExtension(
            DwcTerm.Occurrence.qualifiedName(), DwcTerm.class_.qualifiedName());
    List<String> orders =
        extractDistinctFromExtension(
            DwcTerm.Occurrence.qualifiedName(), DwcTerm.order.qualifiedName());
    List<String> families =
        extractDistinctFromExtension(
            DwcTerm.Occurrence.qualifiedName(), DwcTerm.family.qualifiedName());
    List<String> genera =
        extractDistinctFromExtension(
            DwcTerm.Occurrence.qualifiedName(), DwcTerm.genus.qualifiedName());

    builder.setKingdoms(kingdoms);
    builder.setPhyla(phyla);
    builder.setOrders(orders);
    builder.setClasses(classes);
    builder.setFamilies(families);
    builder.setGenera(genera);

    builder.setExtensions(JsonConverter.convertExtenstions(verbatim));

    // Set raw as indexed
    extractOptValue(verbatim, DwcTerm.eventID).ifPresent(builder::setEventId);
    extractOptValue(verbatim, DwcTerm.parentEventID).ifPresent(builder::setParentEventId);
    extractOptValue(verbatim, DwcTerm.institutionCode).ifPresent(builder::setInstitutionCode);
    extractOptValue(verbatim, DwcTerm.verbatimDepth).ifPresent(builder::setVerbatimDepth);
    extractOptValue(verbatim, DwcTerm.verbatimElevation).ifPresent(builder::setVerbatimElevation);
    extractOptValue(verbatim, DwcTerm.locationID).ifPresent(builder::setLocationID);
  }

  private void mapMeasurementOrFactRecord(EventJsonRecord.Builder builder) {
    if (measurementOrFact != null) {

      List<String> methods =
          measurementOrFact.getMeasurementOrFactItems().stream()
              .map(mof -> mof.getMeasurementMethod())
              .filter(s -> Objects.nonNull(s))
              .distinct()
              .collect(Collectors.toList());

      List<String> types =
          measurementOrFact.getMeasurementOrFactItems().stream()
              .map(mof -> mof.getMeasurementType())
              .filter(s -> Objects.nonNull(s))
              .distinct()
              .collect(Collectors.toList());

      builder.setMeasurementOrFactMethods(methods);
      builder.setMeasurementOrFactTypes(types);
      builder.setMeasurementOrFactCount(measurementOrFact.getMeasurementOrFactItems().size());
    }
  }

  private void mapIssues(EventJsonRecord.Builder builder) {
    JsonConverter.mapIssues(
        Arrays.asList(metadata, eventCore, temporal, location, multimedia),
        builder::setIssues,
        builder::setNotIssues);
  }

  private void mapCreated(ParentJsonRecord.Builder builder) {
    JsonConverter.getMaxCreationDate(metadata, eventCore, temporal, location, multimedia)
        .ifPresent(builder::setCreated);
  }
}
