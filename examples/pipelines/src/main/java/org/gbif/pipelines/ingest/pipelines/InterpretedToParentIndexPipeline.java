package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;

import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.EsIndexingPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.ingest.utils.ElasticsearchTools;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.transforms.common.NotNullOrEmptyFilter;
import org.gbif.pipelines.transforms.converters.ParentEventExpandTransform;
import org.gbif.pipelines.transforms.converters.ParentJsonTransform;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads avro files:
 *      {@link EventCoreRecord},
 *      {@link IdentifierRecord},
 *      {@link ExtendedRecord},
 *    2) Joins avro files
 *    3) Converts to json model (resources/elasticsearch/es-event-core-schema.json)
 *    4) Pushes data to Elasticsearch instance
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/examples-pipelines-BUILD_VERSION-shaded.jar
 *  --pipelineStep=INTERPRETED_TO_ES_INDEX \
 *  --datasetId=4725681f-06af-4b1e-8fff-e31e266e0a8f \
 *  --attempt=1 \
 *  --runner=SparkRunner \
 *  --inputPath=/path \
 *  --targetPath=/path \
 *  --esIndexName=test2_java \
 *  --esAlias=occurrence2_java \
 *  --indexNumberShards=3 \
 * --esHosts=http://ADDRESS:9200,http://ADDRESS:9200,http://ADDRESS:9200 \
 * --esDocumentId=id
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedToParentIndexPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Event;

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "elastic");
    EsIndexingPipelineOptions options = PipelinesOptionsFactory.createIndexing(combinedArgs);
    run(options);
  }

  public static void run(EsIndexingPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      EsIndexingPipelineOptions options,
      Function<EsIndexingPipelineOptions, Pipeline> pipelinesFn) {

    MDC.put("datasetKey", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());

    String esDocumentId = options.getEsDocumentId();

    ElasticsearchTools.createIndexAndAliasForDefault(options);

    log.info("Adding step 1: Options");
    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, ALL_AVRO);

    UnaryOperator<String> occurrencesPathFn =
        t ->
            PathBuilder.buildPathInterpretUsingTargetPath(options, DwcTerm.Occurrence, t, ALL_AVRO);

    options.setAppName("Event indexing of " + options.getDatasetId());
    Pipeline p = pipelinesFn.apply(options);

    log.info("Adding step 2: Creating transformations");
    MetadataTransform metadataTransform = MetadataTransform.builder().create();
    // Core
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();
    ALATaxonomyTransform alaTaxonomyTransform = ALATaxonomyTransform.builder().create();
    IdentifierTransform identifierTransform = IdentifierTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    LocationTransform parentLocationTransform = LocationTransform.builder().create();
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    log.info("Adding step 3: Creating beam pipeline");
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", metadataTransform.read(pathFn))
            .apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", verbatimTransform.read(pathFn))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> identifierCollection =
        p.apply("Read identifiers", identifierTransform.read(pathFn))
            .apply("Map identifiers to KV", identifierTransform.toKv());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        p.apply("Read Event core", eventCoreTransform.read(pathFn))
            .apply("Map Event core to KV", eventCoreTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", temporalTransform.read(pathFn))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", locationTransform.read(pathFn))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, ALATaxonRecord>> taxonCollection =
        p.apply("Read event taxon records", alaTaxonomyTransform.read(pathFn))
            .apply("Map event taxon records to KV", alaTaxonomyTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", multimediaTransform.read(pathFn))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Image", imageTransform.read(pathFn))
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Audubon", audubonTransform.read(pathFn))
            .apply("Map Audubon to KV", audubonTransform.toKv());

    PCollection<KV<String, MeasurementOrFactRecord>> measurementOrFactCollection =
        p.apply("Read Measurement or fact", measurementOrFactTransform.read(pathFn))
            .apply("Map Measurement or fact to KV", measurementOrFactTransform.toKv());

    PCollection<KV<String, DenormalisedEvent>> denormCollection =
        getEventDenormalisation(options, p);

    TupleTag<DenormalisedEvent> denormalisedEventTag = new TupleTag<>();

    PCollection<KV<String, DerivedMetadataRecord>> derivedMetadataRecordCollection =
        DerivedMetadata.builder()
            .pipeline(p)
            .verbatimTransform(verbatimTransform)
            .temporalTransform(temporalTransform)
            .parentLocationTransform(parentLocationTransform)
            .taxonomyTransform(alaTaxonomyTransform)
            .locationTransform(locationTransform)
            .eventCoreTransform(eventCoreTransform)
            .verbatimCollection(verbatimCollection)
            .temporalCollection(temporalCollection)
            .locationCollection(locationCollection)
            .taxonCollection(taxonCollection)
            .eventCoreCollection(eventCoreCollection)
            .occurrencesPathFn(occurrencesPathFn)
            .build()
            .calculate();

    log.info("Adding step 3: Converting into a json object");
    SingleOutput<KV<String, CoGbkResult>, String> eventJsonDoFn =
        ParentJsonTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .identifierRecordTag(identifierTransform.getTag())
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .measurementOrFactRecordTag(measurementOrFactTransform.getTag())
            .denormalisedEventTag(denormalisedEventTag)
            .derivedMetadataRecordTag(DerivedMetadataTransform.tag())
            .metadataView(metadataView)
            .build()
            .converter();

    PCollection<String> jsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(eventCoreTransform.getTag(), eventCoreCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            // Extension
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            // Internal
            .and(identifierTransform.getTag(), identifierCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            .and(measurementOrFactTransform.getTag(), measurementOrFactCollection)
            // denorm
            .and(denormalisedEventTag, denormCollection)
            // Derived metadata
            .and(DerivedMetadataTransform.tag(), derivedMetadataRecordCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", eventJsonDoFn);

    log.info("Adding step 4: Elasticsearch indexing");

    ElasticsearchIO.ConnectionConfiguration esConfig =
        ElasticsearchIO.ConnectionConfiguration.create(
            options.getEsHosts(), options.getEsIndexName(), "_doc");

    if (Objects.nonNull(options.getEsUsername()) && Objects.nonNull(options.getEsPassword())) {
      esConfig =
          esConfig.withUsername(options.getEsUsername()).withPassword(options.getEsPassword());
    }

    ElasticsearchIO.Write writeIO =
        ElasticsearchIO.write()
            .withConnectionConfiguration(esConfig)
            .withMaxBatchSizeBytes(options.getEsMaxBatchSizeBytes())
            .withRoutingFn(
                input ->
                    Optional.of(input.get("joinRecord"))
                        .filter(i -> i.hasNonNull("parent"))
                        .map(i -> i.get("parent").asText())
                        .orElse(input.get("internalId").asText()))
            .withMaxBatchSize(options.getEsMaxBatchSize());

    // Ignore gbifID as ES doc ID, useful for validator
    if (esDocumentId != null && !esDocumentId.isEmpty()) {
      writeIO = writeIO.withIdFn(input -> input.get(esDocumentId).asText());
    }

    jsonCollection.apply(writeIO);

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());

    log.info("Pipeline has been finished");
  }

  /** Load image service records for a dataset. */
  private static PCollection<KV<String, DenormalisedEvent>> getEventDenormalisation(
      EsIndexingPipelineOptions options, Pipeline p) {
    PCollection<KV<String, DenormalisedEvent>> denorm =
        p.apply(
                AvroIO.read(DenormalisedEvent.class)
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)
                    .from(
                        String.join(
                            "/",
                            options.getTargetPath(),
                            options.getDatasetId().trim(),
                            options.getAttempt().toString(),
                            "event",
                            "event_hierarchy",
                            "*.avro")))
            .apply(
                MapElements.into(new TypeDescriptor<KV<String, DenormalisedEvent>>() {})
                    .via((DenormalisedEvent tr) -> KV.of(tr.getId(), tr)));
    return denorm;
  }

  @Builder
  static class DerivedMetadata {
    private final Pipeline pipeline;
    private final VerbatimTransform verbatimTransform;
    private final TemporalTransform temporalTransform;
    private final LocationTransform parentLocationTransform;
    private final ALATaxonomyTransform taxonomyTransform;
    private final EventCoreTransform eventCoreTransform;
    private final LocationTransform locationTransform;
    private final PCollection<KV<String, ExtendedRecord>> verbatimCollection;
    private final PCollection<KV<String, TemporalRecord>> temporalCollection;
    private final PCollection<KV<String, LocationRecord>> locationCollection;
    private final PCollection<KV<String, ALATaxonRecord>> taxonCollection;
    private final PCollection<KV<String, EventCoreRecord>> eventCoreCollection;
    private final UnaryOperator<String> occurrencesPathFn;

    /** Calculates the simple Temporal Coverage of an Event. */
    private PCollection<KV<String, EventDate>> temporalCoverage() {
      PCollection<KV<String, TemporalRecord>> eventOccurrenceTemporalCollection =
          pipeline
              .apply(
                  "Read occurrence event temporal records",
                  temporalTransform.read(occurrencesPathFn))
              .apply(
                  "Remove temporal records with null parent ids",
                  Filter.by(NotNullOrEmptyFilter.of(TemporalRecord::getParentId)))
              .apply(
                  "Map occurrence events temporal records to KV", temporalTransform.toParentKv());

      // Creates a Map of all events and its sub events
      PCollection<KV<String, TemporalRecord>> temporalRecordsOfSubEvents =
          ParentEventExpandTransform.of(temporalTransform.getTag(), eventCoreTransform.getTag())
              .toSubEventsRecords("Temporal", temporalCollection, eventCoreCollection);

      return PCollectionList.of(temporalCollection)
          .and(eventOccurrenceTemporalCollection)
          .and(temporalRecordsOfSubEvents)
          .apply("Joining temporal records", Flatten.pCollections())
          .apply("Calculate the temporal coverage", Combine.perKey(new TemporalCoverageFn()));
    }

    private PCollection<KV<String, String>> convexHull() {
      PCollection<KV<String, LocationRecord>> eventOccurrenceLocationCollection =
          pipeline
              .apply(
                  "Read occurrence events locations",
                  parentLocationTransform.read(occurrencesPathFn))
              .apply(
                  "Remove location records with null parent ids",
                  Filter.by(NotNullOrEmptyFilter.of(LocationRecord::getParentId)))
              .apply("Map occurrence events locations to KV", parentLocationTransform.toParentKv());

      PCollection<KV<String, LocationRecord>> locationRecordsOfSubEvents =
          ParentEventExpandTransform.of(locationTransform.getTag(), eventCoreTransform.getTag())
              .toSubEventsRecords("Location", locationCollection, eventCoreCollection);

      return PCollectionList.of(locationCollection)
          .and(eventOccurrenceLocationCollection)
          .and(locationRecordsOfSubEvents)
          .apply("Joining location records", Flatten.pCollections())
          .apply(
              "Calculate the WKT Convex Hull of all records", Combine.perKey(new ConvexHullFn()));
    }

    private PCollection<KV<String, Iterable<ALATaxonRecord>>> taxonomicCoverage() {
      PCollection<KV<String, ALATaxonRecord>> eventOccurrencesTaxonCollection =
          pipeline
              .apply(
                  "Read event occurrences taxon records", taxonomyTransform.read(occurrencesPathFn))
              //                      .apply(
              //                              "Remove taxon records with null parent ids",
              //
              // Filter.by(NotNullOrEmptyFilter.of(ALATaxonRecord::get)))
              .apply("Map event occurrences taxon to KV", taxonomyTransform.toParentKv());

      PCollection<KV<String, ALATaxonRecord>> taxonRecordsOfSubEvents =
          ParentEventExpandTransform.of(taxonomyTransform.getTag(), eventCoreTransform.getTag())
              .toSubEventsRecords("Taxon", taxonCollection, eventCoreCollection);

      return PCollectionList.of(taxonCollection)
          .and(eventOccurrencesTaxonCollection)
          .and(taxonRecordsOfSubEvents)
          .apply("Join event and occurrence taxon records", Flatten.pCollections())
          .apply("Select a sample of taxon records", Sample.fixedSizePerKey(1000));
    }

    private static final TupleTag<Iterable<ALATaxonRecord>> ITERABLE_ALA_TAXON_TAG =
        new TupleTag<Iterable<ALATaxonRecord>>() {};

    PCollection<KV<String, DerivedMetadataRecord>> calculate() {

      PCollection<KV<String, ExtendedRecord>> eventOccurrenceVerbatimCollection =
          pipeline
              .apply("Read event occurrences verbatim", verbatimTransform.read(occurrencesPathFn))
              .apply(
                  "Remove verbatim records with null parent ids",
                  Filter.by(NotNullOrEmptyFilter.of((ExtendedRecord er) -> er.getParentCoreId())))
              .apply("Map event occurrences verbatim to KV", verbatimTransform.toParentKv());

      return KeyedPCollectionTuple.of(ConvexHullFn.tag(), convexHull())
          .and(TemporalCoverageFn.tag(), temporalCoverage())
          .and(ITERABLE_ALA_TAXON_TAG, taxonomicCoverage())
          .and(
              verbatimTransform.getTag(),
              PCollectionList.of(eventOccurrenceVerbatimCollection)
                  .and(verbatimCollection)
                  .apply("Join event and occurrence verbatim records", Flatten.pCollections()))
          .apply("Grouping derived metadata data", CoGroupByKey.create())
          .apply(
              "Creating derived metadata records",
              DerivedMetadataTransform.builder()
                  .convexHullTag(ConvexHullFn.tag())
                  .temporalCoverageTag(TemporalCoverageFn.tag())
                  .extendedRecordTag(verbatimTransform.getTag())
                  .build()
                  .converter());
    }
  }
}
