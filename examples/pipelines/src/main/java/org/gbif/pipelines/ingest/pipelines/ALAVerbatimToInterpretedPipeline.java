package org.gbif.pipelines.ingest.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER_ABSENT;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.cache.ALANameCheckKVStoreFactory;
import au.org.ala.kvs.cache.ALANameMatchKVStoreFactory;
import au.org.ala.kvs.cache.GeocodeKvStoreFactory;
import au.org.ala.pipelines.transforms.ALATaxonomyTransform;
import au.org.ala.pipelines.transforms.ALATemporalTransform;
import au.org.ala.pipelines.transforms.LocationTransform;
import au.org.ala.pipelines.transforms.MetadataTransform;
import au.org.ala.utils.CombinedYamlConfiguration;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.factory.FileVocabularyFactory;
import org.gbif.pipelines.ingest.pipelines.interpretation.TransformsFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.core.*;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.IdentifierRecord},
 *      {@link org.gbif.pipelines.io.avro.EventCoreRecord},
 *      {@link ExtendedRecord}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -jar target/examples-pipelines-BUILD_VERSION-shaded.jar
 * --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 * --attempt=1
 * --runner=SparkRunner
 * --targetPath=/some/path/to/output/
 * --inputPath=/some/path/to/output/0057a720-17c9-4658-971e-9578f3577cf5/1/verbatim.avro
 *
 * }</pre>
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAVerbatimToInterpretedPipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Event;

  public static void main(String[] args) throws IOException {
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "interpret");
    InterpretationPipelineOptions options =
        PipelinesOptionsFactory.createInterpretation(combinedArgs);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    run(options, Pipeline::create);
  }

  public static void run(
      InterpretationPipelineOptions options,
      Function<InterpretationPipelineOptions, Pipeline> pipelinesFn) {

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Set<String> types = options.getInterpretationTypes();
    String targetPath = options.getTargetPath();

    MDC.put("datasetKey", datasetId);
    MDC.put("step", StepType.EVENTS_VERBATIM_TO_INTERPRETED.name());
    MDC.put("attempt", attempt.toString());

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    ALAPipelinesConfig config =
        FsUtils.readConfigFile(hdfsConfigs, options.getProperties(), ALAPipelinesConfig.class);

    TransformsFactory transformsFactory = TransformsFactory.create(options);

    // Remove directories with avro files for expected interpretation, except IDENTIFIER
    Set<String> deleteTypes = new HashSet<>(types);
    deleteTypes.add(IDENTIFIER.name());
    deleteTypes.remove(IDENTIFIER_ABSENT.name());
    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, deleteTypes);

    String id = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    UnaryOperator<String> pathFn =
        t -> PathBuilder.buildPathInterpretUsingTargetPath(options, CORE_TERM, t, id);

    log.info("Creating a pipeline from options");
    Pipeline p = pipelinesFn.apply(options);

    // Used transforms
    // Metadata
    MetadataTransform metadataTransform =
        MetadataTransform.builder()
            .dataResourceKvStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .datasetId(datasetId)
            .create();
    LocationTransform locationTransform =
        LocationTransform.builder()
            .alaConfig(config)
            .countryKvStoreSupplier(GeocodeKvStoreFactory.createCountrySupplier(config))
            .stateProvinceKvStoreSupplier(GeocodeKvStoreFactory.createStateProvinceSupplier(config))
            .biomeKvStoreSupplier(GeocodeKvStoreFactory.createBiomeSupplier(config))
            .create();
    EventCoreTransform eventCoreTransform =
        EventCoreTransform.builder()
            .vocabularyServiceSupplier(
                FileVocabularyFactory.builder()
                    .config(config.getGbifConfig())
                    .hdfsConfigs(hdfsConfigs)
                    .build()
                    .getInstanceSupplier())
            .create();
    ;
    IdentifierTransform identifierTransform = transformsFactory.createIdentifierTransform();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    ALATaxonomyTransform alaTaxonomyTransform =
        ALATaxonomyTransform.builder()
            .datasetId(datasetId)
            .nameMatchStoreSupplier(ALANameMatchKVStoreFactory.getInstanceSupplier(config))
            .kingdomCheckStoreSupplier(
                ALANameCheckKVStoreFactory.getInstanceSupplier("kingdom", config))
            .dataResourceStoreSupplier(ALAAttributionKVStoreFactory.getInstanceSupplier(config))
            .create();

    MultimediaTransform multimediaTransform = transformsFactory.createMultimediaTransform();
    AudubonTransform audubonTransform = transformsFactory.createAudubonTransform();
    ImageTransform imageTransform = transformsFactory.createImageTransform();
    ALATemporalTransform temporalTransform = ALATemporalTransform.builder().create();
    // Extension
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    log.info("Creating beam pipeline");
    PCollection<MetadataRecord> metadataRecord =
        p.apply("Create metadata collection", Create.of(options.getDatasetId()))
            .apply("Interpret metadata", metadataTransform.interpret());

    metadataRecord.apply("Write metadata to avro", metadataTransform.write(pathFn));

    // Read raw records and filter duplicates
    PCollection<ExtendedRecord> uniqueRawRecords =
        p.apply("Read event  verbatim", verbatimTransform.read(options.getInputPath()))
            .apply("Filter event duplicates", transformsFactory.createUniqueIdTransform())
            .apply("Filter event extensions", transformsFactory.createExtensionFilterTransform());

    // view with the records that have parents to find the hierarchy in the event core
    // interpretation later
    PCollectionView<Map<String, String>> erWithParentEventsView =
        uniqueRawRecords
            .apply(
                Filter.by(
                    (SerializableFunction<ExtendedRecord, Boolean>)
                        input -> extractOptValue(input, DwcTerm.parentEventID).isPresent()))
            .apply(verbatimTransform.toParentEventsKv())
            .apply("View to find parents", View.asMap());
    eventCoreTransform.setErWithParentsView(erWithParentEventsView);

    // Interpret identifiers and write as avro files
    uniqueRawRecords
        .apply("Interpret event identifiers", identifierTransform.interpret())
        .apply("Write event identifiers to avro", identifierTransform.write(pathFn));

    // Interpret event core records and write as avro files
    uniqueRawRecords
        .apply("Interpret event core", eventCoreTransform.interpret())
        .apply("Write event core to avro", eventCoreTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret event temporal", temporalTransform.interpret())
        .apply("Write event temporal to avro", temporalTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret event taxonomy", alaTaxonomyTransform.interpret())
        .apply("Write event taxon to avro", alaTaxonomyTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret event multimedia", multimediaTransform.interpret())
        .apply("Write event multimedia to avro", multimediaTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret event audubon", audubonTransform.interpret())
        .apply("Write event audubon to avro", audubonTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret event image", imageTransform.interpret())
        .apply("Write event image to avro", imageTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret event location", locationTransform.interpret())
        .apply("Write event location to avro", locationTransform.write(pathFn));

    uniqueRawRecords
        .apply("Interpret measurementOrFact", measurementOrFactTransform.interpret())
        .apply("Write measurementOrFact to avro", measurementOrFactTransform.write(pathFn));

    // Write filtered verbatim avro files
    uniqueRawRecords.apply("Write event verbatim to avro", verbatimTransform.write(pathFn));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    log.info("Save metrics into the file and set files owner");
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    FsUtils.setOwnerToCrap(hdfsConfigs, metadataPath);

    log.info("Deleting beam temporal folders");
    String tempPath = String.join("/", targetPath, datasetId, attempt.toString());
    FsUtils.deleteDirectoryByPrefix(hdfsConfigs, tempPath, ".temp-beam");

    log.info("Set interpreted files permissions");
    String interpretedPath =
        PathBuilder.buildDatasetAttemptPath(options, CORE_TERM.simpleName(), false);
    FsUtils.setOwnerToCrap(hdfsConfigs, interpretedPath);

    log.info("Pipeline has been finished");
  }
}
