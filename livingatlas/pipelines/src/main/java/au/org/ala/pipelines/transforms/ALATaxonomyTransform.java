package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.ALA_TAXONOMY;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.pipelines.interpreters.ALATaxonomyInterpreter;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * ALA taxonomy transform for adding ALA taxonomy to interpreted occurrence data.
 *
 * <p>Beam level transformations for the DWC Taxon, reads an avro, writes an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link TaxonRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
 * a source and {@link TaxonomyInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@Slf4j
public class ALATaxonomyTransform extends Transform<ExtendedRecord, ALATaxonRecord> {

  private final String datasetId;
  private KeyValueStore<NameSearch, NameUsageMatch> nameMatchStore;
  private final SerializableSupplier<KeyValueStore<NameSearch, NameUsageMatch>>
      nameMatchStoreSupplier;
  private KeyValueStore<String, Boolean> kingdomCheckStore;
  private final SerializableSupplier<KeyValueStore<String, Boolean>> kingdomCheckStoreSupplier;
  private KeyValueStore<String, ALACollectoryMetadata> dataResourceStore;
  private final SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
      dataResourceStoreSupplier;

  @Builder(buildMethodName = "create")
  private ALATaxonomyTransform(
      String datasetId,
      SerializableSupplier<KeyValueStore<NameSearch, NameUsageMatch>> nameMatchStoreSupplier,
      KeyValueStore<NameSearch, NameUsageMatch> nameMatchStore,
      KeyValueStore<String, Boolean> kingdomCheckStore,
      SerializableSupplier<KeyValueStore<String, Boolean>> kingdomCheckStoreSupplier,
      KeyValueStore<String, ALACollectoryMetadata> dataResourceStore,
      SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
          dataResourceStoreSupplier) {
    super(
        ALATaxonRecord.class,
        ALA_TAXONOMY,
        ALATaxonomyTransform.class.getName(),
        "alaTaxonRecordsCount");
    this.datasetId = datasetId;
    this.nameMatchStore = nameMatchStore;
    this.nameMatchStoreSupplier = nameMatchStoreSupplier;
    this.kingdomCheckStore = kingdomCheckStore;
    this.kingdomCheckStoreSupplier = kingdomCheckStoreSupplier;
    this.dataResourceStore = dataResourceStore;
    this.dataResourceStoreSupplier = dataResourceStoreSupplier;
  }

  /** Maps {@link TaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public MapElements<ALATaxonRecord, KV<String, ALATaxonRecord>> asKv(boolean useParent) {
    return MapElements.into(new TypeDescriptor<KV<String, ALATaxonRecord>>() {})
        .via((ALATaxonRecord tr) -> KV.of(useParent ? tr.getId() : tr.getId(), tr));
  }

  /** Maps {@link TaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public MapElements<ALATaxonRecord, KV<String, ALATaxonRecord>> toKv() {
    return asKv(false);
  }

  /** Maps {@link TaxonRecord} to key value, where key is {@link TaxonRecord#getParentId} */
  public MapElements<ALATaxonRecord, KV<String, ALATaxonRecord>> toParentKv() {
    return asKv(true);
  }

  public ALATaxonomyTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup can be applied only to void method */
  public ALATaxonomyTransform init() {
    setup();
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (this.nameMatchStore == null && this.nameMatchStoreSupplier != null) {
      log.info("Initialize NameUsageMatchKvStore");
      this.nameMatchStore = this.nameMatchStoreSupplier.get();
    }
    if (this.kingdomCheckStore == null && this.kingdomCheckStoreSupplier != null) {
      log.info("Initialize NameCheckKvStore");
      this.kingdomCheckStore = this.kingdomCheckStoreSupplier.get();
    }
    if (this.dataResourceStore == null && this.dataResourceStoreSupplier != null) {
      log.info("Initialize CollectoryKvStore");
      this.dataResourceStore = this.dataResourceStoreSupplier.get();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {}

  @Override
  public Optional<ALATaxonRecord> convert(ExtendedRecord source) {
    ALACollectoryMetadata dataResource = this.dataResourceStore.get(datasetId);
    ALATaxonRecord tr = ALATaxonRecord.newBuilder().setId(source.getId()).build();
    BiConsumer<ExtendedRecord, ALATaxonRecord> sourceCheck =
        ALATaxonomyInterpreter.alaSourceQualityChecks(dataResource, kingdomCheckStore);
    BiConsumer<ExtendedRecord, ALATaxonRecord> interpret =
        ALATaxonomyInterpreter.alaTaxonomyInterpreter(dataResource, nameMatchStore);
    BiConsumer<ExtendedRecord, ALATaxonRecord> resultCheck =
        ALATaxonomyInterpreter.alaResultQualityChecks(dataResource);
    Interpretation.from(source)
        .to(tr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(sourceCheck.andThen(interpret).andThen(resultCheck));

    // the id is null when there is an error in the interpretation. In these
    // cases we do not write the taxonRecord because it is totally empty.
    return Optional.of(tr);
  }
}
