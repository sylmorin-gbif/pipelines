package org.gbif.pipelines.ingest.transform;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.OCCURRENCE_EXT_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.*;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.Transform;

public class OccurrenceTransform extends Transform<ExtendedRecord, Occurrences> {

  private final SerializableSupplier<VocabularyService> vocabularyServiceSupplier;

  @Builder(buildMethodName = "create")
  private OccurrenceTransform(
      List<DateComponentOrdering> orderings,
      SerializableSupplier<VocabularyService> vocabularyServiceSupplier,
      SerializableSupplier<KeyValueStore<String, OccurrenceStatus>> occStatusKvStoreSupplier,
      SerializableSupplier<KeyValueStore<NameSearch, NameUsageMatch>> nameMatchStoreSupplier,
      KeyValueStore<NameSearch, NameUsageMatch> nameMatchStore,
      KeyValueStore<String, Boolean> kingdomCheckStore,
      SerializableSupplier<KeyValueStore<String, Boolean>> kingdomCheckStoreSupplier,
      KeyValueStore<String, ALACollectoryMetadata> dataResourceStore,
      SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
          dataResourceStoreSupplier) {
    super(Occurrences.class, OCCURRENCE, OccurrenceTransform.class.getName(), OCCURRENCE_EXT_COUNT);
    //        this.isTripletValid = isTripletValid;
    //        this.isOccurrenceIdValid = isOccurrenceIdValid;
    //        this.useExtendedRecordId = useExtendedRecordId;
    //        this.useDynamicPropertiesInterpretation = useDynamicPropertiesInterpretation;
    //        this.gbifIdFn = gbifIdFn;
    //        this.keygenServiceSupplier = keygenServiceSupplier;
    //        this.occStatusKvStoreSupplier = occStatusKvStoreSupplier;
    this.vocabularyServiceSupplier = vocabularyServiceSupplier;
    //        this.clusteringServiceSupplier = clusteringServiceSupplier;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {}

  /** Beam @Setup can be applied only to void method */
  public OccurrenceTransform init() {
    setup();
    return this;
  }

  /** Maps {@link ImageRecord} to key value, where key is {@link ImageRecord#getId} */
  public MapElements<ImageRecord, KV<String, ImageRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ImageRecord>>() {})
        .via((ImageRecord ir) -> KV.of(ir.getId(), ir));
  }

  public OccurrenceTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<Occurrences> convert(ExtendedRecord er) {
    if (ModelUtils.hasExtension(er, "http://rs.tdwg.org/dwc/terms/Occurrence")) {
      Occurrences ors =
          Occurrences.newBuilder()
              .setId(er.getId())
              .setOccurrenceRecords(new ArrayList<>())
              .build();

      List<Map<String, String>> occurrenceExtension =
          er.getExtensions().get("http://rs.tdwg.org/dwc/terms/Occurrence");

      occurrenceExtension.forEach(
          rawOcc -> {
            String occurrenceID = rawOcc.get(DwcTerm.occurrenceID.simpleName());
            if (occurrenceID != null) {
              BasicRecord br = BasicRecord.newBuilder().build();
              //                    ALATaxonRecord atr = ALATaxonRecord.newBuilder().build();
              //                    BasicInterpreter.interpretBasisOfRecord(rawOcc, br);
              Occurrence orec =
                  Occurrence.newBuilder()
                      .setOccurrenceID(occurrenceID)
                      .setBasic(br)
                      .setAlaTaxon(null)
                      .build();
              ors.getOccurrenceRecords().add(orec);
            }
          });
      return Optional.of(ors);
    } else {
      return Optional.empty();
    }
  }
}
