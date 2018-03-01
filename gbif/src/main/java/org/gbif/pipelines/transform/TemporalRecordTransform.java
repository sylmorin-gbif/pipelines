package org.gbif.pipelines.transform;

import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.TemporalRecordInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * This transform provides interpretation for the fields: year, month, day and eventDate
 */
public class TemporalRecordTransform extends RecordTransform<ExtendedRecord, TemporalRecord> {

  public TemporalRecordTransform() {
    super("Interpret temporal record");
  }

  /**
   * Transforms a ExtendedRecord into a TemporalRecord.
   */
  @Override
  DoFn<ExtendedRecord, KV<String, TemporalRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, TemporalRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        // Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();
        List<Validation> validations = new ArrayList<>();

        // Transformation main output
        TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(id).build();

        Interpretation.of(extendedRecord)
          .using(TemporalRecordInterpreter.interpretTemporal(temporalRecord))
          .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        // Additional output
        //additional outputs
        if (!validations.isEmpty()) {
          OccurrenceIssue issue = OccurrenceIssue.newBuilder().setId(id).setIssues(validations).build();
          context.output(getIssueTag(), KV.of(id, issue));
        }

        // Main output
        context.output(getDataTag(), KV.of(temporalRecord.getId(), temporalRecord));
      }
    };
  }
}


