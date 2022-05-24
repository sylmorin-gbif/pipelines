package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.REFERENCES_URI_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptListValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;

import com.google.common.base.Strings;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.License;
import org.gbif.common.parsers.LicenseParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;

/**
 * Interpreting function that receives a ExtendedRecord instance and applies an interpretation to
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CoreInterpreter {

  /** {@link DcTerm#references} interpretation. */
  public static void interpretReferences(
      ExtendedRecord er, Issues issues, Consumer<String> consumer) {
    String value = extractValue(er, DcTerm.references);
    if (!Strings.isNullOrEmpty(value)) {
      URI parseResult = UrlParser.parse(value);
      if (parseResult != null) {
        consumer.accept(parseResult.toString());
      } else {
        addIssue(issues, REFERENCES_URI_INVALID);
      }
    }
  }

  /** {@link DwcTerm#sampleSizeValue} interpretation. */
  public static void interpretSampleSizeValue(ExtendedRecord er, Consumer<Double> consumer) {
    extractOptValue(er, DwcTerm.sampleSizeValue)
        .map(String::trim)
        .map(NumberParser::parseDouble)
        .filter(x -> !x.isInfinite() && !x.isNaN())
        .ifPresent(consumer);
  }

  /** {@link DwcTerm#sampleSizeUnit} interpretation. */
  public static void interpretSampleSizeUnit(ExtendedRecord er, Consumer<String> consumer) {
    extractOptValue(er, DwcTerm.sampleSizeUnit).map(String::trim).ifPresent(consumer);
  }

  /** {@link DcTerm#license} interpretation. */
  public static void interpretLicense(ExtendedRecord er, Consumer<String> consumer) {
    String license =
        extractOptValue(er, DcTerm.license)
            .map(CoreInterpreter::getLicense)
            .map(License::name)
            .orElse(License.UNSPECIFIED.name());

    consumer.accept(license);
  }

  /** {@link DwcTerm#datasetID} interpretation. */
  public static void interpretDatasetID(ExtendedRecord er, Consumer<List<String>> consumer) {
    extractOptListValue(er, DwcTerm.datasetID).ifPresent(consumer);
  }

  /** {@link DwcTerm#datasetName} interpretation. */
  public static void interpretDatasetName(ExtendedRecord er, Consumer<List<String>> consumer) {
    extractOptListValue(er, DwcTerm.datasetName).ifPresent(consumer);
  }

  /** {@link DwcTerm#parentEventID} interpretation. */
  public static void interpretParentEventID(ExtendedRecord er, Consumer<String> consumer) {
    extractNullAwareOptValue(er, DwcTerm.parentEventID).ifPresent(consumer);
  }

  /** Interprets the hierarchy of {@link DwcTerm#parentEventID}. */
  public static void interpretParentEventIDHierarchy(
      ExtendedRecord er,
      Map<String, ExtendedRecord> erWithParents,
      Consumer<List<String>> consumer) {
    String parentEventID = extractValue(er, DwcTerm.parentEventID);

    if (parentEventID == null) {
      return;
    }

    // parent event IDs
    List<String> parentEventIds = new ArrayList<>();
    while (parentEventID != null) {
      parentEventIds.add(parentEventID);
      ExtendedRecord parent = erWithParents.get(parentEventID);
      parentEventID = parent != null ? extractValue(parent, DwcTerm.parentEventID) : null;
    }

    if (!parentEventIds.isEmpty()) {
      consumer.accept(parentEventIds);
    }
  }

  /** {@link DwcTerm#samplingProtocol} interpretation. */
  public static void interpretSamplingProtocol(ExtendedRecord er, Consumer<List<String>> consumer) {
    extractOptListValue(er, DwcTerm.samplingProtocol).ifPresent(consumer);
  }

  /** Returns ENUM instead of url string */
  private static License getLicense(String url) {
    URI uri =
        Optional.ofNullable(url)
            .map(
                x -> {
                  try {
                    return URI.create(x);
                  } catch (IllegalArgumentException ex) {
                    return null;
                  }
                })
            .orElse(null);
    License license = LicenseParser.getInstance().parseUriThenTitle(uri, null);
    // UNSPECIFIED must be mapped to null
    return License.UNSPECIFIED == license ? null : license;
  }
}
