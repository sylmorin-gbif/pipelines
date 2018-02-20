package org.gbif.pipelines.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.interpretation.column.InterpretationFactory;
import org.gbif.pipelines.interpretation.column.InterpretationResult;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

public interface LocationInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {


    /**
     * {@link DwcTerm#basisOfRecord} interpretation.
     */
    static LocationInterpreter interpretCountry(Location locationRecord) {
        return (ExtendedRecord extendedRecord) -> {
            InterpretationResult<String> result = InterpretationFactory.interpret(DwcTerm.country, extendedRecord.getCoreTerms().get(DwcTerm.country.qualifiedName()));
            Interpretation<ExtendedRecord> finalResult = Interpretation.of(extendedRecord);

            locationRecord.setCountry(result.getResult().orElse(null));
            finalResult.withValidation(DwcTerm.country.name(), result.getIssueList()).withLineage(DwcTerm.country.name(), result.getLineageList());

            return finalResult;
        };
    }

    /**
     * {@link DwcTerm#basisOfRecord} interpretation.
     */
    static LocationInterpreter interpretCountryCode(Location locationRecord) {
        return (ExtendedRecord extendedRecord) -> {
            InterpretationResult<String> result = InterpretationFactory.interpret(DwcTerm.countryCode, extendedRecord.getCoreTerms().get(DwcTerm.countryCode.qualifiedName()));
            Interpretation<ExtendedRecord> finalResult = Interpretation.of(extendedRecord);
            locationRecord.setCountryCode(result.getResult().orElse(null));
            finalResult.withValidation(DwcTerm.countryCode.name(), result.getIssueList()).withLineage(DwcTerm.countryCode.name(), result.getLineageList());

            return finalResult;
        };
    }

    /**
     * {@link DwcTerm#basisOfRecord} interpretation.
     */
    static LocationInterpreter interpretContinent(Location locationRecord) {
        return (ExtendedRecord extendedRecord) -> {
            InterpretationResult<String> result = InterpretationFactory.interpret(DwcTerm.continent, extendedRecord.getCoreTerms().get(DwcTerm.continent.qualifiedName()));
            Interpretation<ExtendedRecord> finalResult = Interpretation.of(extendedRecord);

            locationRecord.setContinent(result.getResult().orElse(null));
            finalResult.withValidation(DwcTerm.continent.name(), result.getIssueList()).withLineage(DwcTerm.continent.name(), result.getLineageList());

            return finalResult;
        };
    }
}
