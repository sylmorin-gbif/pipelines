package org.gbif.pipelines.core.converters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;

import com.google.common.base.Strings;

public class MultimediaConverter {

  private MultimediaConverter() {}

  public static MultimediaRecord merge(MultimediaRecord mr, ImageRecord ir, AudubonRecord ar) {
    MultimediaRecord record = MultimediaRecord.newBuilder().setId(mr.getId()).build();

    boolean isMrEmpty = mr.getMultimediaItems() == null && mr.getIssues().getIssueList().isEmpty();
    boolean isIrEmpty = ir.getImageItems() == null && ir.getIssues().getIssueList().isEmpty();
    boolean isArEmpty = ar.getAudubonItems() == null && ar.getIssues().getIssueList().isEmpty();

    if (isMrEmpty && isIrEmpty && isArEmpty) {
      return record;
    }

    Set<String> issues = new HashSet<>();
    issues.addAll(mr.getIssues().getIssueList());
    issues.addAll(ir.getIssues().getIssueList());
    issues.addAll(ar.getIssues().getIssueList());

    Map<String, Multimedia> multimediaMap = new HashMap<>();
    putAllMultimediaRecord(multimediaMap, mr);
    putAllImageRecord(multimediaMap, ir);
    putAllAudubonRecord(multimediaMap, ar);

    if (!multimediaMap.isEmpty()) {
      record.setMultimediaItems(new ArrayList<>(multimediaMap.values()));
    }

    if (!issues.isEmpty()) {
      record.getIssues().getIssueList().addAll(issues);
    }

    return record;
  }

  private static void putAllMultimediaRecord(Map<String, Multimedia> map, MultimediaRecord mr) {
    Optional.ofNullable(mr.getMultimediaItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(c -> c.stream()
            .filter(m -> !Strings.isNullOrEmpty(m.getReferences()) || !Strings.isNullOrEmpty(m.getIdentifier()))
            .forEach(r -> {
              String key = Optional.ofNullable(r.getIdentifier()).orElse(r.getReferences());
              map.put(key, r);
            }));
  }

  private static void putAllImageRecord(Map<String, Multimedia> map, ImageRecord ir) {
    Optional.ofNullable(ir.getImageItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(c -> c.stream()
            .filter(m -> !Strings.isNullOrEmpty(m.getReferences()) || !Strings.isNullOrEmpty(m.getIdentifier()))
            .forEach(r -> {
              String key = Optional.ofNullable(r.getIdentifier()).orElse(r.getReferences());
              Multimedia multimedia = Multimedia.newBuilder()
                  .setAudience(r.getAudience())
                  .setContributor(r.getContributor())
                  .setCreated(r.getCreated())
                  .setCreator(r.getCreator())
                  .setDatasetId(r.getDatasetId())
                  .setDescription(r.getDescription())
                  .setFormat(r.getFormat())
                  .setIdentifier(r.getIdentifier())
                  .setLicense(r.getLicense())
                  .setPublisher(r.getPublisher())
                  .setReferences(r.getReferences())
                  .setRightsHolder(r.getRightsHolder())
                  .setTitle(r.getTitle())
                  .setType(MediaType.StillImage.name())
                  .build();
              map.putIfAbsent(key, multimedia);
            }));
  }

  private static void putAllAudubonRecord(Map<String, Multimedia> map, AudubonRecord ar) {
    Optional.ofNullable(ar.getAudubonItems())
        .filter(i -> !i.isEmpty())
        .ifPresent(c -> c.stream()
            .filter(m -> !Strings.isNullOrEmpty(m.getAccessUri()))
            .forEach(r -> {
              String key = r.getAccessUri();
              Multimedia multimedia = Multimedia.newBuilder()
                  .setCreated(r.getCreateDate())
                  .setCreator(r.getCreator())
                  .setDescription(r.getDescription())
                  .setFormat(r.getFormat())
                  .setIdentifier(r.getAccessUri())
                  .setLicense(r.getLicensingException())
                  .setRightsHolder(r.getRights())
                  .setSource(r.getSource())
                  .setTitle(r.getTitle())
                  .setType(r.getType())
                  .build();
              map.putIfAbsent(key, multimedia);
            }));
  }
}
