package org.gbif.pipelines.tasks.validators.validator;

import static org.gbif.api.model.pipelines.StepType.VALIDATOR_VALIDATE_ARCHIVE;
import static org.gbif.api.model.pipelines.StepType.VALIDATOR_VERBATIM_TO_INTERPRETED;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.ValidationWsClientStub;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Validation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class ArchiveValidatorCallbackIT {

  private static final String LABEL = VALIDATOR_VALIDATE_ARCHIVE.getLabel();
  private static final String DATASET_OCCURRENCR_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DATASET_SAMPLING_UUID = "9997fa4e-54c1-43ea-9856-afa90204c162";
  private static final String DATASET_CLB_UUID = "2247944e-3776-40a9-b9c4-abecf7eea177";
  private static final String INPUT_DATASET_FOLDER = "/dataset/dwca";
  private static final long EXECUTION_ID = 1L;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryClient historyClient;

  @BeforeClass
  public static void setUp() throws Exception {

    server = new TestingServer();
    curator =
        CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .namespace("crawler")
            .retryPolicy(new RetryOneTime(1))
            .build();
    curator.start();

    publisher = MessagePublisherStub.create();

    historyClient = Mockito.mock(PipelinesHistoryClient.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    curator.close();
    server.stop();
    publisher.close();
  }

  @After
  public void after() {
    publisher.close();
  }

  @Test
  public void testOccurrenceCase() throws Exception {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.fromString(DATASET_OCCURRENCR_UUID);
    int attempt = 2;
    String crawlId = DATASET_OCCURRENCR_UUID;

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            new HashSet<>(
                Arrays.asList(
                    VALIDATOR_VALIDATE_ARCHIVE.name(), VALIDATOR_VERBATIM_TO_INTERPRETED.name())),
            EXECUTION_ID,
            FileFormat.DWCA.name());

    // When
    callback.handleMessage(message);

    // Should
    // ZK
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Result
    Validation validation = validationClient.getValidation();

    assertEquals(2, validation.getMetrics().getFileInfos().size());

    // Meta
    Optional<FileInfo> metaOpt = validationClient.getFileInfoByFileType(DwcFileType.METADATA);
    assertTrue(metaOpt.isPresent());

    FileInfo meta = metaOpt.get();
    assertEquals("eml.xml", meta.getFileName());
    assertNull(meta.getCount());
    assertNull(meta.getIndexedCount());
    assertEquals(0, meta.getTerms().size());
    assertEquals(3, meta.getIssues().size());
    assertEquals(DwcFileType.METADATA, meta.getFileType());

    Optional<IssueInfo> randomIssue =
        meta.getIssues().stream()
            .filter(x -> x.getIssueCategory() == EvaluationCategory.METADATA_CONTENT)
            .findAny();
    assertTrue(randomIssue.isPresent());
    assertNull(randomIssue.get().getCount());

    // Core
    Optional<FileInfo> coreOpt = validationClient.getFileInfo(DwcFileType.CORE, DwcTerm.Occurrence);
    assertTrue(coreOpt.isPresent());

    FileInfo core = coreOpt.get();
    assertEquals("occurrence.txt", core.getFileName());
    assertNull(core.getCount());
    assertNull(core.getIndexedCount());
    assertEquals(0, core.getTerms().size());
    assertEquals(0, core.getIssues().size());
    assertEquals(DwcFileType.CORE, core.getFileType());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testSamplingEventCase() throws Exception {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.fromString(DATASET_SAMPLING_UUID);
    int attempt = 3;
    String crawlId = DATASET_SAMPLING_UUID;

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            new HashSet<>(
                Arrays.asList(
                    VALIDATOR_VALIDATE_ARCHIVE.name(), VALIDATOR_VERBATIM_TO_INTERPRETED.name())),
            EXECUTION_ID,
            FileFormat.DWCA.name());

    // When
    callback.handleMessage(message);

    // Should

    // ZK
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Result
    Validation validation = validationClient.getValidation();

    assertEquals(2, validation.getMetrics().getFileInfos().size());

    // Meta
    Optional<FileInfo> metaOpt = validationClient.getFileInfoByFileType(DwcFileType.METADATA);
    assertTrue(metaOpt.isPresent());

    FileInfo meta = metaOpt.get();
    assertEquals("eml.xml", meta.getFileName());
    assertNull(meta.getCount());
    assertNull(meta.getIndexedCount());
    assertEquals(0, meta.getTerms().size());
    assertEquals(0, meta.getIssues().size());
    assertEquals(DwcFileType.METADATA, meta.getFileType());

    // Core
    Optional<FileInfo> coreOpt =
        validationClient.getFileInfo(DwcFileType.EXTENSION, DwcTerm.Occurrence);
    assertTrue(coreOpt.isPresent());

    FileInfo core = coreOpt.get();
    assertEquals("occurrence.txt", core.getFileName());
    assertNull(core.getCount());
    assertNull(core.getIndexedCount());
    assertEquals(0, core.getTerms().size());
    assertEquals(0, core.getIssues().size());
    assertEquals(DwcFileType.EXTENSION, core.getFileType());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testClbCase() throws Exception {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.fromString(DATASET_CLB_UUID);
    int attempt = 3;
    String crawlId = DATASET_CLB_UUID;

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            new HashSet<>(
                Arrays.asList(
                    VALIDATOR_VALIDATE_ARCHIVE.name(), VALIDATOR_VERBATIM_TO_INTERPRETED.name())),
            EXECUTION_ID,
            FileFormat.DWCA.name());

    // When
    callback.handleMessage(message);

    // Should

    // ZK
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Result
    Validation validation = validationClient.getValidation();

    assertEquals(2, validation.getMetrics().getFileInfos().size());

    // Meta
    Optional<FileInfo> metaOpt = validationClient.getFileInfoByFileType(DwcFileType.METADATA);
    assertTrue(metaOpt.isPresent());

    FileInfo meta = metaOpt.get();
    assertEquals("eml.xml", meta.getFileName());
    assertNull(meta.getCount());
    assertNull(meta.getIndexedCount());
    assertEquals(0, meta.getTerms().size());
    assertEquals(0, meta.getIssues().size());
    assertEquals(DwcFileType.METADATA, meta.getFileType());

    // Core
    Optional<FileInfo> coreOpt =
        validationClient.getFileInfo(DwcFileType.EXTENSION, DwcTerm.Occurrence);
    assertTrue(coreOpt.isPresent());

    FileInfo core = coreOpt.get();
    assertEquals("occurrence.txt", core.getFileName());
    assertNull(core.getCount());
    assertNull(core.getIndexedCount());
    assertEquals(0, core.getTerms().size());
    assertEquals(0, core.getIssues().size());
    assertEquals(DwcFileType.EXTENSION, core.getFileType());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testOccurrenceSingleStepCase() {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.fromString(DATASET_OCCURRENCR_UUID);
    int attempt = 2;
    String crawlId = DATASET_OCCURRENCR_UUID;

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            Collections.singleton(VALIDATOR_VALIDATE_ARCHIVE.name()),
            EXECUTION_ID,
            FileFormat.DWCA.name());

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, crawlId, LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());
  }

  @Test
  public void testFailedCase() throws Exception {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.randomUUID(); // Use wrong datasetKey
    int attempt = 2;
    String crawlId = uuid.toString();

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            Collections.singleton(VALIDATOR_VALIDATE_ARCHIVE.name()),
            EXECUTION_ID,
            FileFormat.DWCA.name());

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testFailedValidatorCase() {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.validatorOnly = true;

    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.randomUUID(); // Use wrong datasetKey
    int attempt = 2;
    String crawlId = uuid.toString();

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            Collections.singleton(VALIDATOR_VALIDATE_ARCHIVE.name()),
            EXECUTION_ID,
            FileFormat.DWCA.name());

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, crawlId, LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(publisher.getMessages().isEmpty());
  }

  @Test
  public void testFailedMissedFilesCase() {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.validatorOnly = true;

    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.fromString("b578802e-f1ca-4e5b-acf8-4d45306e6b48");
    int attempt = 1;
    String crawlId = uuid.toString();

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            Collections.singleton(VALIDATOR_VALIDATE_ARCHIVE.name()),
            EXECUTION_ID,
            FileFormat.DWCA.name());

    // When
    callback.handleMessage(message);

    // Should
    Validation validation = validationClient.getValidation();
    Optional<FileInfo> occurrenceFile =
        validation.getMetrics().getFileInfos().stream()
            .filter(x -> x.getRowType() != null)
            .filter(x -> x.getRowType().equals(DwcTerm.Occurrence.qualifiedName()))
            .findFirst();

    assertTrue(occurrenceFile.isPresent());
    assertFalse(occurrenceFile.get().getIssues().isEmpty());

    assertFalse(checkExists(curator, crawlId, LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(publisher.getMessages().isEmpty());
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }
}
