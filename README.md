# Lexicon Scan Engine (v2)

Apache Spark Dataproc job that scans communication messages and attachments against
pre-compiled Intel Hyperscan lexicon pattern databases, applies a noise-reduction
decision tree, and writes nested hit-summary output to BigQuery.

This is a ground-up revision of the engine to match the exact BQ table schemas,
nested output structures, and testing requirements captured in the latest
requirements documents, screenshots, and `LexiconScanEngine_Tables.xlsx`.

---

## 1. What changed from the previous version

| Area | Before | Now |
|---|---|---|
| `is_noise_reduction` | `BOOLEAN` | `STRING` ("Y"/"N") on both source tables |
| View extraction | FM-side only filtered lexicon/Composite | **Both** LFD and FM sides filtered symmetrically; composite sub-features filtered to `type='lexicon'` on the LFD side too |
| Output tables | 1 flat hit table + 2 audit tables | **3** output tables (2 of them **nested/repeated**) + 2 audit tables |
| `lexicon-hit-summary` | one row per (message, feature) | **one row per message**, with `evaluated_lexicons[]` REPEATED, each with `term_dtls[]` REPEATED |
| `lexicon-hit-restricted` | did not exist | **new table** — unredacted match text for `message_type='restricted'` messages only |
| `feature-hit-summary` | did not exist | **new table** — flat Yes/No hit_status per evaluated feature/sub-feature |
| Term identity | Hyperscan expression id only | **manifest-based** `expressionId -> termId/pattern` resolution (see §4) |
| Table names | CLI flags | **JSON config file on GCS** (`--configGcsPath`) |
| Message reading | AVRO only | AVRO (prod) **or** JSON fixtures (test), behind a `MessageReader` interface |
| Testing | Spark-only unit tests | Unit tests **+** cross-platform integration tests (Windows/macOS/Linux/CI), zero cloud dependencies |

---

## 2. Module structure

```
lexicon-scan-engine/
├── pom.xml                         Spring Boot 2.7.18 · Java 11 · Spark 3.3.1 · Hyperscan 5.4.0
├── .github/workflows/ci.yml        Matrix CI: ubuntu-latest / macos-latest / windows-latest
└── src/
    ├── main/java/com/db/macs3/ecomms/spectre/
    │   ├── LexiconScanEngineApplication.java   main() entry point
    │   ├── config/AppConfig.java               SparkSession + MessageReader profile wiring
    │   ├── model/                              14 model classes (see §3, §5)
    │   ├── reader/
    │   │   ├── JobConfigReader.java             loads JobConfig JSON from GCS
    │   │   ├── BigQueryViewReader.java          creates + reads the join view
    │   │   ├── GcsHyperscanDatabaseLoader.java  loads .hdb bytes + manifests
    │   │   ├── MessageReader.java               common interface
    │   │   ├── AvroMessageReader.java            production (GCS/AVRO)
    │   │   └── JsonMessageReader.java            testing (local/classpath JSON)
    │   ├── engine/
    │   │   ├── LexiconScanEngine.java           orchestrator
    │   │   ├── LexiconScanPartitionFunction.java core per-message scan logic
    │   │   ├── NoiseReductionEvaluator.java     OR/AND decision matrix
    │   │   └── HyperscanMatcher.java            executor-side scan + static DB cache
    │   └── writer/BigQueryOutputWriter.java     writes all 5 tables
    └── test/
        ├── resources/
        │   ├── application-test.yml             test Spark config + BQ table names
        │   └── fixtures/
        │       ├── messages/scenario1.json      4-message test scenario
        │       └── config/test-job-config.json  JobConfig fixture
        └── java/.../
            ├── model/                            ModelLogicTest, ScanEngineArgsTest
            ├── engine/                           NoiseReductionEvaluatorTest, HyperscanMatcherTest
            ├── reader/                           BigQueryViewReaderTest, GcsHyperscanDatabaseLoaderTest,
            │                                     JsonMessageReaderTest, JobConfigReaderTest
            └── integration/
                ├── HdbTestFixtures.java           compiles real Hyperscan DBs in-memory
                ├── MockFeatureDecisionData.java   simulates BQ view output, no live BQ
                └── LexiconScanEngineIntegrationTest.java   full pipeline, local Spark
```

---

## 3. BigQuery view — exact extraction logic

Both source tables are filtered and flattened **symmetrically**:

```sql
-- language-feature-decision side
lfd_direct_lexicon:    UNNEST(features) WHERE type='lexicon'
lfd_composite_lexicon: UNNEST(features), UNNEST(features.sub_feature)
                       WHERE features.type='composite' AND sub_feature.type='lexicon'
-- Parent id/name/operator/is_noise_reduction retained on every emitted row,
-- even when the row came from a composite's sub-feature.

-- feature-master side (mirrors the same pattern)
fm_direct_lexicon:    WHERE feature_type='lexicon'          -> feature_definition
fm_composite_lexicon: UNNEST(sub_feature) WHERE type='lexicon' -> sub_feature.definition

-- Join
lfd.process_id = fm.policy_engine_id AND lfd.lexicon_name = fm.lexicon_name
```

See `BigQueryViewReader.buildViewSql()` for the full generated SQL, and
`BigQueryViewReaderTest` for a test asserting every clause is present.

Non-lexicon feature types (`metadata`, `evaluation`, etc.) — and composites with
**no** lexicon sub-features at all — are dropped from the view entirely. They are
evaluated by other services in the platform, not by this engine.

---

## 4. The manifest dependency (new requirement)

Hyperscan's `Expression` class only accepts an **integer** id — there is no way to
attach a human-readable string like `"lexicon_market_cond_2::1"` to a compiled
pattern inside the `.hdb` file itself. But the output schema requires reporting
exactly that string, plus the original PCRE pattern, for every hit.

**Design decision:** the Lexicon Compile Service must publish a small
`<featureName>.manifest.json` file alongside each `.hdb` file on GCS:

```json
[
  { "expressionId": 0, "termId": "lexicon_market_cond_2::1",  "pattern": "(?:...)" },
  { "expressionId": 1, "termId": "lexicon_market_cond_2::3",  "pattern": "(?:...)" }
]
```

`GcsHyperscanDatabaseLoader.loadManifests()` loads and broadcasts these alongside
the `.hdb` bytes. `HyperscanMatcher` resolves `expressionId -> termId/pattern` at
match time, and manifest size directly gives `evaluated_lexicons.total_terms_count`
— no need to parse Hyperscan's native binary format.

If a manifest is missing or out of sync, `HyperscanMatcher` falls back to a
synthetic `termId` of `featureName + "::" + expressionId` rather than failing the
scan outright (see `HyperscanMatcherTest.missingManifestEntry_fallsBackToSyntheticTermId`).

> **This requires a corresponding small change to the Lexicon Compile Service's**
> **`/compile/bundle` endpoint** to emit this manifest file. That service was not
> in scope for this task, but the Scan Engine cannot resolve term identity without it.

---

## 5. Output tables

### `lexicon-hit-summary` — nested, one row per message
`evaluated_lexicons[]` contains **only lexicons with at least one hit** — a lexicon
feature that was evaluated but matched nothing is omitted entirely (that exhaustive
Yes/No coverage is `feature-hit-summary`'s job, not this table's).

### `lexicon-hit-restricted` — nested, restricted messages only
When `message_type='restricted'` **and** there was a hit, `lexicon-hit-summary`'s
`matched_text` is replaced with the literal string `"REFER LEXICON HIT RESTRICTED
TABLE"`, and the real match JSON goes here instead. Unrestricted messages never get
a row in this table — see `LexiconScanPartitionFunction.processMessage()` and the
`restrictedMessageMasking` integration test.

### `feature-hit-summary` — flat, exhaustive Yes/No coverage
One row per (message, feature) or (message, composite, lexicon sub-feature) that
**this engine evaluated**. Per explicit scope decision: metadata-type features
appearing in the shared table are written by other services, not this one.

### `pipeline_stage_audit` / `pipeline_record_audit` — shared audit tables
Written by multiple Dataproc jobs across the platform; this engine contributes rows
with `stage_name = "spectre-lexicon-tagging"`, leaving columns owned by other stages
null.

---

## 6. Testing strategy

### Unit tests (`mvn test`, Surefire)
Pure logic and mocked-dependency tests — `NoiseReductionEvaluatorTest` (full decision
matrix + composite sub-feature grouping), `HyperscanMatcherTest` (real Hyperscan
compilation, manifest resolution, static cache), `BigQueryViewReaderTest` (SQL
structure, mocked `BigQuery` client), `GcsHyperscanDatabaseLoaderTest` /
`JobConfigReaderTest` (mocked `Storage` client), `JsonMessageReaderTest` (real file
I/O against `@TempDir`), `ModelLogicTest`, `ScanEngineArgsTest`.

### Integration tests (`mvn verify`, Failsafe)
`LexiconScanEngineIntegrationTest` runs the **full scan pipeline** — grouping, join,
`mapPartitions`, decision tree, redaction — end to end, with every external
dependency replaced by a local equivalent:

- **BigQuery view** → `MockFeatureDecisionData` constructs the exact
  `Dataset<FeatureDecisionRow>` shape the real view would produce
- **Messages** → `JsonMessageReader` reads `fixtures/messages/scenario1.json`
- **Hyperscan `.hdb` files** → `HdbTestFixtures` compiles tiny databases **using the
  real Hyperscan native library** at test run time (not static binary fixtures,
  since `.hdb` format is CPU-architecture-specific and wouldn't be portable across
  Windows/macOS/Linux CI runners)

This runs identically on Windows, macOS, Linux, and GitHub Actions (see
`.github/workflows/ci.yml`'s 3-OS matrix) with zero GCP credentials, no BigQuery
emulator, and no AVRO tooling required anywhere in the test path.

### `application-test.yml`
Test-only Spark config (`local[2]`) and BQ table names, per the requirement that
test Spark/table configuration live in a properties file rather than being
hardcoded in test source.

---

## 7. Deployment

```bash
mvn clean package -DskipTests
gsutil cp target/lexicon-scan-engine-2.0.0-SNAPSHOT.jar gs://bucket/jars/

gcloud dataproc jobs submit spark \
  --cluster=spectre-cluster --region=us-central1 \
  --jar=gs://bucket/jars/lexicon-scan-engine-2.0.0-SNAPSHOT.jar \
  --properties='spark.executor.memory=8g,spark.driver.memory=4g' \
  -- \
  --processId        "1234-5678-9810-1234" \
  --pipelineExecId   "1234-5678-9810-1230" \
  --policyEngineId   "1" \
  --triggerType      "policy-alert-live" \
  --runDate          "20260713" \
  --configGcsPath    "gs://spectre-config-bucket/scan-engine/prod.json" \
  --compsrDagName    "spectre-lexicon-tagging" \
  --compsrDagPath    "gs://spectre-dags/spectre-lexicon-tagging" \
  --dprocScriptName  "lexicon-scan-engine-2.0.0.jar" \
  --dprocScriptPath  "gs://bucket/jars/lexicon-scan-engine-2.0.0.jar"
```

Table/view names, GCS buckets/prefixes, and Spark tuning overrides all live in the
JSON file at `--configGcsPath` — see `JobConfig` Javadoc for the full structure.

---

## 8. Known open items

- **Lexicon Compile Service change required**: the `.manifest.json` sidecar file
  described in §4 does not exist yet in the compile service's `/compile/bundle`
  output — this is a prerequisite for `total_terms_count` and `term_id`/
  `term_regex_pattern` to resolve correctly at scan time.
- **`pipeline_record_audit`** currently derives `status=SUCCESS` unconditionally
  per processed message; per-message failure detection (e.g. malformed AVRO record)
  is a natural follow-up once real production message volume surfaces failure modes
  to design against.
