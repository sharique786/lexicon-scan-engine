# Lexicon Scan Engine

A Dataproc/Spark batch job that scans eComms messages (email, chat, voice) and their attachments against
Hyperscan-compiled lexicon databases, applies the NoiseReduction → Disclaimer → Lexicon decision tree, and writes
hit details and audit records to BigQuery.

**Stack:** JDK 21, Spring Boot 4.0.6 (driver only), Apache Spark 4.1.2 (Scala 2.13), Hyperscan 5.4.0
(`com.gliwka.hyperscan:hyperscan` 5.4.0-2.0.0), Google Cloud Dataproc, GCS (AVRO messages, lexicon zip bundles) and
BigQuery (decision view, output tables). Entry point: `com.db.macs3.ecomms.spectre.LexiconScanEngineApplication`.

It is one of three services in the platform, and the furthest downstream:

```
Lexicon Compile Service  → compiles each lexicon into one <feature>.zip (.hdb + compile-results JSON)
Lexicon Scanner Service  → interactive, analyst-facing "test this rule" tool
Lexicon Scan Engine      → THIS PROJECT: high-volume production batch scan
```

This project consumes the Compile Service's output artifacts, not its API — see [Hyperscan zip bundles](#5-hyperscan-zip-bundles).

---

## Contents

1. [End-to-end flow](#1-end-to-end-flow)
2. [Input data](#2-input-data)
3. [Message filtering](#3-message-filtering)
4. [Message types and what gets scanned](#4-message-types-and-what-gets-scanned)
5. [Hyperscan zip bundles](#5-hyperscan-zip-bundles)
6. [Clean text: HTML stripping and normalisation](#6-clean-text-html-stripping-and-normalisation)
7. [Feature application](#7-feature-application)
8. [Decision tree](#8-decision-tree)
9. [How matches are calculated](#9-how-matches-are-calculated)
10. [Proximity operators](#10-proximity-operators)
11. [Output tables](#11-output-tables)
12. [Failure handling](#12-failure-handling)
13. [Configuration reference](#13-configuration-reference)
14. [Performance and scalability](#14-performance-and-scalability)
15. [Suggested Spark configuration](#15-suggested-spark-configuration)
16. [Build, test, deploy](#16-build-test-deploy)
17. [Package map](#17-package-map)
18. [Known limitations](#18-known-limitations)

---

## 1. End-to-end flow

```
BigQuery view vw_src_msg_lexicon_decision_mapping        AVRO messages on GCS
  (one row per message × feature)                          (restricted/ and unrestricted/ subfolders)
        │ filtered by process / feature partition /               │ restricted to the view's message_ids
        │ dataset partition / policy engine                       │ (broadcast join)
        └───────────────────────────┬────────────────────────────┘
                                    │  inner join on message_id
                                    ▼
                       mapPartitions(PartitionProcessor)
        one HyperscanBundleLoader + one FeatureScanOrchestrator (one native Scanner) per partition
                                    │
        per message:  FeatureGroupingService.groupAndOrder()   → NoiseReduction → Disclaimer → Lexicon groups
                      DecisionTreeEvaluator.evaluate()         → per feature: FeatureScanOrchestrator.scanRow()
                      OutputRowBuilder.build*()                → summary / detail / feature-hit rows
                                    │
                                    ▼
                       MessageProcessingResult  (success rows, or a per-message error)
                                    │
      ┌──────────────┬──────────────┼───────────────┬─────────────────────┐
      ▼              ▼              ▼               ▼                     ▼
lexicon-hit-   lexicon-hit-   lexicon-hit-    feature-hit-      pipeline_record_audit
summary        restricted     unrestricted    summary           (every record)
               (+ CSV mirror)
```

`ScanEngineJobRunner.runPipeline()` runs these stages, in order, on the driver:

1. **Parse arguments and config** — `RuntimeArgs` from the 7 `--key=value` arguments, `DataprocConfig` from the YAML at `--config_file_path`.
2. **Write the `IN_PROGRESS` row** to `pipeline_stage_audit`.
3. **Resolve the Hyperscan base path** — one GCS listing call for the whole run ([§5](#5-hyperscan-zip-bundles)).
4. **Read the BigQuery view** in one filtered query covering every `dataset_details` entry; cache it.
5. **Resolve distinct features** — collect the distinct `feature_definition.body.lexiconName` values (bounded by feature count, never message count) to the driver, build each feature's zip path by string concatenation, and **broadcast** that one `feature → zip path` map.
6. **Read AVRO messages** of every `dataset_details` entry, restricted to the view's `message_id` set, and union them.
7. **Aggregate the view by `message_id`** (one row per message with a `features` array), inner-join it to the messages, and add `pipeline_exec_id`, `created_by` and the output-facing dataset partition value.
8. **`mapPartitions(PartitionProcessor)`** — the only place Hyperscan bundles are loaded and messages are scanned.
9. **Split the results and write** each output table, the restricted CSV mirror and `pipeline_record_audit`; then write the `SUCCESS`/`FAILED` row to `pipeline_stage_audit`.

**Driver load:** the driver only holds the small feature → path map and the distinct feature-name list. Every message-scale dataset (the joined data, the per-message results, every output table) stays a Spark `Dataset` from creation to write; the driver never calls `.collect()` on them.

**Spring stops at the driver.** `ScanEngineJobRunner`, `SparkSessionConfig` and `GcsClient` (on the driver) are Spring-managed. Everything that runs inside `mapPartitions` (`PartitionProcessor`, `HyperscanBundleLoader`, `FeatureScanOrchestrator`, the decision classes, `OutputRowBuilder`) is plain Java, constructible with no `ApplicationContext`, because that is the environment on an executor.

---

## 2. Input data

### 2.1 Job arguments (`RuntimeArgs`)

Composer submits 7 `--key=value` arguments:

```
--process_id=913b68f9-0f62-4f51-a9c1-c9aa0d84c01c
--pipeline_exec_id=2026-09-03_4-101
--trigger_type=policy-alert-test
--policy_engine_id=101
--dataset_details=[{"dataset_id":"006e3f06-045d-4f94-a9bd-780e603ef81f","dataset_partition_value":"2026-06-18"}]
--feature_partition_value=2026-07-16
--config_file_path=gs://<bucket>/tmp/orchestrator_v2/.../dataproc-config-<hash>.yml
```

| Argument | Used for |
|---|---|
| `process_id`, `policy_engine_id`, `feature_partition_value` | Filters on the BigQuery view; `policy_engine_id` also selects the Hyperscan compile folder |
| `dataset_details` | Inline JSON array; one entry per dataset. Each gives the GCS folder to read (`dataset_id`) and the `dataset_partition_value` written to the output tables |
| `pipeline_exec_id`, `trigger_type` | Copied to output and audit rows |
| `config_file_path` | GCS path of the `DataprocConfig` YAML |

`trigger_type` is `policy-alert-live` (always exactly one `dataset_details` entry) or `policy-alert-test` (may have several). A missing, blank or malformed argument fails the job at start.

### 2.2 Dataproc config YAML (`DataprocConfig`)

Only the `spectre.engine.*` subtree is read:

```yaml
spectre:
  engine:
    hyperscan:
      hdb-gcs-bucket: <bucket holding lexicon zip bundles>
      hdb-gcs-prefix: policy_test
    messages:
      msg-gcs-bucket: <bucket holding AVRO messages>
      msg-gcs-prefix: coreapp-trans
    bigquery:
      bq-project / bq-dataset / bq-view-name          # the decision view (three separate fields)
      bq-output-hit-summary, bq-output-hit-restricted, bq-output-hit-unrestricted,
      bq-output-feature-hit-summary, bq-output-stage-audit, bq-output-record-audit   # fully-qualified table ids
```

Unknown properties (`project_id`, `spring:`, ...) are ignored.

### 2.3 Decision view (`FeatureDecisionViewReader`)

`vw_src_msg_lexicon_decision_mapping` holds one row per (message, feature to apply). It is read through the Spark BigQuery connector (`viewsEnabled=true`, materialised into the configured project/dataset) with one pushed-down filter:

```
process_id = '<process_id>' AND feature_partition_value = '<feature_partition_value>'
  AND dataset_partition IN ('<partition 1>', '<partition 2>', ...) AND policy_engine_id = <policy_engine_id>
```

| Column | Type | Meaning |
|---|---|---|
| `process_id`, `policy_engine_id` | STRING | Run identity |
| `message_id` | STRING | Join key to the AVRO message |
| `dataset_partition`, `feature_partition_value` | DATE | Partitions |
| `feature_tagging_type` | STRING | e.g. `Lexicon-Tagging`; copied to `feature-hit-summary.feature_hit_type` |
| `feature_type` | STRING | `lexicon`, `composite`, `disclaimer`, `NoiseReduction` |
| `feature_id`, `feature_name` | LONG, STRING | Groups rows into one (possibly composite) feature |
| `sub_feature_type`, `lexicon_features_to_apply_name` | STRING | The member lexicon of this row |
| `is_noise_reduction` | BOOLEAN | NoiseReduction flag |
| `operator` | STRING | `AND`/`OR` combining sibling rows of one `feature_id` |
| `feature_definition` | JSON string | Parsed by `FeatureDefinition` — see below |

`feature_definition`:

```json
{ "featureId": "2", "featureName": "lexicon_market_cond_4", "featureType": "lexicon", "isNoiseReduction": "N",
  "body": { "id": 1, "lexiconName": "lexicon_market_cond_4", "objectId": "2",
            "scope": ["Message Body", "Attachment", "Subject"], "totalTermsCount": 17, "minimumHits": 2 } }
```

`body.lexiconName` names the zip bundle to load **and** prefixes every `term_id` (`<lexiconName>::<n>`), verbatim, hyphens included. `body.scope` selects the areas to scan (§7). `body.totalTermsCount` fills `total_terms_count` in `lexicon-hit-summary`. `minimumHits` is parsed but not used for the decision.

After reading, the view is grouped by `message_id` into one row per message whose `features` array holds every original column.

### 2.4 AVRO messages (`MessageAvroReader`)

For each `dataset_details` entry the reader lists and reads:

```
gs://<msg-gcs-bucket>/<msg-gcs-prefix>/<dataset_id>/restricted/*.avro
gs://<msg-gcs-bucket>/<msg-gcs-prefix>/<dataset_id>/unrestricted/*.avro
```

Each row is tagged with `restricted` (which subfolder it came from) and `dataset_partition_value` (from `dataset_details`); neither is in the AVRO. A dataset with `.avro` files in neither subfolder fails the job (`NoAvroFilesFoundException`); a missing subfolder is fine. Only `*.avro` files are read (the `restricted/` folder also holds this job's own CSV mirror).

Fields converted into a `ScanMessage` (`MessageRowConverter`):

| AVRO path | Used for |
|---|---|
| `message_id` | Join key, `record_id` in audit |
| `source.channel_name` | Message type — EMAIL / CHAT / VOICE ([§4](#4-message-types-and-what-gets-scanned)) |
| `source.source_name` | `pipeline_record_audit.source_name` |
| `message.content.subject` | Scanned when a feature's scope includes Subject |
| `message.content.raw_text` | The message body — scanned when scope includes Message Body |
| `attachments[].content.clean_text`, `attachments[].metadata.attachment_id` | Scanned when scope includes Attachment |
| `message.metadata.start_time_utc` | `pipeline_record_audit.sent_date` (an unparseable value becomes NULL, not a failure) |
| `processing.run_date` | `pipeline_record_audit.run_date` (ISO `yyyy-MM-dd`) |

Read but **not scanned**: `message.content.header`, `message.content.clean_text`, `src_sys_*`, attachment file names.

---

## 3. Message filtering

A message goes through several independent filters before and during scanning:

| # | Where | Filter |
|---|---|---|
| 1 | BigQuery read | Only view rows for this `process_id`, `feature_partition_value`, `dataset_partition` set and `policy_engine_id` |
| 2 | AVRO read | Only messages whose `message_id` appears in the view (broadcast join on the view's distinct ids); no full-bucket scan |
| 3 | Join | Inner join on `message_id` — an AVRO message with no view row is dropped; a view row with no AVRO message produces no output at all |
| 4 | Per message | Blank `message_id`, blank output dataset partition, or a null/blank `process_id`, `pipeline_exec_id`, `created_by` fail **that message** only (recorded in `pipeline_record_audit`) |
| 5 | Per feature (scope) | A feature scans only the areas listed in `body.scope`. A missing or empty scope scans nothing |
| 6 | Per area | A blank subject or body, or a blank attachment `clean_text`, is not scanned |
| 7 | Attachments | If `SPECTRE_MAX_ATTACHMENT_SIZE_BYTES` is set, an attachment whose UTF-8 `clean_text` is larger is skipped entirely (not scanned, not an error) |
| 8 | CHAT/VOICE body | Only `message_text` cells of the HTML table are scanned ([§4](#4-message-types-and-what-gets-scanned)) |
| 9 | Decision tree | A hit NoiseReduction group stops evaluation: no Disclaimer or Lexicon group is scanned ([§8](#8-decision-tree)) |
| 10 | Disclaimer suppression | A Lexicon match fully inside a disclaimer match in the same area is dropped from the detail tables ([§8](#8-decision-tree)) |

---

## 4. Message types and what gets scanned

**Message type** comes from `source.channel_name`, matched case-insensitively against `EMAIL`, `CHAT`, `VOICE`. Anything else — null, blank, unrecognised — is treated as **EMAIL**, so a message with no channel information is still scanned. The type only changes how the message *body* is prepared; subject and attachments are handled the same for every type.

| Area | Source field | Prepared how |
|---|---|---|
| `SUBJECT` | `message.content.subject` | `HtmlStrippingService.strip` |
| `MESSAGE_BODY` — EMAIL | `message.content.raw_text` | `HtmlStrippingService.strip` on the whole body |
| `MESSAGE_BODY` — CHAT, VOICE | `message.content.raw_text`, an HTML table report | `ChatVoiceMessageTextExtractor` keeps only `<td>` cells whose `class` contains the token `message_text` (e.g. `class="message-text-cell message_text"`), joins them with a space, then strips them |
| `ATTACHMENT` (per attachment) | `attachments[].content.clean_text` | Not stripped (`HtmlStrippingService.identity`) — upstream already delivers markup-free text |

Why chat/voice needs extraction: every other column of the table (room name, event type, email address, attachment metadata) is not message content, and scanning it could match a lexicon term that was never part of the conversation. If a CHAT/VOICE `raw_text` has no `message_text` cell at all (not this table shape), it falls back to stripping the whole text like EMAIL, rather than dropping the message. The `<td>` match is a regex (`<td ...>...</td>`, non-nested), not a full HTML parser.

**Restricted vs. unrestricted** is not a message type. It is the AVRO subfolder a message came from, and only decides which detail table (`lexicon-hit-restricted` or `-unrestricted`) receives its hits.

Every position reported for any area is relative to the **original** text of that area (for chat/voice, the original `raw_text`, not the extracted cells).

---

## 5. Hyperscan zip bundles

### 5.1 Where the bundles are

The Compile Service writes one zip per lexicon feature:

```
gs://<hdb-gcs-bucket>/<hdb-gcs-prefix>/<YYYY-MM-DD_HH-MM-SS>_<policy_engine_id>/output/lex-hyperscan/<feature>.zip
     ├── <feature>.hdb    the compiled Hyperscan database
     └── <feature>.json   compile-results: per-term metadata
```

`HyperscanPathResolver.resolveBasePath` lists the `<hdb-gcs-prefix>/` folder **once per job run** and keeps the folders ending in `_<policy_engine_id>`; if several match, the lexicographically greatest wins (the timestamp format sorts chronologically, so this is the most recent compile). No match is a fatal error. `buildZipPath(basePath, feature)` then appends `<feature>.zip` by string concatenation — no further GCS listing however many features a run references. Only these small path strings (never file bytes) are broadcast to executors.

### 5.2 Loading (`HyperscanBundleLoader`)

One loader per Spark partition (constructed inside the `mapPartitions` function, not per message):

- `load(feature)` downloads the zip once, reads it sequentially with `ZipInputStream`, matches entries by **base filename** (any directory prefix inside the zip is ignored) against `<feature>.hdb` and `<feature>.json`, then loads the `Database` (`Database.load`) and parses the JSON (`TermExpressionMetadata.parse`). Both are cached together as one `LexiconBundle`.
- The cache is a bounded, **not thread-safe** LRU (`scan-engine.max-cached-databases-per-partition`, default 20). A failed load is never cached, so it is retried for the next message.
- A missing entry, a corrupt zip, or unparseable metadata throws `HyperscanFileLoadException` naming the feature, path and (for a missing entry) the entries actually found. **One malformed term fails the load of the whole feature**, so every message using that feature is recorded as a failure.
- `prefetch(features)` warms the cache for a fixed lookahead of distinct features at partition start, concurrently, using JDK 21 virtual threads (§14). It is best-effort; a feature that fails to prefetch simply loads normally later, with its real error.

### 5.3 The metadata JSON (`TermExpressionMetadata`)

The `.hdb` alone cannot say which term a matched expression id belongs to, or evaluate AND NOT and proximity conditions, so the JSON is required. Only `PASS` terms are indexed. A term's shape decides how it is evaluated:

| Shape | JSON signature | How it is evaluated |
|---|---|---|
| **Simple term** | No `resolvedPatterns`. `hyperscanExpressionId` = the term's own number (the `::<n>` suffix of `termId`) | A match on that id **is** the term match. A decomposition without negation stays a native Hyperscan `COMBINATION` under the same id |
| **AND NOT id-list term** | No `resolvedPatterns`. `requiresExclusionCheck: true`; `requiredExpressionIds` / `excludedExpressionIds` hold one *allocated* id per pattern, none equal to the term number | Required ids all present **and not** all excluded ids present, judged over the matches of **every** scanned area |
| **`resolvedPatterns` term** | `resolvedPatterns` non-blank: the operator structure as text (a single regex, a `NEAR{n}`/`FOLLOWEDBY{n}` chain, a plain `AND`, or an `AND NOT`), plus `regexPattern` leaves (and `exclusionRegex` leaves for AND NOT) and, when decomposed, `patternMapping` | Parsed into a `ResolvedPatternTree` and verified in Java **per scanned area** ([§10](#10-proximity-operators)) |
| **Inline-compiled proximity term** | One `regexPattern` entry that differs from `resolvedPatterns`, `resolvedPatterns` still contains `NEAR{n}`/`FOLLOWEDBY{n}`, no `patternMapping`, `hyperscanExpressionId` present | The proximity is baked into one regex, e.g. `\bkeep\b(?:\s+\S+){0,3}\s+\bmouth shut\b`. Hyperscan verified everything, so no tree is built; resolved by id like a simple term |

Why AND NOT is not a native combination: Hyperscan evaluates a `COMBINATION` eagerly and progressively, not once at the end of the scan. A formula mixing a required and a negated pattern (`R&!E`) can fire as soon as `R` matches, before `E` has even been reached, giving a false positive. So the Compile Service compiles each AND NOT pattern as its own plain expression and this engine evaluates the boolean itself. A pure conjunction has no negation, so it cannot fire early and safely stays native.

Why proximity leaves cannot be verified by Hyperscan: decomposed `NEAR`/`FOLLOWEDBY` leaves are compiled `QUIET`, so Hyperscan never reports an individual leaf. Only the wrapping `COMBINATION` fires, and it proves only "every leaf matched somewhere in this scan buffer" — never order or distance. That is why `resolvedPatterns` and `patternMapping` exist, and why Java verifies the real condition.

Other JSON details:

- `regexPattern` is the current name of the leaf list; the older `translatedPattern` is still accepted.
- For an AND NOT `resolvedPatterns` term, the excluded side's leaves arrive separately in `exclusionRegex`; `patternMapping` may also be present there (a readable id formula) and is validated against the term's own ids.
- `termDescription` (the analyst-authored term text) is carried to `lexicon-hit-summary.term_dtls.term_description`.
- Indexing is by expression id **and** by term number; a term that needs per-area evaluation but has no id at all is found through `mandatoryPerAreaTerms()`.
- Inconsistent metadata (a duplicate term number, an id claimed by two terms, `requiresExclusionCheck` disagreeing with the `resolvedPatterns` shape, a `patternMapping` whose id count disagrees with the leaf count) throws `TermMetadataParseException` instead of silently mis-evaluating.

---

## 6. Clean text: HTML stripping and normalisation

Hyperscan never scans the raw text. Each area is turned into **clean text** by `HtmlStrippingService.strip`, exactly once per message (never once per feature):

1. Every contiguous **run** made of HTML tags (`<...>`), whitespace (space, tab, newline, carriage return, form feed, ...) and commas is replaced by **one** space. Commas covered: ASCII `,`, fullwidth `，` (U+FF0C) and Arabic `،` (U+060C).
2. Everything else is copied unchanged. Case, other punctuation and non-ASCII text are untouched.

Examples:

| Original | Clean text |
|---|---|
| `<p>Enjoy</p>\n<p>Happy Birthday</p>` | ` Enjoy Happy Birthday ` |
| `keep,\n\tin the, market` | `keep in the market` |
| `one,, two` | `one two` |

This is what lets a term such as `Enjoy(?:\s+\S+){0,2}\s+Happy` match text where the words are separated by markup, line breaks or commas. The result may start or end with one space.

**Positions stay in original coordinates.** `strip` returns an `OffsetMap` with one entry per stripped-text boundary, mapping any clean-text position back to the original text. A reported match is therefore a *span of the original text*, not necessarily an exact substring equal to `matchedText`: when tags fall inside a match, the span between `startCharIndex` and `endCharIndex` contains them, while `matchedText` is the clean form. For `<p>Enjoy</p>\n<p>Happy Birthday</p>` matched by `Enjoy(?:\s+\S+){0,2}\s+Happy`: `matchedText = "Enjoy Happy"`, `original.substring(3, 21) = "Enjoy</p>\n<p>Happy"`.

**Variants:**

- `stripExtracted` (chat/voice): strips the extracted cells and *composes* the extraction's offset map with the stripping map, so a match resolves straight to the original `raw_text`.
- `identity` (attachments): returns the text unchanged with an O(1) identity map and no array — attachment `clean_text` is markup-free and can be megabytes long.

**Not done:** HTML entities (`&nbsp;`, `&amp;`) are not decoded; the contents of `<script>`/`<style>` are not removed (only the tags); non-breaking spaces (U+00A0) are not treated as whitespace. Attachments are not normalised (no comma or whitespace collapsing).

Consequence of comma normalisation: a lexicon term whose pattern contains a literal comma (e.g. `hello, world`) can no longer match, because Hyperscan scans comma-free text.

---

## 7. Feature application

A message can be evaluated against many features — dozens of view rows. For each message:

1. **Group** the view rows by `feature_id` into `FeatureGroup`s (`FeatureGroupingService`). A group is one standalone feature (one row) or a composite/NoiseReduction feature whose member rows are combined by an operator. Rows of one `feature_id` must agree on `feature_type`, `is_noise_reduction` and (for several rows) `operator`; a disagreement, or several rows with no operator, throws and fails that message. Group order follows first appearance of the `feature_id`, then is stably sorted into three phases:

   ```
   NoiseReduction (is_noise_reduction = true)  →  Disclaimer (feature_type = disclaimer)  →  everything else (lexicon, composite)
   ```

2. **Scan each member row** (`FeatureScanOrchestrator.scanRow`): parse the row's `feature_definition`, load the bundle for `body.lexiconName`, and scan the areas listed in `body.scope` (`Subject`, `Message Body`, `Attachment`, matched case-insensitively). The lexicon actually scanned is `body.lexiconName`, not the view's display `feature_name`. A term matches if it satisfies its own condition ([§9](#9-how-matches-are-calculated)).

3. **Member hit / group hit.** A member is a *hit* when at least one term matched anywhere in its scope; `minimumHits` is informational and not enforced. A single-member group's hit is its member's. For several members the group's operator applies across the members' hits: `OR` = any member hit, `AND` = every member hit (any other operator value throws).

A message therefore costs one `scanRow` per applicable feature member, but its text is stripped once and one `Scanner` is reused for all of them.

---

## 8. Decision tree

Implemented by `DecisionTreeEvaluator` over the ordered groups from `FeatureGroupingService`.

### 8.1 NoiseReduction — short-circuit

Groups are evaluated in order. The moment one is a **hit**, evaluation stops for the whole message: later NoiseReduction groups, the Disclaimer group and every Lexicon group are **never scanned** (not scanned-and-discarded). `MessageEvaluationResult.isShortCircuited()` is true and no lexicon matches are produced. The hit group's own matches are still reported — in `lexicon-hit-summary` and, with `matched_text`, in the detail table. A NoiseReduction group that was evaluated but is not a hit (an `AND` group where only some members matched) appears in the summary tables but not in the detail table.

### 8.2 Disclaimer — scanned like any lexicon

If nothing short-circuited, Disclaimer groups are scanned through the same path and bundle format as any lexicon. Their matches are used twice:

- as **suppression spans** for Lexicon matches (§8.3), and
- as **reported hits**: a Disclaimer group that is a hit gets its own entry in the detail table (`lexicon-hit-restricted`/`-unrestricted`), so a message whose only hit is a disclaimer still has a detail row.

Disclaimer matches are never themselves suppressed. Suppression spans come from every disclaimer match found, including those of a Disclaimer group that ended up not being a hit (an `AND` group with only some members matching); such a group is not reported.

### 8.3 Lexicon — scanned, then suppressed by disclaimers

Every remaining group is scanned. A Lexicon match is **suppressed** when it is **fully contained** in a disclaimer match's span **in the same coordinate space**: same area, and for attachments the same `attachment_id`. Partial overlap does not suppress. A term left with no surviving matches is removed; a group left with no surviving term is absent from the detail table. The number of suppressed matches is kept on the result for observability only.

### 8.4 What each output gets

| Table | NoiseReduction | Disclaimer | Lexicon |
|---|---|---|---|
| `lexicon-hit-summary` | every *evaluated* group, raw matches (a no-hit group gets an `N/A` placeholder entry) | same | same, **before** suppression |
| `lexicon-hit-restricted` / `-unrestricted` | hit groups only | hit groups only | **after** suppression |
| `feature-hit-summary` | every evaluated group with its hit status | same | same |

Worked example — body `"... confidential information ... bomb threat"`; the disclaimer matches `confidential information` (chars 10–34), the lexicon matches `information` (15–27, inside the disclaimer) and `bomb` (50–54):

- summary: disclaimer entry (1 term) and lexicon entry (2 terms, raw);
- detail: disclaimer entry (`confidential information`) and lexicon entry with only `bomb` (`information` is suppressed);
- if the lexicon matched only `information`, the detail row would carry the disclaimer entry alone.

---

## 9. How matches are calculated

For each (feature member, message):

1. **Text.** Each in-scope, non-blank area is turned into clean text ([§6](#6-clean-text-html-stripping-and-normalisation)).
2. **Hyperscan scan.** A single native `Scanner` (one per partition) scans each area's clean text against the feature's `Database`. `scanner.allocScratch(database)` runs before each scan. Hyperscan reports matches by expression id; positions are Java character indices, and the end index is inclusive (`HyperscanScanService` converts it to exclusive). Every match is then translated to original-text coordinates through the offset map. The result is a list of `RawExpressionMatch` (one per distinct expression id, with every occurrence found) **per area**.
3. **Merge and resolve.** `FeatureScanOrchestrator.resolveAndEvaluate` merges raw matches across all scanned areas by expression id, then finds every distinct term any matched id belongs to (`TermExpressionMetadata.termByAnyExpressionId`, plus the mandatory per-area terms). Each term is evaluated by one of two paths, chosen per term:
   - **Id presence, merged across areas** (no `resolvedPatternTree`: simple, inline-compiled proximity, id-list AND NOT). Required side satisfied iff **every** id of `requiredExpressionIds` is present in the merged set; excluded side satisfied iff **every** id of `excludedExpressionIds` is also present; the term matches iff required is satisfied and excluded is not. For an AND NOT term the required pattern and the excluded pattern may sit in different areas (the required word in the subject, the excluded word in the body), which is why this merges across areas. Reported occurrences are those of the required-side ids only — the excluded side is a condition, never a hit.
   - **Per-area Java evaluation** (`resolvedPatternTree` present). The term's `hyperscanExpressionId`, when present, is only a cheap pre-filter, applied globally and per area: if it never fired in an area, the area is skipped. Otherwise `ResolvedPatternAreaEvaluator` checks the real condition against **that one area's original text** ([§10](#10-proximity-operators)). Areas are **never merged** for this path, because word distance and order are meaningless across two different texts.
4. **Term result.** A satisfied term becomes a `TermMatchResult`: `term_id = <lexiconName>::<termNumber>` (always built from the term's own number, never from a raw expression id), `term_regex_pattern` (the verbatim `resolvedPatterns`, else the regex(es) joined with ` & `), `term_description`, and every occurrence as an `AreaMatch` (area, attachment id, span). A term with no match produces no result.

**Counts and hits:**

| Value | Meaning |
|---|---|
| `regex_match_hit_count` | Number of `AreaMatch` occurrences of a term, summed over every area and attachment (5 occurrences → 5, not 1) |
| `regex_hit_count` (per group) | Number of *distinct terms* that matched in that group (a no-hit group: 0) |
| member hit | At least one term matched |
| group hit | Member hit, or `OR`/`AND` across members |
| `total_terms_count` | Sum of `body.totalTermsCount` over the group's members |

**Spans** (`MatchSpan`): `startCharIndex` inclusive, `endCharIndex` exclusive, both in the original text of that area; `matchedText` is the clean-text form of what matched (for a Java-verified proximity term, the original substring). In `matched_text` JSON, `start` is `startCharIndex` and `length` is `endCharIndex - startCharIndex`.

---

## 10. Proximity operators

`NEAR{n}` and `FOLLOWEDBY{n}` are handled in one of two ways, depending on how the Compile Service compiled the term ([§5.3](#53-the-metadata-json-termexpressionmetadata)).

### 10.1 Inline-compiled terms (handled by Hyperscan)

When the whole proximity fits in one Hyperscan regex, the Compile Service bakes it in, e.g. `keep FOLLOWEDBY{3} mouth shut` becomes

```
\bkeep\b(?:\s+\S+){0,3}\s+\bmouth shut\b
```

and `Off shore NEAR{2} account` becomes an alternation of both orders. Hyperscan reports the match directly and this engine resolves the term by id. Distance is counted in whitespace-separated tokens (`(?:\s+\S+){0,n}`).

### 10.2 Decomposed terms (verified in Java)

When the term is too large for one regex, it is split into leaves compiled `QUIET`, with a `resolvedPatterns` string such as `manipulate NEAR{5} (?:price|spread|stock)` and a `patternMapping` such as `(7&8)`. The pipeline:

1. **Parse the shape, not the text.** `ResolvedPatternTree.build` scans `resolvedPatterns` with paren-depth awareness to discover only its *shape* — leaves per chain, operator and distance sequence, the `AND`/`AND NOT` split — and zips that shape **positionally** against the structured `regexPattern` list. It deliberately never slices leaf regex text out of the string (a leaf could itself contain the text ` NEAR{5} `). A leaf-count disagreement is a parse error, not a silent mismatch. Supported tree nodes:
   - **Chain**: leaves joined by `NEAR{n}`/`FOLLOWEDBY{n}`; an element may itself be a parenthesised chain (nesting to any depth), e.g. `(?:manipulate|front run) NEAR{5} ((?:price|spread) NEAR{5} stock)`.
   - **And**: two sub-trees that must both be satisfied in the same area (`chain AND leaf`).
   - **AndNot**: `required AND NOT (excluded)`.
2. **Coarse pre-filter.** The wrapping `COMBINATION` id (`hyperscanExpressionId`) fires only if every leaf is present somewhere in an area; areas where it did not fire are skipped.
3. **Per-area evaluation.** `ResolvedPatternAreaEvaluator` compiles each leaf as a `java.util.regex.Pattern` (`CASE_INSENSITIVE | UNICODE_CASE | UNICODE_CHARACTER_CLASS`), finds every occurrence with `Matcher.find()` in the area's **original** text, maps each occurrence to word indices, and searches by backtracking for every leaf-occurrence combination that satisfies the chain.

### 10.3 Semantics

| Operator | Meaning |
|---|---|
| `NEAR{n}` | The two occurrences are in **either** order, with at most `n` whole words strictly between them |
| `FOLLOWEDBY{n}` | The second occurrence comes strictly after the first, with at most `n` whole words strictly between them |

- **Gap** = whole words strictly between the two occurrences, measured from the last word of the earlier occurrence to the first word of the later one. A multi-word leaf (`"mouth shut"`, `"Off shore"`) never counts its own internal words as gap; overlapping occurrences never satisfy a chain. Example: `keep in the market mouth shut` has a gap of 3, so `FOLLOWEDBY{3}` matches and `FOLLOWEDBY{2}` does not.
- **`NEAR` is symmetric**: `A NEAR{n} B` and `B NEAR{n} A` give identical results.
- **Word** = a maximal run of non-whitespace characters, so Latin, Cyrillic, Hebrew and Arabic text is counted per whitespace-separated token. **Chinese, Japanese and Korean** have no spaces between words, so every Han, Hiragana, Katakana and Hangul code point is its own one-character word (supplementary-plane characters count once); `n` then counts characters. Latin words between CJK terms still count as one word each.
- **Chains** with more than two leaves check every consecutive pair with its own operator and distance. **Nested chains** are evaluated recursively: each satisfying occurrence of the inner group becomes one candidate occurrence for the outer chain.
- **`AND`** (plain): both sides must be satisfied within the same area; occurrences from both sides are reported.
- **`AND NOT`**: within one area, if the excluded tree is satisfied the whole term fails there; otherwise the required tree is evaluated. Evaluated **per area** (unlike the id-list AND NOT of §5.3, which merges areas).
- **Reported occurrences**: every satisfying combination is reported, each as one span from the earliest to the latest chosen leaf, deduplicated. Capped at **50 per area**, with a separate larger backtracking bound (200 000 visits); a pathological input degrades by truncation (logged at DEBUG) rather than failing the message.

---

## 11. Output tables

All tables are written in append mode through the Spark BigQuery connector; this job never updates rows in place. Column names follow the delivered BigQuery schemas.

| Table | Grain | Contents |
|---|---|---|
| `lexicon-hit-summary` | One row per message | `evaluated_lexicons[]`: one entry per **evaluated group** (`id`, `name`, `total_terms_count`, `regex_hit_count`, `term_dtls[]`), for NoiseReduction, Disclaimer and Lexicon groups alike, using raw (pre-suppression) counts. `term_dtls[]`: `term_id`, `term_regex_pattern`, `regex_match_hit_count`, `term_description`. A group evaluated with no matching term is still listed, with `regex_hit_count = 0` and one placeholder term (`term_id`/`term_regex_pattern`/`term_description = "N/A"`, count 0); groups never evaluated (after a NoiseReduction hit) have no entry |
| `lexicon-hit-restricted` | One row per message *that has something to report*, restricted source only | `evaluated_lexicons[]`: `id` and `term_dtls[]` of `term_id` + `matched_text`. Contains hit NoiseReduction groups, hit Disclaimer groups and Lexicon groups after suppression. **No row** is written when nothing remains |
| `lexicon-hit-unrestricted` | Same, unrestricted source only | Identical shape, split purely by AVRO subfolder |
| `feature-hit-summary` | One row per message | `features[]`: `id`, `name`, `type`, `is_noise_reduction`, `hit_status` for every evaluated group, plus `sub_features[]` (`type`, `name`, `hit_status`) for multi-member groups; `feature_hit_type` from the view's `feature_tagging_type` |
| `pipeline_record_audit` | One row per message, **success and failure** | `status` `SUCCESS`/`FAILED`, `return_code` 0/1, `error_message`, `record_id` = `message_id`, `source_name`, `sent_date`, `run_date`. De-duplicated on `record_id`/`stage_name`/`execution_date`/`pipeline_exec_id`. Other columns belong to stages this job does not run and are left null/0 |
| `pipeline_stage_audit` | Two rows per run | `IN_PROGRESS` at start, then `SUCCESS` or `FAILED` (with `error_message`) |

Each per-message table also carries `message_id`, `process_id`, `pipeline_exec_id`, `dataset_partition_value`, `created_by`, `created_ts`.

**`matched_text`** (a JSON string in a JSON column) has this shape, one per term:

```json
{"hit_details_hs":[{
  "message_id": "...",
  "msg_text":        [{"text": "bomb", "start": 50, "length": 4}],
  "subject":         [{"text": "bomb", "start": 10, "length": 4}],
  "attachment_text": [{"attachment_id": "...", "att_text": [{"text": "bomb", "start": 3, "length": 4}]}]
}]}
```

**Restricted CSV mirror.** The restricted detail rows are also written as one CSV file (`coalesce(1)`, header, overwrite) to `gs://<msg-gcs-bucket>/<msg-gcs-prefix>/<first dataset_id>/restricted/csv`; `evaluated_lexicons` is flattened to a JSON string, since CSV cannot hold nested values.

**Write methods** (`OutputTableWriter`): the four message-scale tables use the connector's indirect write (staged as Avro on GCS, then one BigQuery load job; `intermediateFormat=avro`, `useAvroLogicalTypes=true`); the two small audit tables use `writeMethod=direct` (Storage Write API). A write failure fails the job and is recorded in `pipeline_stage_audit`. Array-valued fields are converted to `scala.collection.Seq` at every nesting depth, as `Encoders.row` requires.

---

## 12. Failure handling

| Failure | Effect |
|---|---|
| A message fails inside `processOneRow` (blank ids, inconsistent feature group, bad `feature_definition`, bundle load error, evaluation error) | Isolated: a `FAILED` `pipeline_record_audit` row with the error, no rows in the other tables, and the job continues |
| A feature's bundle cannot be loaded | Every message that needs that feature fails individually as above (the failed load is not cached and is retried) |
| Prefetch fails for a feature | Ignored; the feature loads synchronously when first needed |
| Anything outside the per-message try/catch (loader construction, unexpected partition-level error) | Fails the partition, therefore the Spark job |
| An unreadable `start_time_utc` | `sent_date` is NULL; the message still succeeds |
| Argument/config error, no Hyperscan compile folder, no AVRO files, BigQuery write failure | Fails the job; `pipeline_stage_audit` gets a `FAILED` row where possible (argument/config errors happen before any audit row can be written) |

---

## 13. Configuration reference

| Source | Carries |
|---|---|
| `RuntimeArgs` (7 `--key=value` arguments) | See [§2.1](#21-job-arguments-runtimeargs) |
| `DataprocConfig` (YAML on GCS, from `--config_file_path`) | Hyperscan zip bucket/prefix, message bucket/prefix, BigQuery view and output-table identifiers |
| `application.yml` (`scan-engine.*`) | `max-cached-databases-per-partition` (default 20), `created-by` (`SPECTRE-COMPOSER-SA`), `stage-name` (`spectre-lexicon-tagging`) — overridable at submit time as driver system properties |
| Env `SPECTRE_MAX_ATTACHMENT_SIZE_BYTES` | Skip attachments whose UTF-8 `clean_text` is larger; unset = no limit |
| `log4j2.properties` | Driver **and** executor logging (root `WARN`, this project's package `INFO`); Spring's `logging.level.*` would only reach the driver |

---

## 14. Performance and scalability

Sized for millions of messages, thousands of lexicon bundles, and a **shared** Dataproc cluster.

- **One `Scanner` per partition.** `FeatureScanOrchestrator` owns exactly one native `Scanner` for its lifetime (one instance = one Spark partition = one task thread) and calls `allocScratch(database)` before each scan. Constructing one per scan would waste native resources the wrapper caps at 256 per JVM.
- **Strip once per message, not per feature.** A message is scanned against dozens of features; its subject, body and attachments are cleaned once and reused. Attachments use the zero-cost `identity` path.
- **One zip download per feature per partition**, cached in a bounded LRU (native database and metadata together). The same feature may still be downloaded once per partition that needs it — accepted; broadcasting every bundle from the driver was rejected because it risks driver OOM.
- **Concurrent bundle prefetch.** `PartitionProcessor` peeks at most `PREFETCH_LOOKAHEAD_ROWS` (200) rows to discover the features the start of the partition needs, then `HyperscanBundleLoader.prefetch` loads them concurrently on virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`, stable in JDK 21, no `--enable-preview`), capped at the cache size. Only I/O-bound work (GCS download, unzip, parse) is concurrent; the cache itself is only touched by one thread (concurrent phase first, then a sequential insertion loop). The rest of the partition streams row by row — the partition is never materialised.
- **Scanning is never parallelised internally.** It is CPU-bound native (JNI) work; virtual threads would pin their carrier and add only overhead, and would take CPU beyond what YARN allocated on a shared cluster. Scanning stays as parallel as Spark's one-task-per-core scheduling.
- **Two independent skew sources:** attachment/body size, and applicable-feature count per message. `SparkSessionConfig` sets AQE and skew-join thresholds explicitly (see §15) rather than trusting shared-cluster defaults.
- **Broadcast join** on the view's distinct `message_id`s restricts the AVRO read; the view is cached because it is used twice.

---

## 15. Suggested Spark configuration

This job has not run against a live cluster; treat the numbers as reasoned starting points to monitor and adjust. Settings marked **(code-enforced)** are set by `SparkSessionConfig` and apply regardless of cluster defaults.

**Executors:** favour moderate cores, since each concurrent task holds its own bundle cache, Scanner scratch space and GCS connections.

```
spark.executor.cores=4
spark.executor.memory=14g
spark.executor.memoryOverhead=6g          # Hyperscan databases and scratch are off-heap (JNI): size overhead generously
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=<baseline for the audit-write tail>
spark.dynamicAllocation.maxExecutors=<peak for expected daily volume>
spark.dynamicAllocation.shuffleTracking.enabled=true
spark.speculation=false                   # tasks do external work and write; a duplicate attempt is unverified
```

**Partitioning and skew (code-enforced)**

```
spark.serializer=org.apache.spark.serializer.KryoSerializer         (set on the SparkSession builder — static config)
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionFactor=3                 (Spark default 5)
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=128m    (Spark default 256m)
spark.sql.adaptive.advisoryPartitionSizeInBytes=64m
spark.sql.shuffle.partitions=max(200, 3 × defaultParallelism)       computed at job start
spark.sql.files.maxPartitionBytes=67108864                          (64 MB; Spark default 128 MB)
```

Aim for `mapPartitions` partitions of a few thousand messages: large enough that a partition's bundle cache is reused and prefetch pays off, small enough that the message batch plus up to `max-cached-databases-per-partition` bundles fits in memory.

**BigQuery connector:** the message-scale tables use the indirect write and need a staging bucket (`spark.datasource.bigquery.temporaryGcsBucket=<staging-bucket>`). The connector's Spark 4.1 module was in *preview* at the last upgrade; confirm its GA status and version before relying on it for production volumes.

**GCS connector:** every bundle cache miss opens one read stream; `spark.hadoop.fs.gs.http.max.retry=10` is a safety margin for throttling at job start.

**GC (optional):** generational ZGC (`spark.executor.extraJavaOptions=-XX:+UseZGC -XX:+ZGenerational`, a production feature in JDK 21) is worth an A/B test against G1 if pause times matter for the occasional very large message; it affects only this job's executors.

**Spark 4.x notes:** Scala 2.13 only (every Spark artifact uses `_2.13`); JDK 17+ required; ANSI SQL mode is the default, so check the view SQL for implicit coercions; the Surefire JVM needs the broader `--add-opens` set Spark documents for JDK 17+ (see `pom.xml`).

---

## 16. Build, test, deploy

```bash
mvn clean test         # JUnit 5 tests against real Hyperscan, real Jackson, local-mode Spark; JaCoCo report
mvn clean package      # shaded jar target/lexicon-scan-engine-2.0.0.jar
```

The tests compile real Hyperscan databases through `Database.compile`/`Database.save` (genuine `.hdb` bytes), zip them as bundles in-test, and run the real scan and evaluation path, including native `COMBINATION`/`QUIET` behaviour. Tests use the `test` Spring profile (`application-test.yml`).

```bash
gcloud dataproc jobs submit spark \
  --cluster=<cluster-name> --region=<region> \
  --jar=gs://<bucket>/lexicon-scan-engine-2.0.0.jar \
  --class=com.db.macs3.ecomms.spectre.LexiconScanEngineApplication \
  --properties="<see Suggested Spark configuration>" \
  -- \
  --process_id=913b68f9-0f62-4f51-a9c1-c9aa0d84c01c \
  --pipeline_exec_id=2026-09-03_4-101 \
  --trigger_type=policy-alert-test \
  --policy_engine_id=101 \
  --dataset_details=[{"dataset_id":"006e3f06-045d-4f94-a9bd-780e603ef81f","dataset_partition_value":"2026-06-18"}] \
  --feature_partition_value=2026-07-16 \
  --config_file_path=gs://<bucket>/tmp/orchestrator_v2/.../dataproc-config-<hash>.yml
```

---

## 17. Package map

| Package | Role |
|---|---|
| `spark/` | `ScanEngineJobRunner` (driver pipeline), `SparkSessionConfig`, `PartitionProcessor` (executor `mapPartitions`), per-table row mappers, `MessageProcessingResult` |
| `avro/`, `bq/` | AVRO message reader/converter; view reader/converter; `OutputTableWriter` |
| `config/` | `RuntimeArgs`, `DataprocConfig`, `BqTableConfig`, `ScanEngineProperties` |
| `gcs/` | `GcsClient`, `HyperscanPathResolver` |
| `hyperscan/` | `HyperscanBundleLoader`, `HyperscanScanService`, `TermIdBuilder` |
| `model/termmeta/` | `TermExpressionMetadata`, `ResolvedPatternTree` — the compile-results contract |
| `html/` | `HtmlStrippingService` (clean text + offset map), `ChatVoiceMessageTextExtractor` |
| `decision/` | `FeatureGroupingService`, `DecisionTreeEvaluator`, `FeatureScanOrchestrator`, `ResolvedPatternAreaEvaluator` |
| `output/` | `OutputRowBuilder` — builds the summary, detail and feature-hit rows |
| `model/` | Messages, view rows, feature definition, match results, decision results, output rows |

---

## 18. Known limitations

- **Nothing has run against a live cluster.** Spark's distributed behaviour, the BigQuery connector's read/write path, GCS connectivity, the Hyperscan native library on Dataproc, and every numeric tuning value in §14–15 are unverified beyond local tests. `PartitionProcessor`, `ScanEngineJobRunner`, `FeatureDecisionViewReader` and `ViewRowConverter` have no dedicated tests; the AVRO reader/converter, the row mappers, `OutputTableWriter` and the audit path do (including a local-mode Spark test).
- **Hyperscan `\b` and UCP.** Hyperscan's `\b` is ASCII-only and `UCP` mode rejects `\b`, so a term wrapped in `\b…\b` whose word is Han, Hiragana, Katakana, Hangul, Hebrew or Arabic never matches in Hyperscan. This is a Compile Service concern (drop `\b` for non-ASCII words).
- **Inline proximity counts whitespace tokens.** Inline-compiled `NEAR`/`FOLLOWEDBY` terms count `(?:\s+\S+)` tokens, so CJK character-level distance applies only to decomposed terms verified in Java.
- **Java verification uses original text, Hyperscan uses clean text.** A decomposed multi-word leaf that is separated by a newline or comma in the original text can pass the Hyperscan pre-filter and still fail in `ResolvedPatternAreaEvaluator`, whose leaf regexes see the un-normalised text.
- **Literal commas in patterns cannot match** (§6). Attachments and HTML entities are not normalised.
- **A view row with no AVRO message produces no output** (no failure row); a message present in both `restricted/` and `unrestricted/` would be scanned twice.
- **Mis-encoded compile-results** (e.g. `�` in place of `ü`) cannot match the intended words; they must be fixed at the Compile Service.
- **Scripts and styles in HTML** are not removed, only their tags.
- **`term_regex_pattern`** for a term with many leaves is joined with `" & "` without a length cap.
- **BigQuery connector for Spark 4.1** is in preview upstream.
- **Not built:** an integration-test harness against real GCP services, and a Dockerfile.
