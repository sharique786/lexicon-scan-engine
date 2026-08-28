# Lexicon Scan Engine

A Dataproc/Spark batch job that scans eComms messages and attachments
against Hyperscan-compiled lexicon feature databases, applies the
NoiseReduction → Disclaimer → Lexicon decision tree, and writes hit and
audit results to BigQuery.

**Stack:** JDK 21.0.11, Spring Boot 4.0.6, Apache Spark 4.1.2 (Scala 2.13),
Hyperscan 5.4.0-2.0.0 (via `com.gliwka.hyperscan-java`), running on Google
Cloud Dataproc, reading AVRO messages from GCS and a BigQuery view, writing
to BigQuery.

---

## Contents

1. [What's implemented](#whats-implemented)
2. [Architecture & pipeline stages](#architecture--pipeline-stages)
3. [Decision tree](#decision-tree)
4. [Hyperscan file processing: AND NOT and decomposed terms](#hyperscan-file-processing-and-not-and-decomposed-terms)
5. [Input path formation](#input-path-formation)
6. [Output tables / BQ population](#output-tables--bq-population)
7. [Configuration](#configuration)
8. [Suggested Spark configuration](#suggested-spark-configuration)
9. [Build, test, deploy](#build-test-deploy)
10. [Known limitations / deferred work](#known-limitations--deferred-work)

---

## What's implemented

- **AVRO message ingestion** from GCS, restricted to the message-id set the
  BQ view actually references (never a full-bucket scan).
- **BigQuery view reading**, filtered by dataset partition / feature
  partition / process id, unioned across every `dataset_details` entry in
  one run, then grouped by `message_id`.
- **Hyperscan-native scanning** of message subject, body, and attachments
  (per feature's own configured scope), using per-partition cached
  databases — never reloaded per message.
- **Full NoiseReduction → Disclaimer → Lexicon decision tree**, including
  short-circuit-on-NoiseReduction-hit, disclaimer-suppression of lexicon
  matches, and multi-member `AND`/`OR` group operators.
- **AND NOT and decomposed ("Pattern is too large") lexicon term support**,
  resolved correctly against the Lexicon Compile Service's *current* id
  scheme — see [Hyperscan file processing](#hyperscan-file-processing-and-not-and-decomposed-terms)
  below for the full explanation and the bug this fixes.
- **NEAR/FOLLOWEDBY proximity operator support**, for terms the Compile
  Service decomposes into multiple QUIET Hyperscan leaves plus a
  `resolvedPatterns` description of the operator/distance structure — the
  real word-distance condition is verified in Java against the message text,
  since Hyperscan's own QUIET-leaf combination can only prove "all leaves
  present somewhere," never their order or distance. See
  [Hyperscan file processing](#hyperscan-file-processing-and-not-and-decomposed-terms).
- **HTML-aware match positioning** — tags and collapsed whitespace never
  shift a reported match's position relative to the original message text.
- **Attachment size limiting** — attachments over a configurable byte
  threshold are skipped entirely (not scanned, not an error).
- **Five BigQuery output tables** — `lexicon-hit-summary`,
  `lexicon-hit-restricted`, `lexicon-hit-unrestricted`, `feature-hit-summary`,
  plus `pipeline_stage_audit` / `pipeline_record_audit` — and a CSV mirror
  of the restricted detail table.
- **Per-message fault isolation** — one message's processing failure is
  recorded to `pipeline_record_audit` and does not fail the job.

---

## Architecture & pipeline stages

```
BQ view (vw_src_msg_lexicon_decision_mapping)     AVRO messages (GCS)
        │  filtered, grouped by message_id                │  filtered to relevant message_ids
        └──────────────────────┬───────────────────────────┘
                                │  join on message_id
                                ▼
                    mapPartitions(PartitionProcessor)
              (one HyperscanBundleLoader + one FeatureScanOrchestrator
               per partition — see "Memory-safety design" below)
                                │
                for each message: FeatureGroupingService.groupAndOrder()
                              → DecisionTreeEvaluator.evaluate()
                                  (per feature row: FeatureScanOrchestrator.scanRow())
                              → OutputRowBuilder.build*()
                                │
                                ▼
                    MessageProcessingResult (success or per-message error)
                                │
                ┌───────────────┼───────────────┬─────────────────┐
                ▼               ▼               ▼                 ▼
      lexicon-hit-summary   lexicon-hit-      feature-hit-   pipeline_record_audit
                             restricted/       summary        (failures only)
                             unrestricted
                             (+ CSV mirror)
```

### The 9 pipeline stages, in the order `ScanEngineJobRunner.runPipeline()` runs them

1. **Resolve the Hyperscan base path** — exactly ONE GCS listing call for
   the whole job run (`HyperscanPathResolver.resolveBasePath`), finding the
   compile folder's wildcard timestamp segment. Every feature's `.zip`
   bundle path is then built by plain string concatenation, no further GCS
   listing needed — see [Input path formation](#input-path-formation).
2. **Read + union the BQ view** across every `dataset_details` entry,
   filtered by dataset partition / feature partition / process id
   (`FeatureDecisionViewReader.readFiltered` + `unionAll`), cached since it
   is read twice (once for distinct features, once for the message join).
3. **Resolve every DISTINCT feature referenced** to its `.zip` bundle path,
   and broadcast that one small map from the driver. The distinct-feature
   list is bounded by feature count (typically tens to low hundreds), never
   by message count — safe to `collectAsList()` to the driver.
4. **Read + union AVRO messages** across every `dataset_details` entry,
   restricted to the view's own distinct `message_id` set — never a
   full-bucket AVRO scan.
5. **Aggregate the view by `message_id`**, join against messages, attach a
   few output-facing columns not present in either source
   (`pipeline_exec_id`, `created_by`, the output-facing dataset partition
   value).
6. **`mapPartitions(PartitionProcessor)`** — the only place Hyperscan
   databases and term metadata are loaded (from the SAME zip bundle, see
   `HyperscanBundleLoader`). For each message: group and order its feature
   rows, evaluate the decision tree (which calls into
   `FeatureScanOrchestrator` per feature), build the three per-message
   output rows.
7. **Split results into successes/failures**, write each output table via
   `OutputTableWriter`, write the restricted-detail CSV mirror.
8. **Write `pipeline_record_audit`** for any per-message failures (skipped
   entirely if there were none).
9. **Write the `SUCCESS`/`FAILED` `pipeline_stage_audit` row**, closing out
   the `IN_PROGRESS` row written at job start.

### Driver-load discipline

The only things the driver holds/collects at more-than-trivial size: the
small, string-only feature→path maps (broadcast, not held per-executor) and
the distinct-feature-name list used to build them. Every message-scale
dataset (the joined message+view `Dataset`, the per-message results, every
output table) stays a Spark `Dataset` from creation to write — the driver
never calls `.collect()` on any of them.

---

## Decision tree

Implemented across `FeatureGroupingService` (grouping/ordering) and
`DecisionTreeEvaluator` (evaluation), driven by `FeatureScanOrchestrator`
for the actual Hyperscan scanning per feature.

### 1. Grouping and ordering

View rows for one message are grouped by `feature_id` into `FeatureGroup`s
(a group may have one member, or several combined by an explicit `AND`/`OR`
operator), then ordered into exactly three phases, **in this order**:

```
NoiseReduction  →  Disclaimer  →  Lexicon
```

### 2. NoiseReduction: short-circuit, not skip

Each NoiseReduction group is evaluated in order. The moment ONE is a hit
(its `AND`/`OR` operator resolved across its members), evaluation **stops
entirely** for the whole message — every later group (further
NoiseReduction groups, the Disclaimer group, every Lexicon group) is never
evaluated at all, not evaluated-and-discarded. This is a genuine
short-circuit: `MessageEvaluationResult.shortCircuited()` reflects it, and
no Hyperscan scan runs for the skipped groups, saving real work for
messages a NoiseReduction rule has already ruled uninteresting.

### 3. Disclaimer: scanned like a standard lexicon feature

If no NoiseReduction group short-circuited, the Disclaimer group (if
present in this message's feature set) is scanned exactly like a standard
lexicon feature — through the same `FeatureScanOrchestrator.scanRow()`
path, same Hyperscan database mechanics. This is a genuinely different
mechanism from the Lexicon Scanner Service's own disclaimer handling (which
does exact-substring detection via `DisclaimerDetectionService`, entirely
outside Hyperscan) — here, disclaimer text is itself compiled into a
Hyperscan feature database like any other lexicon rule, and its matches are
recorded for the suppression step below.

### 4. Lexicon: scanned, then suppressed where it overlaps a disclaimer match

Every Lexicon-category group is scanned. A match is suppressed (excluded
from `hasMatches`/output) when it is **fully contained** within a
disclaimer match's span — same area (subject/body/attachment), and for an
attachment match, the *same* `attachmentId` too, never comparing spans
across different coordinate spaces. Partial overlap does **not** suppress a
match; only full containment does.

### AND NOT and decomposed terms fit into this transparently

A feature's decision (hit or not) is based on which of its terms
`FeatureScanOrchestrator.scanRow()` returns a `TermMatchResult` for — see
the next section for exactly how an AND NOT term's boolean condition, and a
decomposed term's leaf-matching, are resolved before that decision even
sees them. From `DecisionTreeEvaluator`'s point of view, every term looks
identical regardless of how complex its underlying Hyperscan structure is:
either it produced a `TermMatchResult` (it counted) or it didn't.

---

## Hyperscan file processing: AND NOT and decomposed terms

### The confirmed bug this fixes

Before this round of changes, `FeatureScanOrchestrator` resolved any
matched Hyperscan expression id directly to a `term_id` via
`TermIdBuilder.build(feature, expressionId)`, on the assumption that a PASS
term's reportable expression id was *always* its own term number — true
under the Lexicon Compile Service's *original* id scheme, where AND NOT
terms compiled to a single native `HS_FLAG_COMBINATION` expression (e.g.
`(R&!E)`) at the term's own number.

That scheme was confirmed BROKEN and fixed at the Compile Service (and
Lexicon Scanner Service) level: Hyperscan's own documentation states a
combination expression "will raise matches at every offset where one of
its sub-expressions matches and the logical value of the whole expression
is true" — evaluated **eagerly and progressively**, not once after the
whole scan completes. Hyperscan's changelog separately documents that only
*purely negative* combinations (ones that can be satisfied by nothing
having matched at all) are deferred to end-of-data; `R&!E` is not purely
negative (it also requires the positive `R`), so it does not qualify — the
moment `R` matched, if `E` had simply not been *reached yet* by the scan
(not confirmed absent), the combination could fire immediately and
incorrectly.

**The Compile Service's fix removed native COMBINATION for AND NOT terms
entirely.** Every required and excluded pattern of an AND NOT term now
compiles as its own plain, individually-reportable expression, using an
*allocated* id that is **not** the term's own number. This engine's old
`expressionId == termNumber` assumption broke as a direct, confirmed
consequence:

- `lexicon-hit-summary.evaluated_lexicons.term_dtls.term_id` would be
  populated with a wrong, meaningless value (a raw allocated id, not the
  term number) for any AND NOT term's required/excluded pattern match.
- There was no AND NOT boolean evaluation anywhere in this engine at all —
  a message containing *only* the excluded pattern, with the required
  pattern entirely absent, would still register as a feature hit. A real
  false-positive risk, not merely a labelling issue.
- `term_regex_pattern` for a purely-decomposed (non-AND-NOT) term showed
  the unreadable native COMBINATION formula string itself (e.g.
  `"(10&11&12)"`) rather than the term's actual pattern text, since that
  formula *is* the matched expression's own text for a combination match —
  a separate, independently-discovered display bug fixed by the same change.

### The fix: `TermExpressionMetadata`, extracted alongside the `.hdb`

The Compile Service's `/compile/bundle` endpoint writes one
`<feature>.zip` bundle per feature, containing both `<feature>.hdb` and
`<feature>-compile-results.json` as entries (see
[Input path formation](#input-path-formation) for how this project reads
that zip) — the JSON entry is the same `CompileResponse`/
`TermCompilationResult` shape `/compile` returns, including the
`requiredExpressionIds`/`excludedExpressionIds` fields an AND NOT term now
needs. This engine reads and indexes that JSON:

```
TermExpressionMetadata.parse(feature, json)
  → for every PASS term:
      - non-AND-NOT: hyperscanExpressionId becomes its one requiredExpressionIds entry
      - AND NOT: requiredExpressionIds / excludedExpressionIds read directly
        (verbatim, possibly absent, for a resolvedPatterns-shaped AND NOT term —
         see "A second schema change" below)
      - resolvedPatterns (if present) parsed + zipped into a resolvedPatternTree
  → indexed TWO ways: byExpressionId (ANY expression id, one OR many → TermEntry —
             what FeatureScanOrchestrator's by-id discovery loop uses) AND
             byTermNumber (EVERY term, including one with no expression id at all —
             see mandatoryPerAreaTerms())
    TermEntry(termNumber, termRegexPattern, requiresExclusionCheck,
              requiredExpressionIds, excludedExpressionIds, resolvedPatternTree)
```

`FeatureScanOrchestrator.resolveAndEvaluate()` then, for one message:

1. Scans every area the feature's scope covers (`HyperscanScanService.scan`,
   now returning *raw*, un-resolved `RawExpressionMatch`es per area, not a
   resolved `term_id` — resolution moved out of that class entirely, since
   it has no cross-area visibility).
2. **Merges raw matches across ALL scanned areas** by expression id first —
   required and excluded patterns of the *same* AND NOT term can
   legitimately match in different areas of the same message (e.g. the
   required word in the subject, the excluded word in the body), so
   evaluation cannot correctly happen per-area.
3. For every DISTINCT term any matched id belongs to (via
   `TermExpressionMetadata.termByAnyExpressionId`), evaluates:
   - **Required side satisfied** iff EVERY entry of `requiredExpressionIds`
     is present in the combined match-id set.
   - **Excluded side satisfied** (term therefore excluded) iff EVERY entry
     of `excludedExpressionIds` is also present — same AND convention on
     *both* sides, mirroring the Compile Service's and Scanner Service's
     own documented AND NOT contracts exactly, for consistency across all
     three services in this platform.
   - A term produces a `TermMatchResult` only when required is satisfied
     AND excluded is not.
4. `term_id` is built from the term's own number
   (`TermIdBuilder.build(feature, entry.termNumber())`) — always correct
   now, regardless of which raw expression id actually fired.
5. `term_regex_pattern` prefers the metadata's own pattern text over the raw
   match's `Expression` text, fixing the unreadable-combination-formula
   display bug — the verbatim `resolvedPatterns` string (preserving the
   NEAR/FOLLOWEDBY operator and distance) for a term that has one, else the
   Compile Service's `translatedPattern`/`regexPattern` leaves joined.

### What's unaffected, and why

Pure decomposition without AND NOT (`R1&R2&...&Rn`, no negation) is
unaffected by any of this and still uses native Hyperscan COMBINATION —
provably safe, since a positive sub-expression's truth value is only ever
true after it genuinely matches, never before; there is no "not yet
reached" ambiguity for a formula with no negation in it at all.

### A second schema change: NEAR/FOLLOWEDBY proximity, and QUIET leaves that Hyperscan itself can never report individually

The Compile Service changed its `compile-results.json` schema again,
specifically for terms using `NEAR{n}`/`FOLLOWEDBY{n}` proximity operators
(and, potentially, AND NOT terms too — see the open question below). A
complex term can now be split ("Pattern Too Large") into multiple decomposed
`regexPattern` leaves (renamed from `translatedPattern`), each compiled with
Hyperscan's `QUIET` flag — confirmed against a real compiled `.hdb` dump.
**QUIET means Hyperscan's own match callback NEVER reports an individual
leaf's matches** — only the wrapping native `COMBINATION` expression (the
term's `hyperscanExpressionId`) fires, and firing only proves "every leaf
matched somewhere in this one scan buffer." No order or word-distance
information for the leaves is ever recoverable from Hyperscan itself for
these terms — a real, confirmed constraint, not an assumption.

Two new JSON fields carry what Hyperscan can no longer prove on its own:

```
resolvedPatterns  — e.g. "manipulate NEAR{5} (?:price|spread|stock)" — the
                    leaves' regex text joined by the literal operator/distance
                    text, and, for an AND NOT term, an " AND NOT (...) " wrapper
patternMapping    — e.g. "(7&8)" — the ordered Hyperscan expression ids for
                    each regexPattern leaf, in the same order (the exact
                    formula also compiled as the native COMBINATION
                    expression's own pattern text in the .hdb)
```

**The fix — a hybrid, per-area evaluation, never merged across areas:**
`TermExpressionMetadata.parse()` parses a non-blank `resolvedPatterns`
string's SHAPE (leaf count, operator+distance sequence, AND NOT split point)
via `ResolvedPatternTree.build()` — deliberately WITHOUT slicing leaf regex
text out of the string itself (the naive approach the reference
`ResolvedPatternMatcher` class takes, and its own Javadoc admits is unsafe
if a leaf's regex text happens to contain the literal substring
`" NEAR{5} "`) — then zips that shape against the structured `regexPattern`
list positionally. `FeatureScanOrchestrator.resolveAndEvaluate()` then, for
any term with a parsed tree:

1. Uses the term's `hyperscanExpressionId` (when present — absent for AND
   NOT, which still can't safely use native COMBINATION, per the fix above)
   as a cheap, coarse pre-filter, both globally and per scanned area: if it
   never fired in an area, that area cannot possibly satisfy the condition,
   skip it entirely.
2. When the pre-filter passes (or unconditionally for AND NOT, which has no
   such pre-filter available), compiles each `regexPattern` leaf as a real
   `java.util.regex.Pattern` and runs a word-distance backtracking search
   (`ResolvedPatternAreaEvaluator`, adapted from the reference
   `ResolvedPatternMatcher`) directly against **that same area's own real
   original text** — the only way to genuinely verify order/distance, since
   Hyperscan cannot.

**This per-area evaluation must NEVER be merged across areas** — unlike the
existing cross-area AND-NOT-by-presence logic above (a required word in the
subject and an excluded word in the body legitimately share ONE boolean
check there), word-distance/order is only meaningful within one contiguous
text. A required leaf's occurrence in the subject and an excluded leaf's
occurrence in the body do not share a coordinate space for a proximity/AND
NOT condition — conflating the two evaluation paths would silently
reintroduce a false-positive class of bug. `FeatureScanOrchestrator` keeps
these as two genuinely separate code paths for this reason, selected
per-term by whether `resolvedPatterns` was present in that term's JSON
entry (the sole discriminator — a single compile-results file can mix old-
and new-style terms with no special handling).

`regex_match_hit_count` for a proximity term enumerates every distinct
satisfying leaf-occurrence combination found per area (not a single
synthetic "matched: yes"), capped at 50 per area with a separate, larger
internal backtracking work-bound so a pathological leaf-occurrence count
degrades by truncation (logged), never by failing the message.

**Open question, not yet resolved by a real example:** no PASS AND NOT term
under this new schema has been observed yet — it's unconfirmed whether such
a term still emits `requiredExpressionIds`/`excludedExpressionIds` (a cheap
per-area pre-filter) or relies purely on `resolvedPatterns`/`regexPattern`
with no id at all. `TermExpressionMetadata.parse()` handles both shapes
without hard-failing, and adds validation tripwires (throwing loudly if
`hyperscanExpressionId` or `patternMapping` is ever populated alongside an
AND-NOT-shaped `resolvedPatterns`) so a real example that contradicts either
assumption surfaces immediately rather than silently mis-evaluating.

### Optimization: why ONE combined loader now, not two

Both the `.hdb` database and the term metadata now come from the SAME GCS
object — see [Input path formation](#input-path-formation) for why the
Compile Service writes one `<feature>.zip` bundle per feature instead of two
separate files. `HyperscanBundleLoader` downloads and unzips that bundle
EXACTLY ONCE per feature per partition, caching both resulting objects
together (one `LexiconBundle` per feature, in one bounded `LruCache`) —
this **replaces** the earlier two-loader design (`HyperscanDatabaseLoader` +
`TermMetadataLoader`, each with its own broadcast/cache), which was a
deliberate choice at the time (a `Database` is heavy/native/off-heap, a
`TermExpressionMetadata` is light/on-heap, and the two came from genuinely
independent GCS objects) but would now mean downloading and unzipping the
identical file twice per feature per partition for no benefit, since
`FeatureScanOrchestrator` always needs both together for any feature it
scans anyway.

---

## Input path formation

### One GCS listing call, then pure string concatenation

`HyperscanPathResolver.resolveBasePath()` is the ONLY GCS *listing* call
this job makes for Hyperscan inputs, run once at job start on the driver.
It resolves the compile output folder's wildcard timestamp segment (the
Compile Service writes each run's bundle output under a
timestamp-suffixed subfolder), returning one base path string.

Every individual file path is then built by plain string concatenation —
no further listing calls, however many features a run references:

```
buildZipPath(basePath, feature)  → <basePath>/<feature>.zip
```

**One zip per feature, not two separate files.** The Compile Service used
to write two separate files per feature to this folder (`<feature>.hdb`,
`<feature>-compile-results.json`), resolved via two separate path-builder
methods. It now writes ONE `<feature>.zip` containing both as entries —
`<feature>.hdb` and `<feature>-compile-results.json`, the exact same names
as before, just zipped together instead of standing alone (see
`TermIdBuilder.hdbFileName`/`termMetadataFileName` for those two entry-name
builders, still used, now purely to identify entries INSIDE the zip rather
than top-level GCS object names).

### One small, driver-resolved map, broadcast once

```
featureToZipPath = { feature → .zip bundle path }
```

Built once (stage 3 of the pipeline — see
[Architecture & pipeline stages](#architecture--pipeline-stages)) from the
distinct feature set the BQ view references for this run, then broadcast
via `JavaSparkContext.broadcast(...)` — sent once per executor JVM, not
re-serialized per task. This used to be TWO maps (one per loader — see
"Optimization: why ONE combined loader now, not two" above); one map is
sufficient now that both artifacts resolve to the same GCS object.

### How the zip is read: `HyperscanBundleLoader`

`HyperscanBundleLoader.load(feature)` downloads the zip bytes via the
injected `GcsByteStreamer`, then reads it with a plain
`java.util.zip.ZipInputStream` (sequential, no seeking needed — matches how
the bytes arrive off a GCS read stream), matching each entry's BASE
filename (any directory prefix inside the zip is stripped) against
`TermIdBuilder.hdbFileName(feature)`/`termMetadataFileName(feature)` to
decide which buffer to fill. Missing either entry throws
`HyperscanFileLoadException` naming exactly which one was expected and
every entry name actually found in the zip — never a silent partial load.

---

## Output tables / BQ population

| Table | Grain | Notes |
|---|---|---|
| `lexicon-hit-summary` | One row per message | One `evaluated_lexicons` entry per **evaluated feature group** (not per sub-feature member) — includes NoiseReduction/Disclaimer/Lexicon groups alike, using raw (pre-suppression) match counts. `term_dtls.term_id` is now always the term's own `<feature>::<n>` — see [Hyperscan file processing](#hyperscan-file-processing-and-not-and-decomposed-terms) |
| `lexicon-hit-restricted` | One row per message (restricted source only) | Lexicon-category groups only, **post** disclaimer-suppression; `matched_text` is a serialized `hit_details_hs` JSON structure per term |
| `lexicon-hit-unrestricted` | One row per message (unrestricted source only) | Identical shape to `-restricted`, split purely by source GCS subfolder |
| `feature-hit-summary` | One row per message | Every evaluated group's resolved hit status, plus each multi-member group's own sub-feature breakdown |
| `pipeline_stage_audit` | One row per job start + one per completion | `IN_PROGRESS` → `SUCCESS`/`FAILED` |
| `pipeline_record_audit` | One row per **failed** message | Per-message processing failures — the whole job does not fail on one message's error |

`lexicon-hit-restricted`'s rows are also mirrored to a single CSV file at
`gs://<environment_bkt>/<policy_engine_id>/<process_id>/restricted/<pipeline_exec_id>.csv`
(nested columns flattened to JSON strings, since CSV has no native nested
representation).

### `term_dtls.term_id` population — the specific column this fix addresses

For every term a message's applicable features reference,
`OutputRowBuilder.buildSummaryRow()` reads `TermMatchResult.termId()`
directly from what `FeatureScanOrchestrator` returned — see
[Hyperscan file processing](#hyperscan-file-processing-and-not-and-decomposed-terms)
for the full resolution/evaluation path that now guarantees this is always
`TermIdBuilder.build(feature, termNumber)`, correctly, whether the term was
a simple pattern, a decomposed (no AND NOT) term, or an AND NOT term with
either side decomposed.

`regex_match_hit_count` counts every individual occurrence of a term's
matched pattern within a message — for an AND NOT term, this reflects only
the *required* side's occurrences (combined across every required-side
expression id, if the required side was itself decomposed), since the
excluded side is never itself a "hit" to count, only a condition already
resolved before a `TermMatchResult` is built at all. For a NEAR/FOLLOWEDBY
proximity term, this counts every distinct satisfying leaf-occurrence
combination found per scanned area (capped at 50 per area — see "A second
schema change" above), not a single synthetic "matched" flag.

---

## Configuration

| Source | Carries |
|---|---|
| `RuntimeArgs` (JSON, Airflow-supplied) | `dataset_details[]`, `feature_partition_value`, `pipeline_exec_id`, `policy_engine_id`, `process_id`, `trigger_type` |
| `BqTableConfig` (JSON on GCS, path passed as a Dataproc submit argument) | Fully-qualified view/table identifiers |
| `application.yml` / `application-test.yml` (Spring) | GCS buckets, cache sizing, audit identity strings |
| `SPECTRE_MAX_ATTACHMENT_SIZE_BYTES` (env var) | Skip attachments larger than this; unset = unlimited |

Confirmed values: `trigger_type` is `policy-alert-live` (always exactly one
`dataset_details` entry) or `policy-alert-test` (can have several — read
and unioned per entry).

---

## Suggested Spark configuration

Starting points below — this job has not run against a live cluster yet
(no Spark/GCP connectivity in this project's development environment), so
these are reasoned defaults grounded in the job's own architecture and
current Dataproc/Spark 4.x tuning guidance, not numbers pulled from an
actual run. **Treat them as a starting point to monitor and adjust from**,
particularly executor sizing and partition counts, once real message
volumes and `.hdb`/metadata file sizes are known.

### Executor sizing — favor moderate cores per executor, not maximal

```
spark.executor.cores=4
spark.executor.memory=14g
spark.executor.memoryOverhead=6g
spark.executor.instances=<see dynamic allocation below>
```

The general Spark best practice of ~5 cores per executor (the widely-cited
balance between within-executor parallelism and GC/overhead pressure — very
high core counts cause more GC contention, very low counts under-utilize
each executor) applies here, but this job has an **additional, specific**
reason to stay on the low side of that range: each concurrent task on an
executor can trigger its own zip-bundle download-and-cache cycle
(`HyperscanBundleLoader`, up to `max-cached-databases-per-partition` entries).
More cores means more concurrent partitions means more simultaneous
native-memory pressure and more simultaneous GCS read connections from the
same executor. 4 cores keeps this bounded while still getting reasonable
parallelism.

**`memoryOverhead` needs to be explicitly generous, not left at Dataproc's
auto-calculated default.** Hyperscan is accessed via JNI — every loaded
`.hdb` database and every active `Scanner` consumes **off-heap** native
memory that the JVM heap sizing (and therefore Dataproc's own default
overhead calculation) knows nothing about. Term metadata objects
(`TermExpressionMetadata`) are ordinary on-heap Java objects (plain id
lists), small relative to `.hdb` files, and not a significant contributor
to this overhead figure on their own. Undersizing overhead is a likely
source of YARN killing containers for exceeding physical memory even when
the JVM heap itself looks healthy in the Spark UI. Start with the ~6GB
above (well beyond the ~1-4GB typical non-native-library default) and watch
for container-killed errors; raise further if `.hdb` files turn out to be
large or `max-cached-databases-per-partition` is increased.

### Partitioning

```
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.shuffle.partitions=<start at 2-3x total executor cores, let AQE adjust>
```

Adaptive Query Execution should stay on — it rebalances post-shuffle
partition sizes automatically after the message↔view join and the view's
`groupBy(message_id)` aggregation, both of which have data-dependent skew
potential (some messages legitimately have far more applicable features
than others).

Aim for partitions large enough that a partition's `HyperscanBundleLoader`
cache gets reused across a meaningful number of messages before the task
ends (a partition of a few dozen messages barely benefits from the
per-partition cache at all), but not so large that one
partition's peak memory (its own message batch plus up to
`max-cached-databases-per-partition` loaded `.hdb` files) risks exceeding
executor memory. A few thousand messages per partition is a reasonable
target to tune around, adjusted by `spark.sql.files.maxPartitionBytes` on
the AVRO read side or an explicit `.repartition()` after the join if the
natural file-based partitioning doesn't land near that.

### Serialization

```
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

`PartitionProcessor`'s `mapPartitions` result type (`MessageProcessingResult`)
is already read via `Encoders.kryo(...)` in code; setting Kryo as the
**default** serializer (not just for that one Encoder) speeds up shuffle
and broadcast serialization generally — including the broadcast
`featureToZipPath` map, which is small but sent to every executor.
Explicit class registration (`spark.kryo.registrationRequired=true` plus a
registrator) is optional — Kryo works correctly without it, just marginally
slower on first use of an unregistered class — worth adding later if
serialization shows up in profiling, not a correctness requirement now.

### BigQuery connector writes

```
spark.datasource.bigquery.writeMethod=indirect
spark.datasource.bigquery.intermediateFormat=avro
spark.datasource.bigquery.temporaryGcsBucket=<staging-bucket>
```

For datasets above roughly 10GB, the indirect write path (stage as Avro on
GCS, then a single BigQuery load job) generally outperforms the direct
Storage Write API path, which is the better fit for the small, per-message-
scale tables here (`lexicon-hit-summary`, `-restricted`, `-unrestricted`,
`feature-hit-summary`, given "millions of messages"). The two audit tables
(`pipeline_stage_audit`: two rows per run; `pipeline_record_audit`: only
failed messages, expected to be a small fraction) are small enough that
`writeMethod=direct` is a reasonable alternative for just those two writes
if per-table tuning is preferred over one global setting — `OutputTableWriter`
doesn't currently set `writeMethod` explicitly (it uses the connector's own
default), so this is worth revisiting as an explicit `.option(...)` per
table if write performance profiling shows it matters.

**Connector version note (Spark 4.1 upgrade):** the BigQuery connector
project added a dedicated `spark-4.1-bigquery` module, but as of this
upgrade it is explicitly marked "currently in preview mode" by the
connector project itself — not yet a GA release the way the 3.x-line
connectors are. Confirm the exact connector patch version against its own
release notes at build/deploy time (see `pom.xml`'s own note on
`spark-bigquery-connector.version`), and track the connector's GA status
before depending on this write path for production volumes.

### Dynamic allocation

```
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=<baseline for the audit-write tail>
spark.dynamicAllocation.maxExecutors=<peak for expected daily message volume>
spark.dynamicAllocation.shuffleTracking.enabled=true
```

Daily message volume is not fixed, so scaling executor count to the actual
workload (rather than a fixed `--num-executors`) avoids both
over-provisioning on light days and under-provisioning on heavy ones.
Align the min/max bounds with the underlying Dataproc cluster's own
autoscaling policy so the two don't fight each other.

### Speculative execution

```
spark.speculation=false
```

Leave off, at least initially. Each task does real external work (possible
GCS zip-bundle fetches on cache miss, and ultimately a BigQuery
write) — a speculative retry duplicates that work, and this job's
correctness depends on the BigQuery connector's own handling of a
duplicated write attempt, which has not been verified against a live
cluster here. Revisit only after confirming the connector's duplicate-write
behavior is safe for this job's write pattern.

### GCS connector (per-partition zip-bundle streaming reads)

```
spark.hadoop.fs.gs.io.buffersize=<tune from default if profiling shows read latency>
spark.hadoop.fs.gs.http.max.retry=10
```

Every cache miss in `HyperscanBundleLoader` opens ONE fresh GCS read stream
per feature (the zip bundle containing both the `.hdb` and the metadata
JSON — previously two independent streams, one per loader, before the zip
consolidation); with many executors and moderate core counts, this can
still mean a meaningful number of concurrent GCS connections at job start
(before each partition's cache warms up). The retry setting above is a
safety margin for transient GCS throttling under that initial burst; the
buffer size is worth profiling against actual zip sizes rather than guessed.

### Spark 4.1 / Scala 2.13 / JDK 21 upgrade notes

- Spark 4.x is pre-built with **Scala 2.13 only** — Scala 2.12 support was
  officially dropped. Every Spark-family Maven artifact in `pom.xml` uses
  the `_2.13` suffix now; any custom Scala interop code (none currently in
  this project) would need the same.
- Spark 4.x requires **JDK 17+** at runtime; JDK 21 (an LTS release)
  satisfies this comfortably.
- **Maven Surefire's JVM flags changed** from the JDK-11-era minimal set to
  a broader module-open set Spark itself documents as necessary for JDK
  17+ (`java.nio`, `java.lang.invoke`, `jdk.internal.misc`, in addition to
  the JDK-11 set) — needed because Surefire's in-JVM local-mode
  `SparkSession` bypasses Spark's own `bin/spark-class` launcher (which
  supplies these automatically via `org.apache.spark.launcher.JavaModuleOptions`
  for a real `spark-submit`/cluster job) — see `pom.xml`'s own comment on
  this for the exact flag set and its source.
- Spark 4.0 made **ANSI SQL mode the default** — worth a specific check
  against `FeatureDecisionViewReader`'s own filter/join SQL for any
  implicit type coercion that was previously silently permitted and would
  now raise under ANSI mode, before first production run on this version.

---

## Build, test, deploy

```bash
mvn clean test        # unit + JUnit 5 tests, JaCoCo coverage report
mvn clean package      # shaded jar for Dataproc submission
```

Tests run against the `test` Spring profile (`application-test.yml`).

**Both commands are confirmed to genuinely succeed end-to-end** in a real
environment with the project's actual dependencies available (Maven
Central, Spark, the BigQuery connector, and — critically — the real
`com.gliwka.hyperscan` native library): `mvn clean test` runs all 126
tests against that real Hyperscan library and real Jackson serialization
(not a stub), and `mvn clean package` produces the shaded jar with the
correct `Main-Class` manifest entry. (104 at the point this claim was first
verified; grew to 126 across the NEAR/FOLLOWEDBY/AND-NOT `resolvedPatterns`
work and the `HyperscanDatabaseLoader`/`TermMetadataLoader` →
`HyperscanBundleLoader` zip-bundle consolidation — see "Known limitations"
below for both.) This corrects an earlier claim in
this document that testing relied on a "hand-built stub environment" for
Hyperscan — that was never accurate for this repository's own test
classes, two of which referenced test-only static fields on
`com.gliwka.hyperscan.wrapper.Database` that don't exist on the real
class and were never checked in; the test suite did not compile at all
until this was fixed (rewritten to compile real Hyperscan databases via
`Database.compile(...)`/`Database.save(...)`). See `CLAUDE.md`'s
"Testing" section for the full list of bugs this surfaced and fixed —
including one genuine production bug (`MatchedTextJson` had no
Jackson-serializable properties, so every real hit on
`lexicon-hit-restricted`/`-unrestricted` would have thrown at write time)
and one build-config bug (`spring-boot-starter-parent`'s inherited
`maven-shade-plugin` `pluginManagement` was silently corrupting this
project's own shade execution, so `mvn package` could never produce a
working jar).

What remains unverified: the 7 Spark-dependent files (see "Known
limitations" below) now compile as part of this build but still have no
dedicated unit tests, and nothing here has run against a live Dataproc
cluster, a real BigQuery table/view, or real GCP credentials.

```bash
gcloud dataproc jobs submit spark \
  --cluster=<cluster-name> \
  --region=<region> \
  --jar=gs://<bucket>/lexicon-scan-engine-2.0.0.jar \
  --class=com.db.macs3.ecomms.spectre.scanengine.spark.ScanEngineApplication \
  --properties="<see Suggested Spark configuration above>" \
  -- \
  gs://<bucket>/runtime-args/<process_id>.json \
  gs://<bucket>/config/bq-table-config.json
```

### Other design notes worth knowing

**`HtmlStrippingService`** — HTML tags and whitespace runs are collapsed to
a single space before scanning, with an offset map translating
stripped-text match positions back to the original message text, so a
match's reported position is always correct against the message the
analyst/downstream table actually sees, never the internally-stripped
version.

**Memory-safety design (`HyperscanBundleLoader`, `LruCache`)** — an earlier
approach read every `.hdb` file's full bytes on the **driver** into a
`Map<String, byte[]>` and broadcast that whole map to every executor,
risking driver OOM for many/large files and wasting executor memory on
files a given partition never needs. This engine instead downloads and
unzips each feature's `.zip` bundle **lazily, per partition, streamed** (via
`mapPartitions`, specifically so the loader and its cache are constructed
once and reused across every message in that partition), with each
partition's cumulative loaded-bundle memory bounded by a configurable
`LruCache` (`scan-engine.max-cached-databases-per-partition`, default 20 —
one bound now covers both the native database and its metadata together,
since a `LexiconBundle` holds both and they're always loaded/evicted as one
unit), evicting the least-recently-used entry rather than growing without
bound.

---

## Known limitations / deferred work

- **NEAR/FOLLOWEDBY proximity operator support (this pass)** — added for
  terms the Compile Service decomposes into QUIET Hyperscan leaves plus a
  `resolvedPatterns`/`patternMapping` description of the operator/distance
  structure, verified in Java against real message text since Hyperscan's
  own combination can only prove leaf presence, never distance/order — see
  "A second schema change" under
  [Hyperscan file processing](#hyperscan-file-processing-and-not-and-decomposed-terms).
  **Open gap**: no real PASS AND NOT term under this new schema has been
  observed yet (only a `FAILED` example exists in the sample data used to
  build this), so whether such a term still emits
  `requiredExpressionIds`/`excludedExpressionIds` or relies purely on
  `resolvedPatterns` with no id at all is unconfirmed — handled defensively
  with validation tripwires that throw loudly the moment a real example
  contradicts either assumption, rather than hard-coding one shape. Verify
  against a real Compile Service output once available.
- **Zip-bundle consolidation (this pass)** — the Compile Service now writes
  ONE `<feature>.zip` per feature (containing both `<feature>.hdb` and
  `<feature>-compile-results.json`) instead of two separate GCS objects.
  `HyperscanDatabaseLoader`/`TermMetadataLoader` (two loaders, two
  broadcasts, two caches) were replaced by a single `HyperscanBundleLoader`
  — see "Optimization: why ONE combined loader now, not two" under
  [Hyperscan file processing](#hyperscan-file-processing-and-not-and-decomposed-terms)
  and [Input path formation](#input-path-formation). Not verified against a
  real Compile Service zip output (only synthetic zips built in-test via
  `java.util.zip.ZipOutputStream`) — worth confirming entry-naming
  conventions (any directory prefix inside the zip, compression method)
  against a real Compile Service artifact before first production use,
  though `HyperscanBundleLoader` already strips any directory prefix
  defensively when matching entry names.
- **Five real bugs found and fixed by this revision's full-codebase
  review — resolved, not merely documented.** A prior revision's claims
  about test coverage and build state were not backed by an actual
  successful `mvn clean test`/`mvn clean package` run; this pass ran both
  for real and fixed what broke: two compile errors (`OutputRowBuilder`'s
  unreported `JsonProcessingException`; `ScanEngineJobRunner`'s ambiguous
  `Dataset.filter(...)` lambda overload), one genuine production bug
  (`MatchedTextJson` had no Jackson-serializable accessors, so every
  message with a surviving Lexicon-category hit would have thrown while
  building its `lexicon-hit-restricted`/`-unrestricted` row — the entire
  purpose of those two tables), one build-config bug (`spring-boot-starter-parent`'s
  inherited `maven-shade-plugin` `pluginManagement` silently corrupted
  this project's own shade execution, so `mvn package` could never
  produce a working jar), and a rewrite of two test classes that depended
  on a Hyperscan `Database` test stub that does not exist in this
  repository or the real dependency. See `CLAUDE.md`'s "Testing" section
  for full detail on each.
- **AND NOT / decomposed term_id resolution and boolean evaluation —
  resolved, correctly, in this revision.** An earlier version of this
  document claimed this was "resolved" based on the Compile Service's
  *original* id scheme (every term's reportable id always its own number).
  That scheme was itself confirmed broken and fixed at the Compile
  Service/Scanner Service level — this revision brings the Scan Engine's
  own resolution and evaluation logic into alignment with the *current*
  scheme. See [Hyperscan file processing](#hyperscan-file-processing-and-not-and-decomposed-terms)
  for the full, current explanation. Flagging the earlier claim's
  staleness explicitly here, rather than silently deleting it, since it is
  a useful reminder that this kind of cross-service id-scheme assumption
  is exactly the kind of thing that can silently drift out of sync when
  one service changes and a downstream consumer does not — worth an
  explicit compatibility check whenever the Compile Service's own id
  scheme changes again in the future.
- **`Match` position semantics** (`getStartPosition()`/`getEndPosition()`
  return Java char indices, not raw Hyperscan byte offsets; `getEndPosition()`
  is inclusive) are documented in `HyperscanScanService`'s own class
  Javadoc as verified against the wrapper library's own documentation, not
  against a live scan on a real native Hyperscan build — worth a direct,
  live-cluster confirmation before first production use, alongside the
  general Spark/GCS/BigQuery verification gap below.
- **Spark/BigQuery/GCS behavior is not verified against a live
  cluster/real GCP credentials** — that specific gap remains. It is
  narrower than earlier revisions of this document claimed, though: this
  pass confirmed the development environment DOES have full dependency
  access (Maven Central, Spark, the BigQuery connector, the real
  Hyperscan native library), and used it to get `mvn clean test` and
  `mvn clean package` to genuinely succeed for the first time — all 48
  main-source files, including the 7 Spark-dependent ones
  (`PartitionProcessor`, `ScanEngineJobRunner`, `MessageAvroReader`,
  `MessageRowConverter`, `OutputTableWriter`, `ViewRowConverter`,
  `FeatureDecisionViewReader`), now genuinely COMPILE as part of the
  build (previously only "reviewed by hand, not compiled" — that review
  missed two real compile errors and one real runtime serialization bug;
  see `CLAUDE.md`'s "Testing" section for the full list). Those 7 files
  still have **no dedicated unit tests of their own**, and Spark's actual
  distributed behavior, the BigQuery connector's real read/write path, and
  GCS connectivity remain unverified beyond successful compilation. Should
  be exercised against a real cluster (or the integration test setup
  below) before first production use.
- **BigQuery connector's Spark 4.1 support is in preview**, per the
  connector project's own release notes at the time of this upgrade — see
  the Spark configuration section's connector version note. Track its GA
  status before relying on it for production write volumes.
- **Integration test harness, Dockerfile** — not yet built.
- **`term_regex_pattern` for a term whose translated pattern list is
  unusually long** (many decomposed leaves) is currently joined with `" & "`
  as a single display string with no length cap — worth revisiting if a
  real lexicon rule produces a pattern list long enough to make this
  unwieldy in the BQ column, though no such case has been observed.
