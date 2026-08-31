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
8. [Performance & scalability](#performance--scalability)
9. [Suggested Spark configuration](#suggested-spark-configuration)
10. [Build, test, deploy](#build-test-deploy)
11. [Known limitations / deferred work](#known-limitations--deferred-work)

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

## Performance & scalability

This section covers the JDK 21 / Spark 4.1 optimisations added in this
revision, targeting three concrete challenges this job faces at real
production scale: **data skew** (one message's attachment or applicable-
feature-count can dwarf the median by orders of magnitude, in ways plain
row-count-based partitioning does not account for), **thousands of
Hyperscan zip bundles** shared across **millions of messages**, and running
on a **shared Dataproc cluster** where this job cannot assume it has the
cluster's full attention or that cluster-wide defaults suit its own skew
shape. See [Suggested Spark configuration](#suggested-spark-configuration)
below for the accompanying config-only tuning.

### Eliminated: redundant work that scaled with (features × messages), not just messages

Two real, measured anti-patterns existed in the per-message scan hot path —
both scaling with how many lexicon features a message is evaluated against
(legitimately dozens, per `DecisionTreeEvaluator.evaluateGroup`, which calls
`FeatureScanOrchestrator.scanRow` once per applicable feature), not just
with message count:

- **A new native `Scanner` was constructed and destroyed on every single
  scan call.** The `com.gliwka.hyperscan-java` wrapper's own `Scanner`
  Javadoc: "In case of multithreaded scanning, you need one scanner
  instance per CPU thread" — i.e. ONE, reused, not one per call. Confirmed
  from the wrapper's own source: a `Scanner` holds a native function-pointer
  callback and scratch space, both real, per-JVM-process native resources
  the wrapper hard-caps at 256 live instances. `FeatureScanOrchestrator` now
  owns exactly ONE `Scanner` for its entire lifetime (`implements AutoCloseable`,
  one instance = one Spark partition, matching Spark's own
  single-thread-per-task execution model), calling `allocScratch(database)`
  before each scan (cheap/idempotent per the wrapper's own contract: "must
  be called at least once with each database... before scan is called" —
  calling it more often than strictly needed is safe) rather than
  constructing/destroying the whole `Scanner` every time.
- **`HtmlStrippingService.strip()` re-ran on the SAME subject/body/attachment
  text once per applicable feature.** The text doesn't change across which
  feature is being scanned — only the `Database` does. `FeatureScanOrchestrator.scannerFor`
  now strips every area's text EXACTLY ONCE per message, and every
  `scanRow` call for that message reuses the same precomputed
  `StripResult`. Attachment `cleanText` uses a new `HtmlStrippingService.identity()`
  path instead of `strip()` — attachment text is already HTML-free by the
  time it reaches this engine (see `MessageAttachment` class Javadoc), so
  running the full tag/whitespace-scanning algorithm (and allocating an
  `int[]` sized to the text's length) on text that can legitimately be
  megabytes long was pure waste for a transform guaranteed to be a no-op.
  `OffsetMap.identity()` maps every position to itself in O(1), with no
  backing array at all.

Both are pure internal restructuring — behaviourally identical output,
confirmed by the full existing test suite passing unchanged (see "Build,
test, deploy" below); only the *amount of redundant work* per message
changed.

### Concurrent Hyperscan bundle prefetch (JDK 21 virtual threads)

`HyperscanBundleLoader.prefetch(features)` (JDK 21's `Executors.newVirtualThreadPerTaskExecutor()`,
a stable, non-preview API — no `--enable-preview` flag needed anywhere in
the deploy pipeline) concurrently warms this partition's cache for a batch
of distinct features before any message is actually scanned, so the first
several messages in a partition don't each pay a COLD-CACHE GCS round-trip
serially, one feature at a time, the way they otherwise would.

**Why virtual threads are the right tool here, specifically, and not for
Hyperscan scanning itself**: the work being parallelised — GCS download +
zip extraction — is I/O-BOUND (waiting on network reads). Many concurrent
virtual threads blocked on I/O consume no executor CPU while waiting, so
this genuinely does not compete for the Spark task's allocated CPU core(s)
— important multi-tenant behaviour on a **shared** cluster, where grabbing
extra CPU beyond what YARN allocated this job would be poor citizenship.
Hyperscan scanning itself is deliberately **not** parallelised this way —
it's CPU-bound native work; virtual threads doing CPU-bound native (JNI)
calls pin their carrier thread rather than yielding it, so "parallelising"
CPU-bound scanning via virtual threads would not add real parallelism, only
overhead, and — worse — could oversubscribe the core(s) this job was
actually allocated. Scanning stays exactly as parallel as Spark's own task
scheduling already makes it (one task per allocated core), which is the
socially correct amount of parallelism to use on shared infrastructure.

**Thread-safety, made safe without touching `LruCache`'s own contract**:
`HyperscanBundleLoader`'s cache (`LruCache`) is deliberately NOT thread-safe
(documented on that class itself) — matching this loader's normal,
single-threaded, one-per-partition usage. `prefetch` stays safe under
concurrency via a strict two-phase design: the expensive, cache-independent
download/extract/parse work runs concurrently (one virtual thread per
feature); only once every one of those has completed does a single,
sequential, calling-thread-only loop insert the results into the cache —
the cache itself is never actually touched by more than one thread at a
time, regardless of how much concurrent I/O happened to produce those
results. A feature that fails to prefetch (bad path, corrupt zip, transient
GCS error) is silently skipped — best-effort only, since it will load
synchronously, with its real error, the first time it's actually needed;
prefetching must never be why a partition fails that would otherwise have
succeeded.

**Bounded lookahead, not a full-partition materialisation** — `PartitionProcessor`
peeks at most a small, FIXED number of rows ahead (`PREFETCH_LOOKAHEAD_ROWS`,
currently 200) purely to discover which distinct features the START of a
partition will need, then prefetches those, then continues streaming the
REST of the partition exactly as before (one row at a time from the
iterator). This is a deliberate, load-bearing design choice, not an
oversight: a partition can hold many thousands of messages, some
individually large (big attachments) — buffering the WHOLE partition into a
`List<Row>` to compute a fully exhaustive distinct-feature set would work
directly against the bounded, one-row-at-a-time memory footprint
`mapPartitions`'s iterator-based contract exists to provide, in exactly the
"some messages can be very large" scenario this needs to stay safe under.
`HyperscanBundleLoader.prefetch` also caps its own concurrent work at the
cache's own `maxSize()` — never spending concurrent I/O effort warming
entries that would just be evicted again before ever being consulted.

### Handling data skew — two independent sources, not one

A message's processing cost does not scale uniformly with its row's byte
size in the shuffle — two genuinely independent dimensions matter here:

1. **Attachment/body size skew** — one message's attachment text can be
   megabytes while the median is kilobytes. This DOES flow through the
   message↔view join's shuffle (attachment text is part of the AVRO row),
   so Spark's own byte-size-based skew detection (AQE's skew-join handling)
   is positioned to catch it — but only if its thresholds are tuned
   aggressively enough for THIS job's specific skew shape, not left at
   Spark's generic defaults (see [Suggested Spark configuration](#suggested-spark-configuration)).
2. **Applicable-feature-count skew** — independently of attachment size, a
   message tagged against an unusually large number of lexicon features
   (a large `features` array collected per row by the view's own
   `groupBy(message_id)`) costs proportionally more CPU to scan (one
   `FeatureScanOrchestrator.scanRow` call per applicable feature), even if
   its own text is unremarkable in size. This also contributes to the same
   row's serialized byte size (more array elements = more bytes), so it is
   NOT an entirely separate signal from Spark's perspective, but it is a
   genuinely separate real-world CAUSE worth naming, since "spread work
   evenly" tuning decisions (partition sizing, skew thresholds) should
   account for both causes, not just the more obviously visible one
   (attachment size).

`ScanEngineJobRunner.applyJobSpecificSparkConf` sets AQE/skew-join
thresholds explicitly (tightened from Spark's own defaults) and computes
`spark.sql.shuffle.partitions` relative to the driver's own observed
`defaultParallelism()` at job start, rather than trusting Spark's hardcoded
default (200) or a shared cluster's own `spark-defaults.conf` to happen to
suit this run's actual allocated core count. `spark.sql.files.maxPartitionBytes`
is also tightened for the AVRO read side specifically, so fewer OTHER
messages get bundled alongside one large one into the same initial
partition, before any shuffle-stage rebalancing even has a chance to help.
See [Suggested Spark configuration](#suggested-spark-configuration) for the
exact values and reasoning.

### Scaling to thousands of Hyperscan zip bundles across millions of messages

- **One combined loader, one download per feature per partition** — see
  [Hyperscan file processing](#hyperscan-file-processing-and-not-and-decomposed-terms)'s
  "Optimization: why ONE combined loader now, not two." With potentially
  thousands of distinct feature bundles across a whole job, and potentially
  many concurrent partitions/executors each independently populating their
  OWN cache, the SAME popular feature's zip can legitimately be downloaded
  from GCS more than once across the whole job (once per partition that
  needs it) — this is expected and accepted (broadcasting every bundle's
  full bytes from the driver instead, to eliminate this entirely, was
  deliberately rejected — see "Memory-safety design" below — since it would
  risk driver OOM for a job dealing in potentially thousands of files of
  unknown/unbounded individual size); tune `max-cached-databases-per-partition`
  (and, indirectly, partition size/count) to bound how often it happens in
  practice, not eliminate it structurally.
- **`max-cached-databases-per-partition` sizing** — this single bound now
  covers a partition's cumulative cached-bundle count (database + metadata
  together, per the ONE-loader design above). With thousands of distinct
  features in play, the RIGHT value depends on how many DISTINCT features a
  typical partition's messages actually reference (not the job-wide total)
  — too low and legitimate reuse thrashes (repeated eviction+reload of
  features still in active rotation within one partition); too high and
  per-partition native/off-heap memory grows unpredictably. Start from the
  default (20) and raise it if profiling shows a real partition's distinct-feature
  count typically exceeds it; watch `HyperscanBundleLoader.cachedCount()`-style
  signals (not currently exported as a metric — worth adding if this becomes
  a real tuning question) rather than guessing.
- **Prefetch amortises cold-start latency, not total download volume** —
  see "Concurrent Hyperscan bundle prefetch" above. It changes WHEN the
  first several downloads happen (concurrently, at partition start) and how
  long they collectively take (roughly the slowest one, not the sum), not
  HOW MANY total downloads the whole job does.

### Shared Dataproc cluster etiquette

- **Never grab more CPU than YARN allocated this job.** The one place this
  job introduces internal concurrency beyond Spark's own task scheduling —
  `HyperscanBundleLoader.prefetch`'s virtual threads — is deliberately
  scoped to I/O-bound work only (see above), which does not compete for
  allocated CPU cores while blocked waiting on network reads. CPU-bound
  Hyperscan scanning is deliberately left exactly as parallel as Spark's
  own per-task-one-core model already makes it — resist the temptation to
  "speed up" scanning with an ad hoc internal thread pool; on a shared
  cluster, that CPU was never allocated to this job in the first place.
- **This job sets its own critical Spark configs explicitly** (AQE/skew-join
  thresholds, shuffle partition count, `maxPartitionBytes`, the Kryo
  serializer) rather than relying on the shared cluster's
  `spark-defaults.conf` — see `ScanEngineJobRunner.applyJobSpecificSparkConf`.
  A cluster shared with other tenants' workloads has defaults shaped by
  THEIR jobs too, not tuned with this job's specific skew profile in mind;
  a job that depends on inherited global defaults being "close enough" is
  fragile to other tenants' configuration changes it has no visibility
  into or control over.
- **Dynamic allocation stays the right default**, not a fixed
  `--num-executors` — see [Suggested Spark configuration](#suggested-spark-configuration)'s
  own "Dynamic allocation" section, unchanged by this pass: scaling to
  actual workload (rather than reserving a fixed slice of the shared
  cluster regardless of whether this run needs it) is itself part of being
  a good multi-tenant citizen.

---

## Suggested Spark configuration

Starting points below — this job has not run against a live cluster yet
(no Spark/GCP connectivity in this project's development environment), so
these are reasoned defaults grounded in the job's own architecture and
current Dataproc/Spark 4.x tuning guidance, not numbers pulled from an
actual run. **Treat them as a starting point to monitor and adjust from**,
particularly executor sizing and partition counts, once real message
volumes and `.hdb`/metadata file sizes are known.

**Some of these are no longer just "suggested" — they're set programmatically
by the job itself**, in `ScanEngineJobRunner.applyJobSpecificSparkConf`
(the Kryo serializer, AQE/skew-join thresholds, `spark.sql.shuffle.partitions`,
`spark.sql.files.maxPartitionBytes` — see [Performance & scalability](#performance--scalability)'s
"Shared Dataproc cluster etiquette" for why: a shared cluster's own
`spark-defaults.conf` cannot be trusted to already suit this specific job's
skew profile). Settings marked **(code-enforced)** below take effect
regardless of the cluster's own defaults; everything else remains a
`--properties`/submit-time suggestion only.

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
overhead calculation) knows nothing about. Each concurrent task now holds
exactly ONE `Scanner` for its whole lifetime (see
[Performance & scalability](#performance--scalability) — reused across
every scan rather than one per call), whose scratch space grows to fit the
LARGEST database that task's partition ever scans against and is retained
until the partition finishes — a bounded, predictable off-heap footprint
per concurrent task, but still one this JVM-heap-based overhead
calculation cannot see. Term metadata objects
(`TermExpressionMetadata`) are ordinary on-heap Java objects (plain id
lists), small relative to `.hdb` files, and not a significant contributor
to this overhead figure on their own. Undersizing overhead is a likely
source of YARN killing containers for exceeding physical memory even when
the JVM heap itself looks healthy in the Spark UI. Start with the ~6GB
above (well beyond the ~1-4GB typical non-native-library default) and watch
for container-killed errors; raise further if `.hdb` files turn out to be
large or `max-cached-databases-per-partition` is increased.

### Partitioning and skew handling **(code-enforced)**

```
spark.sql.adaptive.enabled=true                                     (code-enforced)
spark.sql.adaptive.coalescePartitions.enabled=true                  (code-enforced)
spark.sql.adaptive.skewJoin.enabled=true                            (code-enforced)
spark.sql.adaptive.skewJoin.skewedPartitionFactor=3                 (code-enforced, tightened from Spark's default of 5)
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=128m    (code-enforced, tightened from Spark's default of 256m)
spark.sql.adaptive.advisoryPartitionSizeInBytes=64m                 (code-enforced)
spark.sql.shuffle.partitions=<max(200, 3 x defaultParallelism), computed at job start>  (code-enforced)
spark.sql.files.maxPartitionBytes=67108864                          (code-enforced, 64MB — down from Spark's default 128MB)
```

Adaptive Query Execution rebalances post-shuffle partition sizes
automatically after the message↔view join and the view's
`groupBy(message_id)` aggregation, both of which have data-dependent skew
potential from TWO independent sources — one message's attachment/body
size, and independently, one message's applicable-feature count (see
[Performance & scalability](#performance--scalability)'s "Handling data
skew" for the full reasoning). The `skewedPartitionFactor`/
`skewedPartitionThresholdInBytes` values above are deliberately tightened
from Spark's own generic defaults (factor 5, 256MB) — a single message with
an unusually large attachment can dwarf the median shuffle-partition size
by far more than 5x while still being one real, unsplittable row, so the
default threshold can under-react to exactly this job's skew shape.
`spark.sql.shuffle.partitions` is computed relative to the driver's own
observed `defaultParallelism()` at job start rather than left at Spark's
hardcoded default of 200, which has no relationship to how many executor
cores a given run actually has on a shared, dynamically-allocated cluster.
`maxPartitionBytes` is tightened on the AVRO read side specifically so
fewer OTHER messages get bundled alongside one large one into the same
initial partition, before any shuffle-stage rebalancing even has a chance
to help. **All of the above are set programmatically** in
`ScanEngineJobRunner.applyJobSpecificSparkConf` — a `--properties` override
at submit time can still change them if a specific run needs to, but the
job no longer depends on the shared cluster's own defaults happening to
already suit it.

Separately from the shuffle-partition count above, aim for
`mapPartitions`-stage partitions large enough that a partition's
`HyperscanBundleLoader` cache gets reused across a meaningful number of
messages before the task ends (a partition of a few dozen messages barely
benefits from the per-partition cache at all, and gets no benefit at all
from `HyperscanBundleLoader.prefetch`'s concurrent warm-up either — see
[Performance & scalability](#performance--scalability)), but not so large
that one partition's peak memory (its own message batch plus up to
`max-cached-databases-per-partition` loaded bundles) risks exceeding
executor memory. A few thousand messages per partition is a reasonable
target to tune around.

### Serialization **(code-enforced)**

```
spark.serializer=org.apache.spark.serializer.KryoSerializer   (code-enforced)
```

Set via `SparkSession.builder().config(...)` before `getOrCreate()` in
`ScanEngineJobRunner` — `spark.serializer` is a "static" config that only
takes effect if set before the `SparkContext` is actually constructed, NOT
via `spark.conf().set(...)` afterward (unlike every other code-enforced
setting on this page, which are ordinary runtime SQL configs).
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

### Garbage collection — Generational ZGC as an option for large-attachment workloads

```
spark.executor.extraJavaOptions=-XX:+UseZGC -XX:+ZGenerational
```

Not enabled by default (G1, JDK 21's default collector, remains a
reasonable baseline) — offered here as a starting point WORTH TRYING if
profiling shows GC pause time is a real problem, specifically because of
this job's own allocation pattern: HTML-stripping, Hyperscan match
processing, and JSON parsing all allocate short-lived `String`/`char[]`
objects proportional to message/attachment TEXT SIZE, and "some messages
can be very large" means this job's allocation pattern includes real
outliers — occasional very large, short-lived objects mixed in with the
usual small ones. Generational ZGC (JEP 439, a genuine JDK 21 production
feature — **not preview**, just not the default collector) targets
sub-millisecond pause times even on large heaps and is designed specifically
to handle this "mostly small, occasionally huge" allocation shape well.
This is a per-job Spark config (`spark.executor.extraJavaOptions`), not a
cluster-wide JVM change — safe to try on a **shared** cluster since it only
affects this job's own executor JVMs, never other tenants'. Since this
project has no live-cluster access to benchmark GC behaviour directly (see
"Known limitations"), treat this as a documented, reasoned option to A/B
test against G1 under real production load, not a settled recommendation.

### Spark 4.1 / Scala 2.13 / JDK 21 upgrade notes

- Spark 4.x is pre-built with **Scala 2.13 only** — Scala 2.12 support was
  officially dropped. Every Spark-family Maven artifact in `pom.xml` uses
  the `_2.13` suffix now; any custom Scala interop code (none currently in
  this project) would need the same.
- Spark 4.x requires **JDK 17+** at runtime; JDK 21 (an LTS release)
  satisfies this comfortably.
- **Virtual threads (JEP 444) are used in production code, not just
  discussed** — `HyperscanBundleLoader.prefetch` (see
  [Performance & scalability](#performance--scalability)) uses
  `Executors.newVirtualThreadPerTaskExecutor()`, a STABLE, non-preview JDK
  21 API — no `--enable-preview` flag needed anywhere in the build or
  deploy pipeline, unlike JDK 21's still-preview features (e.g. Structured
  Concurrency, JEP 453), which this project deliberately does NOT use for
  exactly that reason (a preview API would mean every executor JVM needs
  `--enable-preview`, a real operational complication on a shared cluster
  this project has no need to take on).
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
`com.gliwka.hyperscan` native library): `mvn clean test` runs all 138
tests against that real Hyperscan library and real Jackson serialization
(not a stub), and `mvn clean package` produces the shaded jar with the
correct `Main-Class` manifest entry. (104 at the point this claim was first
verified; 126 across the NEAR/FOLLOWEDBY/AND-NOT `resolvedPatterns` work and
the `HyperscanDatabaseLoader`/`TermMetadataLoader` → `HyperscanBundleLoader`
zip-bundle consolidation; 138 after this pass's performance work — new
coverage for `HtmlStrippingService.identity()`/`OffsetMap.identity()` and
`HyperscanBundleLoader.prefetch()`'s concurrency, capping, and best-effort
failure handling, including a real timing-based test proving the virtual-thread
prefetch genuinely runs concurrently rather than merely compiling — see
"Known limitations" below for all three passes.) This corrects an earlier claim in
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

- **Performance/scalability pass (this pass) — not verified against a live
  cluster or real production message-volume/skew profile.** See
  [Performance & scalability](#performance--scalability) for full detail on
  what changed: `Scanner` reuse and per-message HTML-strip caching in
  `FeatureScanOrchestrator`, `HyperscanBundleLoader.prefetch`'s JDK 21
  virtual-thread concurrent warm-up, and `ScanEngineJobRunner.applyJobSpecificSparkConf`'s
  explicit AQE/skew-join/partition-sizing tuning. All of it is confirmed
  correct by the full existing test suite passing unchanged (behaviourally
  identical output, just less redundant work) plus new tests specifically
  covering the new concurrency/caching behavior (138 tests total — see
  "Build, test, deploy"). What remains genuinely unverified: the actual
  MAGNITUDE of improvement (no before/after benchmark exists, since this
  project has no live Dataproc/GCS access), and every specific numeric
  config value (`skewedPartitionFactor=3`, `128m`/`64m` thresholds, the 200-row
  prefetch lookahead, `max-cached-databases-per-partition`'s default of 20) —
  all reasoned from the job's own architecture and documented Spark/Hyperscan
  behaviour, not tuned against real production message-size/skew
  distributions. Profile against real traffic before treating any of these
  numbers as final.
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
