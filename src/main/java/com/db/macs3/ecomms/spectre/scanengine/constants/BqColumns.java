package com.db.macs3.ecomms.spectre.scanengine.constants;

/**
 * Column names for {@code vw_src_msg_lexicon_decision_mapping} and every
 * BigQuery output/audit table this job writes to. Centralised here so a
 * schema change touches one file, and so Spark {@code Dataset} column
 * references (which are string-keyed, not compiler-checked) are built from
 * named constants rather than scattered literals.
 */
public final class BqColumns {

    private BqColumns() {}

    /** Columns of {@code vw_src_msg_lexicon_decision_mapping}. */
    public static final class View {
        private View() {}
        public static final String PROCESS_ID              = "process_id";
        public static final String MESSAGE_ID               = "message_id";
        public static final String DATASET_PARTITION        = "dataset_partition";
        public static final String FEATURE_TAGGING_TYPE     = "feature_tagging_type";
        public static final String FEATURE_TYPE             = "feature_type";
        public static final String FEATURE_ID                = "feature_id";
        public static final String FEATURE_NAME              = "feature_name";
        public static final String SUB_FEATURE_TYPE         = "sub_feature_type";
        public static final String FEATURES_TO_APPLY        = "features_to_apply";
        public static final String IS_NOISE_REDUCTION       = "is_noise_reduction";
        public static final String OPERATOR                  = "operator";
        public static final String FEATURE_DEFINITION        = "feature_definition";
        public static final String FEATURE_PARTITION_VALUE  = "feature_partition_value";
        public static final String POLICY_ENGINE_ID          = "policy_engine_id";

        /** Query parameter names the view accepts — see class Javadoc on {@code FeatureDecisionViewReader}. */
        public static final String PARAM_DATASET_PARTITION_VALUE = "dataset_partition_value";
        public static final String PARAM_FEATURE_PARTITION_VALUE = "feature_partition_value";
        public static final String PARAM_PROCESS_ID               = "process_id";
    }

    /** {@code feature_definition} JSON field names (both root-level and nested {@code body}). */
    public static final class FeatureDefinitionJson {
        private FeatureDefinitionJson() {}
        public static final String FEATURE_NAME          = "featureName";
        public static final String FEATURE_TYPE           = "featureType";
        public static final String IS_NOISE_REDUCTION    = "isNoiseReduction";
        public static final String BODY                   = "body";
        public static final String BODY_FEATURE            = "feature";
        public static final String BODY_TOTAL_TERMS_COUNT = "totalTermsCount";
        public static final String BODY_MINIMUM_HITS      = "minimumHits";
        public static final String BODY_SCOPE              = "scope";

        // Recognised values of the "scope" array.
        public static final String SCOPE_SUBJECT           = "subject";
        public static final String SCOPE_MESSAGE_BODY      = "Message Body";
        public static final String SCOPE_ATTACHMENT         = "Attachment";
    }

    /** Feature-type values as seen in the view (case as delivered by upstream, not normalised). */
    public static final class FeatureType {
        private FeatureType() {}
        public static final String LEXICON          = "lexicon";
        public static final String COMPOSITE         = "composite";
        public static final String DISCLAIMER        = "disclaimer";
        public static final String NOISE_REDUCTION   = "NoiseReduction";
    }

    public static final String YES = "Y";
    public static final String NO  = "N";

    public static final String OPERATOR_OR  = "OR";
    public static final String OPERATOR_AND = "AND";

    /** {@code lexicon-hit-summary} table and its nested {@code evaluated_lexicons}/{@code term_dtls}. */
    public static final class LexiconHitSummary {
        private LexiconHitSummary() {}
        public static final String TABLE = "lexicon-hit-summary";
        public static final String MESSAGE_ID       = "message_id";
        public static final String PROCESS_ID        = "process_id";
        public static final String PIPELINE_EXEC_ID = "pipeline_exec_id";
        public static final String EVALUATED_LEXICONS = "evaluated_lexicons";
        public static final String CREATED_BY         = "created_by";
        public static final String CREATED_TS          = "created_ts";

        public static final class EvaluatedLexicon {
            private EvaluatedLexicon() {}
            public static final String ID                 = "id";
            public static final String NAME                = "name";
            public static final String TOTAL_TERMS_COUNT   = "total_terms_count";
            public static final String REGEX_HIT_COUNT     = "regex_hit_count";
            public static final String TERM_DTLS            = "term_dtls";
        }

        public static final class TermDtl {
            private TermDtl() {}
            public static final String TERM_ID              = "term_id";
            public static final String TERM_REGEX_PATTERN   = "term_regex_pattern";
            public static final String REGEX_MATCH_HIT_COUNT = "regex_match_hit_count";
        }
    }

    /** Shared shape of {@code lexicon-hit-restricted} and {@code lexicon-hit-unrestricted}. */
    public static final class LexiconHitDetail {
        private LexiconHitDetail() {}
        public static final String TABLE_RESTRICTED    = "lexicon-hit-restricted";
        public static final String TABLE_UNRESTRICTED  = "lexicon-hit-unrestricted";
        public static final String MESSAGE_ID                = "message_id";
        public static final String PROCESS_ID                 = "process_id";
        public static final String PIPELINE_EXEC_ID          = "pipeline_exec_id";
        public static final String DATASET_PARTITION_VALUE   = "dataset_partition_value";
        public static final String EVALUATED_LEXICONS         = "evaluated_lexicons";
        public static final String CREATED_BY                  = "created_by";
        public static final String CREATED_TS                   = "created_ts";

        public static final class EvaluatedLexicon {
            private EvaluatedLexicon() {}
            public static final String ID         = "id";
            public static final String TERM_DTLS  = "term_dtls";
        }

        public static final class TermDtl {
            private TermDtl() {}
            public static final String TERM_ID      = "term_id";
            public static final String MATCHED_TEXT = "matched_text";
        }
    }

    /** {@code feature-hit-summary} table and its nested {@code features}/{@code sub_features}. */
    public static final class FeatureHitSummary {
        private FeatureHitSummary() {}
        public static final String TABLE = "feature-hit-summary";
        public static final String MESSAGE_ID                = "message_id";
        public static final String DATASET_PARTITION_VALUE   = "dataset_partition_value";
        public static final String PIPELINE_EXEC_ID          = "pipeline_exec_id";
        public static final String PROCESS_ID                 = "process_id";
        public static final String FEATURE_HIT_TYPE           = "feature_hit_type";
        public static final String FEATURES                    = "features";
        public static final String CREATED_BY                  = "created_by";
        public static final String CREATED_TS                   = "created_ts";

        public static final class Feature {
            private Feature() {}
            public static final String ID                  = "id";
            public static final String NAME                 = "name";
            public static final String TYPE                  = "type";
            public static final String IS_NOISE_REDUCTION  = "is_noise_reduction";
            public static final String HIT_STATUS            = "hit_status";
            public static final String SUB_FEATURES           = "sub_features";
        }

        public static final class SubFeature {
            private SubFeature() {}
            public static final String TYPE       = "type";
            public static final String NAME        = "name";
            public static final String HIT_STATUS = "hit_status";
        }
    }

    /** {@code pipeline_stage_audit} table. */
    public static final class PipelineStageAudit {
        private PipelineStageAudit() {}
        public static final String TABLE = "pipeline_stage_audit";
        public static final String PROCESS_ID          = "process_id";
        public static final String TRIGGER_TYPE         = "trigger_type";
        public static final String PIPELINE_EXEC_ID    = "pipelinex_exec_id"; // sic — matches the delivered schema verbatim
        public static final String STAGE_NAME           = "stage_name";
        public static final String COMPOSER_DAG_NAME   = "composer_dag_name";
        public static final String COMPOSER_DAG_PATH    = "composer_dag_path";
        public static final String DPROC_DAG_NAME       = "dproc_dag_name";
        public static final String DPROC_DAG_PATH        = "dproc_dag_path";
        public static final String START_TIME            = "start_time";
        public static final String END_TIME               = "end_time";
        public static final String JOB_STATUS             = "job_status";
        public static final String ERROR_COUNT            = "error_count";
        public static final String ERROR_MESSAGE           = "error_message";
        public static final String ADDITIONAL_INFO        = "additional_info";
        public static final String EXECUTION_DATE          = "execution_date";
    }

    /** {@code pipeline_record_audit} table. */
    public static final class PipelineRecordAudit {
        private PipelineRecordAudit() {}
        public static final String TABLE = "pipeline_record_audit";
        public static final String PROCESS_ID          = "process_id";
        public static final String TRIGGER_TYPE         = "trigger_type";
        public static final String PIPELINE_EXEC_ID    = "pipelinex_exec_id"; // sic — matches the delivered schema verbatim
        public static final String STAGE_NAME           = "stage_name";
        public static final String RECORD_ID             = "record_id";
        public static final String STATUS                 = "status";
        public static final String RETURN_CODE            = "return_code";
        public static final String ERROR_MESSAGE           = "error_message";
        public static final String EXECUTION_DATE          = "execution_date";
        public static final String CREATED_BY              = "created_by";
        public static final String CREATED_TS               = "created_ts";
    }

    /** Job status values written to {@code pipeline_stage_audit.job_status}. */
    public static final class JobStatus {
        private JobStatus() {}
        public static final String IN_PROGRESS = "IN_PROGRESS";
        public static final String SUCCESS      = "SUCCESS";
        public static final String FAILED        = "FAILED";
    }

    /** Record status values written to {@code pipeline_record_audit.status}. */
    public static final class RecordStatus {
        private RecordStatus() {}
        public static final String SUCCESS = "SUCCESS";
        public static final String FAILED   = "FAILED";
    }
}
