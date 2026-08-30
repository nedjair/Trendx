-- ============================================================================
-- Trendx Catalogue — Schéma source Trendz 1.15.0 extrait du serveur source abandonné
-- Base : trendx
-- Propriétaire DDL : trendx_migration
-- Droits lecture/écriture : trendx_app
-- Source : pg_dump --schema-only de la base trendz sur thingsboard-trendz-postgres-1
-- ============================================================================
--
-- PostgreSQL database dump
--

-- Dumped from database version 16.11 (Debian 16.11-1.pgdg13+1)
-- Dumped by pg_dump version 16.11 (Debian 16.11-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Schéma cible du catalogue Trendx : trendx_catalog (Bounded Context).
-- Créé explicitement par cette migration pour que toute la chaîne (notamment
-- 005/006/007/008) puisse s'exécuter sur une base vierge SANS bootstrap CI
-- ad hoc. Le schéma public reste réservé aux extensions/objets PostgreSQL
-- (ex. pgcrypto) et aux dépendances partagées.
--

CREATE SCHEMA IF NOT EXISTS trendx_catalog;

--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: is_cached_telemetry_timestamps_do_not_intersect(uuid, uuid, character varying, bigint, bigint, character varying); Type: FUNCTION; Schema: trendx_catalog; Owner: -
--

CREATE OR REPLACE FUNCTION trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect(_business_entity_field_id uuid, _item_id uuid, _date_aggregation_type character varying, _end_ts bigint, _start_ts bigint, _function character varying) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
declare

BEGIN
  RETURN
    NOT EXISTS(
      SELECT 1 FROM trendx_catalog.cached_telemetry ct
      WHERE ct.item_id = _item_id
      and (
            --	case for everything except old calc fields. "function" field holds FieldGapSettings
            (_business_entity_field_id is not null and ct.business_entity_field_id = _business_entity_field_id)
      	    OR  --	case for old calculation field
            (_business_entity_field_id IS NULL AND NOT starts_with(_function, 'null') and _function = ct."function"))
      AND ct.date_aggregation_type = _date_aggregation_type
      AND NOT (ct.end_ts < _start_ts or ct.start_ts > _end_ts));
end;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: agent_ai; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.agent_ai (
    id uuid NOT NULL,
    agent_type character varying(255) NOT NULL,
    system_message character varying(65536) NOT NULL,
    version integer NOT NULL
);


--
-- Name: anomaly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.anomaly (
    id uuid NOT NULL,
    item_id uuid,
    item_name character varying(255),
    start_ts bigint,
    end_ts bigint,
    cluster_id bigint,
    score double precision,
    score_index integer,
    model_id uuid,
    alarm_id uuid
);


--
-- Name: anomaly_model_task_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.anomaly_model_task_data (
    anomaly_model_id uuid NOT NULL,
    enabled_refresh boolean DEFAULT false NOT NULL,
    enabled_save_to_tb boolean DEFAULT false NOT NULL,
    enabled_alarm_creation boolean DEFAULT false NOT NULL,
    item_set_json text,
    refresh_time_unit character varying(32),
    refresh_time_unit_count integer,
    refresh_time_unit_truncated boolean
);


--
-- Name: api_key; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.api_key (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    token character varying(255) NOT NULL
);


--
-- Name: business_entity; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.business_entity (
    id uuid NOT NULL,
    description character varying(255),
    hidden boolean NOT NULL,
    name character varying(255),
    query character varying(100000),
    shared_with_customers boolean NOT NULL,
    tenant_id uuid
);


--
-- Name: business_entity_field; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.business_entity_field (
    id uuid NOT NULL,
    description character varying(255),
    hidden boolean NOT NULL,
    name character varying(255),
    query character varying(100000),
    type character varying(255),
    calc_function character varying(100000),
    sql_id_key boolean NOT NULL,
    sql_ts_key boolean NOT NULL,
    business_entity_id uuid
);


--
-- Name: business_entity_field_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.business_entity_field_metadata (
    id uuid NOT NULL,
    business_entity_field_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    aggregation character varying(32),
    date_grouping character varying(32),
    range_config text
);


--
-- Name: business_entity_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.business_entity_metadata (
    id uuid NOT NULL,
    business_entity_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    item_id uuid,
    item_name character varying(256)
);


--
-- Name: cached_telemetry; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.cached_telemetry (
    id uuid NOT NULL,
    item_id uuid,
    tenant_id uuid,
    upload_time bigint,
    calculated_field boolean,
    state_field boolean,
    start_ts bigint,
    end_ts bigint,
    field_type character varying(255),
    field_aggregation character varying(255),
    date_aggregation_type character varying(255),
    business_entity_id uuid,
    business_entity_field_id uuid,
    function character varying(1000000),
    latest_telemetry_point_ts bigint,
    CONSTRAINT is_timestamps_do_not_intersect_constraint CHECK (trendx_catalog.is_cached_telemetry_timestamps_do_not_intersect(business_entity_field_id, item_id, date_aggregation_type, end_ts, start_ts, 'function'::character varying))
);


--
-- Name: cached_telemetry_point; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.cached_telemetry_point (
    ts bigint NOT NULL,
    cached_telemetry_id uuid NOT NULL,
    numeric_value double precision,
    string_value character varying(255),
    boolean_value boolean
);


--
-- Name: calculation_field; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.calculation_field (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    enabled boolean NOT NULL,
    name character varying(255) NOT NULL,
    creation_time bigint,
    update_time bigint,
    business_entity_id uuid NOT NULL,
    associated_entity_field_id uuid NOT NULL,
    language character varying(32) NOT NULL,
    calculation_field_type character varying(32) NOT NULL,
    return_data_type character varying(32) NOT NULL,
    grouping_interval character varying(32) NOT NULL,
    field_aggregation character varying(32) NOT NULL,
    fill_gap_enable boolean NOT NULL,
    fill_gap_time_unit character varying(32),
    fill_gap_strategy character varying(32),
    tb_telemetry_key character varying(64) NOT NULL,
    script character varying(51200) NOT NULL,
    time_range_strategy character varying(5120) NOT NULL,
    json_fixed_strategy_date_picker character varying(5120),
    manual_dataset_id uuid NOT NULL,
    split_time_range boolean NOT NULL,
    split_time_range_time_unit character varying(32) NOT NULL
);


--
-- Name: calculation_field_task_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.calculation_field_task_data (
    calculation_field_id uuid NOT NULL,
    enabled boolean,
    tz_name character varying(32),
    json_item_name_set text,
    json_item_set text,
    json_reprocess_date_picker_config text,
    refresh_time_unit character varying(32),
    refresh_time_unit_count integer,
    refresh_time_unit_truncated boolean
);


--
-- Name: cluster_example; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.cluster_example (
    id uuid NOT NULL,
    cluster_info_id uuid
);


--
-- Name: cluster_info; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.cluster_info (
    id uuid NOT NULL,
    cluster_id bigint,
    segments_count integer,
    segments_percent integer,
    duration_ms bigint,
    min_score double precision,
    max_score double precision,
    cluster_model_id uuid
);


--
-- Name: cluster_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.cluster_model (
    id uuid NOT NULL,
    tenant_id uuid,
    customer_id uuid,
    create_ts bigint NOT NULL,
    update_ts bigint NOT NULL,
    name character varying(255),
    status character varying(50),
    type character varying(50),
    properties_id uuid,
    dataset_config_id uuid,
    tb_telemetry_key character varying(64),
    score_associated_field_id uuid,
    score_index_associated_field_id uuid,
    telemetry_save_period_unit character varying(32),
    alarm_type character varying(255),
    enabled_alarm_deletion boolean DEFAULT true NOT NULL,
    alarm_warning_threshold integer,
    alarm_minor_threshold integer,
    alarm_major_threshold integer,
    alarm_critical_threshold integer
);


--
-- Name: custom_prediction_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.custom_prediction_model (
    id uuid NOT NULL,
    model_name character varying(32) NOT NULL,
    content character varying(5120) NOT NULL
);


--
-- Name: custom_prompt; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.custom_prompt (
    id uuid NOT NULL,
    name character varying(256) NOT NULL,
    prompt text,
    is_system boolean DEFAULT false NOT NULL,
    tenant_id uuid,
    customer_id uuid,
    user_id uuid,
    created_ts bigint NOT NULL,
    last_modified_ts bigint NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL
);


--
-- Name: custom_prompt_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.custom_prompt_metadata (
    id uuid NOT NULL,
    prompt text,
    data text,
    response text,
    tenant_id uuid NOT NULL,
    customer_id uuid,
    user_id uuid,
    duration bigint NOT NULL,
    created_ts bigint NOT NULL
);


--
-- Name: custom_view_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.custom_view_settings (
    domain character varying(128) NOT NULL,
    palette_selection character varying(32),
    palette_trendz character varying(5120),
    palette_tb character varying(5120),
    url character varying(128),
    tab_name character varying(128),
    logo_base64 character varying(1500000),
    dark_mode boolean NOT NULL,
    help_mode_enabled boolean NOT NULL,
    thingsboard_redirect_url character varying(512)
);


--
-- Name: dataset_config; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.dataset_config (
    id uuid NOT NULL,
    max_points_count integer,
    start_ts bigint,
    end_ts bigint,
    business_entity_id uuid,
    tz_name character varying(32),
    item_set text
);


--
-- Name: datasource; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.datasource (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    db_type character varying(255),
    url character varying(255),
    login character varying(255),
    pass character varying(255)
);


--
-- Name: domain_tenant_pair; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.domain_tenant_pair (
    domain character varying(1024) NOT NULL,
    tenant_id uuid NOT NULL
);


--
-- Name: latest_telemetry; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.latest_telemetry (
    calculation_field_id uuid NOT NULL,
    item_id uuid NOT NULL,
    key character varying(128) NOT NULL,
    value bigint NOT NULL
);


--
-- Name: licence_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.licence_data (
    tenant_id uuid NOT NULL,
    content character varying(100)
);


--
-- Name: llm_config; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.llm_config (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    created_ts bigint NOT NULL,
    updated_ts bigint NOT NULL,
    name character varying(255) NOT NULL,
    model_provider character varying(255) NOT NULL,
    model_name character varying(255) NOT NULL,
    json_properties character varying(10240) NOT NULL
);


--
-- Name: llm_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.llm_settings (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    active boolean NOT NULL,
    use_default boolean NOT NULL,
    default_llm_config_id uuid
);


--
-- Name: llm_settings_chat_type_link; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.llm_settings_chat_type_link (
    llm_setting_id uuid NOT NULL,
    chat_type character varying(255) NOT NULL,
    llm_config_id uuid NOT NULL
);


--
-- Name: manual_dataset; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.manual_dataset (
    id uuid NOT NULL,
    data character varying(10240) NOT NULL
);


--
-- Name: metric_definition; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.metric_definition (
    id uuid NOT NULL,
    business_entity_id uuid NOT NULL,
    item_id uuid NOT NULL,
    item_name character varying(255) NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid,
    name text NOT NULL,
    user_input text NOT NULL,
    description text NOT NULL,
    how_to_calculate text NOT NULL,
    is_advanced_mode boolean DEFAULT false NOT NULL,
    use_cases text,
    fields text,
    code text,
    calculation_field_id uuid,
    is_outdated boolean DEFAULT false NOT NULL,
    auto_deletable boolean DEFAULT true NOT NULL,
    created_ts bigint NOT NULL,
    updated_ts bigint NOT NULL
);


--
-- Name: metric_definition_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.metric_definition_metadata (
    id uuid NOT NULL,
    metric_definition_id uuid NOT NULL,
    business_entity_id uuid NOT NULL,
    item_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid,
    metric_name text NOT NULL,
    is_advanced_mode boolean DEFAULT false NOT NULL,
    is_saved boolean NOT NULL,
    created_ts bigint NOT NULL
);


--
-- Name: metric_exploration; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.metric_exploration (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    item_id uuid NOT NULL,
    item_name character varying(256) NOT NULL,
    business_entity_id uuid NOT NULL,
    business_entity_field_id uuid NOT NULL,
    metric_definition_id uuid NOT NULL,
    exploration_ts bigint NOT NULL
);


--
-- Name: ml_properties; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.ml_properties (
    id uuid NOT NULL,
    json_value character varying(10000000)
);


--
-- Name: prediction_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.prediction_model (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    created_ts bigint NOT NULL,
    updated_ts bigint NOT NULL,
    name character varying(256) NOT NULL,
    enabled boolean NOT NULL,
    partial_fit_enabled boolean NOT NULL,
    status character varying(32) NOT NULL,
    type character varying(32) NOT NULL,
    associated_entity_field_id uuid NOT NULL,
    tb_telemetry_key character varying(128) NOT NULL,
    model_parameters text NOT NULL,
    datasource_parameters text NOT NULL,
    method_parameters text NOT NULL,
    item_state_map text NOT NULL,
    trained_item_set text NOT NULL,
    business_entity_id uuid NOT NULL,
    business_entity_field_id uuid NOT NULL,
    avoid_disabling boolean NOT NULL
);


--
-- Name: prediction_model_last_item_point; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.prediction_model_last_item_point (
    prediction_model_id uuid NOT NULL,
    item_id uuid NOT NULL,
    ts bigint NOT NULL
);


--
-- Name: prediction_model_task_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.prediction_model_task_data (
    prediction_model_id uuid NOT NULL,
    item_set_json text,
    enabled boolean,
    enabled_partial_fit boolean,
    refresh_time_unit character varying(32),
    refresh_time_unit_count integer,
    refresh_time_unit_truncated boolean
);


--
-- Name: relation; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.relation (
    business_entity_id uuid NOT NULL,
    name character varying(255) NOT NULL,
    related_entity_id uuid NOT NULL,
    direction character varying(255) NOT NULL,
    enabled boolean NOT NULL,
    query character varying(100000)
);


--
-- Name: scored_point_anomaly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.scored_point_anomaly (
    t bigint,
    s double precision,
    anomaly_id uuid
);


--
-- Name: scored_point_centroid; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.scored_point_centroid (
    t bigint,
    s double precision,
    cluster_info_id uuid
);


--
-- Name: scored_point_cluster; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.scored_point_cluster (
    t bigint,
    s double precision,
    cluster_example_id uuid
);


--
-- Name: scored_point_histogram; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.scored_point_histogram (
    t bigint,
    s double precision,
    cluster_info_id uuid
);


--
-- Name: segment_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.segment_data (
    id uuid NOT NULL,
    model_id uuid,
    item_id uuid,
    item_name character varying(128),
    range_start_ts bigint,
    range_end_ts bigint,
    prehistorical_telemetry text,
    historical_telemetry text,
    additional_telemetries text,
    prediction_telemetry text
);


--
-- Name: trendz_system_property; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_system_property (
    property_key character varying(1000) NOT NULL,
    property_value character varying(1000)
);


--
-- Name: trendz_task; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_task (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    user_id uuid NOT NULL,
    created_ts bigint NOT NULL,
    updated_ts bigint NOT NULL,
    name character varying(256) NOT NULL,
    enabled boolean NOT NULL,
    reference_type character varying(128) NOT NULL,
    reference_key character varying(128) NOT NULL,
    job_type character varying(128) NOT NULL,
    json_job text NOT NULL,
    schedule_type character varying(128) NOT NULL,
    schedule_period_ts bigint NOT NULL,
    schedule_planned_ts bigint NOT NULL,
    schedule_scheduling_unit character varying(128) NOT NULL,
    schedule_scheduling_unit_count integer NOT NULL,
    schedule_scheduling_time_zone character varying(128) NOT NULL,
    ttl_enabled boolean NOT NULL,
    ttl_duration bigint NOT NULL,
    store_execution_enabled boolean NOT NULL,
    store_execution_count integer NOT NULL,
    json_configs text NOT NULL
);


--
-- Name: trendz_task_execution; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_task_execution (
    id uuid NOT NULL,
    task_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    user_id uuid NOT NULL,
    status character varying(128) NOT NULL,
    created_ts bigint NOT NULL,
    start_ts bigint NOT NULL,
    finish_ts bigint NOT NULL,
    duration bigint NOT NULL,
    json_progress_content text NOT NULL,
    job_type character varying(128) NOT NULL,
    json_job text NOT NULL,
    json_result text NOT NULL
);


--
-- Name: trendz_task_execution_progress_step; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_task_execution_progress_step (
    id uuid NOT NULL,
    execution_id uuid NOT NULL,
    parent_step_id uuid,
    name character varying(256) NOT NULL,
    start_ts bigint NOT NULL,
    finish_ts bigint NOT NULL
);


--
-- Name: trendz_task_execution_request; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_task_execution_request (
    task_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    user_id uuid NOT NULL,
    scheduled boolean NOT NULL,
    job_type character varying(128) NOT NULL,
    json_job text NOT NULL,
    created_ts bigint NOT NULL,
    state character varying(16) NOT NULL
);


--
-- Name: trendz_task_execution_state_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_task_execution_state_record (
    execution_id uuid NOT NULL,
    state character varying(16) NOT NULL,
    last_update_ts bigint NOT NULL,
    removed_task boolean NOT NULL
);


--
-- Name: trendz_task_scheduling_state_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_task_scheduling_state_record (
    task_id uuid NOT NULL,
    state character varying(16) NOT NULL,
    last_finish_ts bigint NOT NULL
);


--
-- Name: trendz_task_sequence; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_task_sequence (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    user_id uuid NOT NULL,
    created_ts bigint NOT NULL,
    updated_ts bigint NOT NULL,
    name character varying(256) NOT NULL,
    enabled boolean NOT NULL,
    json_configs character varying(10240) NOT NULL
);


--
-- Name: trendz_task_sequence_item; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.trendz_task_sequence_item (
    id uuid NOT NULL,
    sequence_id uuid NOT NULL,
    reference_type character varying(128) NOT NULL,
    reference_key character varying(128) NOT NULL,
    order_number integer,
    execution_delay_ms bigint
);


--
-- Name: user_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.user_metadata (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    default_business_entity_id uuid,
    default_date_picker_config text,
    default_item_id uuid,
    default_item_name character varying(256)
);


--
-- Name: user_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.user_record (
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    user_id uuid NOT NULL,
    username character varying(64) NOT NULL,
    visit_first_ts bigint NOT NULL,
    visit_last_ts bigint NOT NULL,
    valid boolean NOT NULL,
    validation_last_ts bigint NOT NULL,
    json_data text NOT NULL
);


--
-- Name: view_assistance_chat; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.view_assistance_chat (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid,
    user_id uuid,
    type character varying(255) NOT NULL,
    chat_summary character varying(255),
    available_topology text,
    reference_key character varying(255),
    metric_definition_id uuid,
    created_ts bigint NOT NULL,
    last_modified_ts bigint NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL
);


--
-- Name: view_assistance_chat_message; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.view_assistance_chat_message (
    id uuid NOT NULL,
    chat_id uuid NOT NULL,
    user_question character varying(65536) NOT NULL,
    code text,
    ai_answer text,
    ai_memory text,
    json_job_response text,
    last_modified_by character varying(255) NOT NULL,
    created_ts bigint NOT NULL,
    last_modified_ts bigint NOT NULL,
    version bigint NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL
);


--
-- Name: view_assistance_token_usage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.view_assistance_token_usage (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    input_token_used bigint DEFAULT 0 NOT NULL,
    output_token_used bigint DEFAULT 0 NOT NULL,
    model_name character varying(255),
    start_of_month_ts bigint NOT NULL,
    is_system_model boolean DEFAULT true NOT NULL
);


--
-- Name: view_collection; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.view_collection (
    id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    parent_id uuid NOT NULL,
    collection_name character varying(255) NOT NULL,
    creation_ts bigint NOT NULL,
    update_ts bigint NOT NULL
);


--
-- Name: view_config; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.view_config (
    id uuid NOT NULL,
    config_definition character varying(100000),
    created_at bigint,
    date_picker_config character varying(100000),
    name character varying(255),
    runtime_filters character varying(100000),
    settings character varying(100000),
    tz_name character varying(255),
    updated_at bigint,
    view_type character varying(255),
    enable_report_cache boolean,
    enable_persisted_cache boolean,
    cache_time_unit character varying(50),
    auto_refresh_cache boolean,
    task_id uuid,
    refresh_frequency_time_unit character varying(50),
    refresh_frequency_time_unit_count integer,
    enable_calculated_telemetry_saving boolean DEFAULT false NOT NULL,
    calculated_telemetry_saving_task_id uuid,
    calculated_telemetry_saving_execution_time_unit character varying(255),
    calculated_telemetry_saving_execution_time_unit_count integer,
    root_entity_id uuid,
    row_click_entity_id uuid,
    tenant_id uuid NOT NULL,
    customer_id uuid NOT NULL,
    collection_id uuid NOT NULL,
    is_favorite boolean NOT NULL,
    parent_path character varying(255) NOT NULL
);


--
-- Name: view_field; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS trendx_catalog.view_field (
    id uuid NOT NULL,
    aggregation_type character varying(255),
    use_delta boolean NOT NULL,
    color_config character varying(100000),
    condition_field_ids character varying(100000),
    date_grouping character varying(100000),
    enable_runtime_filter boolean NOT NULL,
    field_definition character varying(255),
    hidden boolean NOT NULL,
    skip_render boolean NOT NULL,
    include_historical_data boolean NOT NULL,
    field_label character varying(255),
    local_time_range character varying(100000),
    missed_relation_field boolean NOT NULL,
    batch_calculation boolean NOT NULL,
    native_calculation boolean NOT NULL,
    calc_function character varying(100000),
    local_calculation boolean NOT NULL,
    from_template_entity_field boolean NOT NULL,
    calculated_field boolean NOT NULL,
    state_condition character varying(100000),
    state_field boolean NOT NULL,
    state_property character varying(32),
    parsed_condition character varying(100000),
    parsed_function character varying(100000),
    is_anomaly_field boolean,
    anomaly_model_id uuid,
    is_prediction_model_field boolean,
    prediction_model_id uuid,
    prediction_model_orig_entity_field_id uuid,
    is_set_prediction_period boolean NOT NULL,
    prediction_period_unit character varying(32) NOT NULL,
    prediction_period_unit_count integer NOT NULL,
    selected_anomaly_field character varying(255),
    for_state_condition boolean NOT NULL,
    is_alarm_field boolean NOT NULL,
    selected_alarm_field character varying(255),
    script_language character varying(30),
    fill_gap_enable boolean NOT NULL,
    fill_gap_time_unit character varying(255),
    fill_gap_strategy character varying(255),
    prediction_enabled boolean NOT NULL,
    prediction_method character varying(255),
    prediction_range_sec integer NOT NULL,
    custom_prediction_method character varying(5120),
    multivariable_prediction_field_ids character varying(512),
    scale_value boolean NOT NULL,
    separate_axis boolean NOT NULL,
    separate_view_group boolean NOT NULL,
    seria_type character varying(255),
    unit character varying(255),
    virtual_date_field boolean NOT NULL,
    field_order integer NOT NULL,
    visually_hidden_field boolean NOT NULL,
    prediction_pre_aggregation_disabled boolean DEFAULT false NOT NULL,
    state_max_duration bigint DEFAULT 0 NOT NULL,
    entity_field_id uuid,
    business_entity_id uuid,
    view_config_id uuid,
    dataset_config_id uuid
);


--
-- Name: agent_ai agent_ai_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'agent_ai_pkey'
      AND conrelid = 'trendx_catalog.agent_ai'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.agent_ai ADD CONSTRAINT agent_ai_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: anomaly_model_task_data anomaly_model_task_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'anomaly_model_task_data_pkey'
      AND conrelid = 'trendx_catalog.anomaly_model_task_data'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.anomaly_model_task_data ADD CONSTRAINT anomaly_model_task_data_pkey PRIMARY KEY (anomaly_model_id);
  END IF;
END $$;


--
-- Name: anomaly anomaly_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'anomaly_pkey'
      AND conrelid = 'trendx_catalog.anomaly'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.anomaly ADD CONSTRAINT anomaly_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: api_key api_key_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'api_key_pkey'
      AND conrelid = 'trendx_catalog.api_key'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.api_key ADD CONSTRAINT api_key_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: api_key api_key_tenant_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'api_key_tenant_id_key'
      AND conrelid = 'trendx_catalog.api_key'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.api_key ADD CONSTRAINT api_key_tenant_id_key UNIQUE (tenant_id);
  END IF;
END $$;


--
-- Name: api_key api_key_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'api_key_token_key'
      AND conrelid = 'trendx_catalog.api_key'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.api_key ADD CONSTRAINT api_key_token_key UNIQUE (token);
  END IF;
END $$;


--
-- Name: business_entity_field_metadata business_entity_field_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'business_entity_field_metadata_pkey'
      AND conrelid = 'trendx_catalog.business_entity_field_metadata'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.business_entity_field_metadata ADD CONSTRAINT business_entity_field_metadata_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: business_entity_field_metadata business_entity_field_metadata_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'business_entity_field_metadata_unique'
      AND conrelid = 'trendx_catalog.business_entity_field_metadata'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.business_entity_field_metadata ADD CONSTRAINT business_entity_field_metadata_unique UNIQUE (business_entity_field_id, tenant_id, customer_id);
  END IF;
END $$;


--
-- Name: business_entity_field business_entity_field_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'business_entity_field_pkey'
      AND conrelid = 'trendx_catalog.business_entity_field'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.business_entity_field ADD CONSTRAINT business_entity_field_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: business_entity_metadata business_entity_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'business_entity_metadata_pkey'
      AND conrelid = 'trendx_catalog.business_entity_metadata'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.business_entity_metadata ADD CONSTRAINT business_entity_metadata_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: business_entity_metadata business_entity_metadata_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'business_entity_metadata_unique'
      AND conrelid = 'trendx_catalog.business_entity_metadata'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.business_entity_metadata ADD CONSTRAINT business_entity_metadata_unique UNIQUE (business_entity_id, tenant_id, customer_id);
  END IF;
END $$;


--
-- Name: business_entity business_entity_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'business_entity_pkey'
      AND conrelid = 'trendx_catalog.business_entity'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.business_entity ADD CONSTRAINT business_entity_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: cached_telemetry cached_telemetry_item_id_start_ts_end_ts_field_aggregation__key; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cached_telemetry_item_id_start_ts_end_ts_field_aggregation__key'
      AND conrelid = 'trendx_catalog.cached_telemetry'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cached_telemetry ADD CONSTRAINT cached_telemetry_item_id_start_ts_end_ts_field_aggregation__key UNIQUE (item_id, start_ts, end_ts, field_aggregation, date_aggregation_type, business_entity_field_id, function);
  END IF;
END $$;


--
-- Name: cached_telemetry cached_telemetry_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cached_telemetry_pkey'
      AND conrelid = 'trendx_catalog.cached_telemetry'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cached_telemetry ADD CONSTRAINT cached_telemetry_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: cached_telemetry_point cached_telemetry_point_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cached_telemetry_point_pkey'
      AND conrelid = 'trendx_catalog.cached_telemetry_point'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cached_telemetry_point ADD CONSTRAINT cached_telemetry_point_pkey PRIMARY KEY (ts, cached_telemetry_id);
  END IF;
END $$;


--
-- Name: calculation_field calculation_field_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'calculation_field_pkey'
      AND conrelid = 'trendx_catalog.calculation_field'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.calculation_field ADD CONSTRAINT calculation_field_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: calculation_field_task_data calculation_field_task_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'calculation_field_task_data_pkey'
      AND conrelid = 'trendx_catalog.calculation_field_task_data'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.calculation_field_task_data ADD CONSTRAINT calculation_field_task_data_pkey PRIMARY KEY (calculation_field_id);
  END IF;
END $$;


--
-- Name: calculation_field calculation_field_tenant_id_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'calculation_field_tenant_id_name_key'
      AND conrelid = 'trendx_catalog.calculation_field'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.calculation_field ADD CONSTRAINT calculation_field_tenant_id_name_key UNIQUE (tenant_id, name);
  END IF;
END $$;


--
-- Name: cluster_example cluster_example_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cluster_example_pkey'
      AND conrelid = 'trendx_catalog.cluster_example'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cluster_example ADD CONSTRAINT cluster_example_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: cluster_info cluster_info_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cluster_info_pkey'
      AND conrelid = 'trendx_catalog.cluster_info'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cluster_info ADD CONSTRAINT cluster_info_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: cluster_model cluster_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cluster_model_pkey'
      AND conrelid = 'trendx_catalog.cluster_model'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cluster_model ADD CONSTRAINT cluster_model_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: custom_prediction_model custom_prediction_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'custom_prediction_model_pkey'
      AND conrelid = 'trendx_catalog.custom_prediction_model'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.custom_prediction_model ADD CONSTRAINT custom_prediction_model_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: custom_prompt_metadata custom_prompt_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'custom_prompt_metadata_pkey'
      AND conrelid = 'trendx_catalog.custom_prompt_metadata'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.custom_prompt_metadata ADD CONSTRAINT custom_prompt_metadata_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: custom_prompt custom_prompt_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'custom_prompt_pkey'
      AND conrelid = 'trendx_catalog.custom_prompt'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.custom_prompt ADD CONSTRAINT custom_prompt_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: custom_view_settings custom_view_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'custom_view_settings_pkey'
      AND conrelid = 'trendx_catalog.custom_view_settings'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.custom_view_settings ADD CONSTRAINT custom_view_settings_pkey PRIMARY KEY (domain);
  END IF;
END $$;


--
-- Name: dataset_config dataset_config_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'dataset_config_pkey'
      AND conrelid = 'trendx_catalog.dataset_config'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.dataset_config ADD CONSTRAINT dataset_config_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: datasource datasource_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'datasource_pkey'
      AND conrelid = 'trendx_catalog.datasource'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.datasource ADD CONSTRAINT datasource_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: domain_tenant_pair domain_tenant_pair_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'domain_tenant_pair_pkey'
      AND conrelid = 'trendx_catalog.domain_tenant_pair'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.domain_tenant_pair ADD CONSTRAINT domain_tenant_pair_pkey PRIMARY KEY (tenant_id, domain);
  END IF;
END $$;


--
-- Name: latest_telemetry latest_telemetry_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'latest_telemetry_pkey'
      AND conrelid = 'trendx_catalog.latest_telemetry'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.latest_telemetry ADD CONSTRAINT latest_telemetry_pkey PRIMARY KEY (calculation_field_id, item_id, key);
  END IF;
END $$;


--
-- Name: licence_data licence_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'licence_data_pkey'
      AND conrelid = 'trendx_catalog.licence_data'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.licence_data ADD CONSTRAINT licence_data_pkey PRIMARY KEY (tenant_id);
  END IF;
END $$;


--
-- Name: llm_config llm_config_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'llm_config_pkey'
      AND conrelid = 'trendx_catalog.llm_config'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.llm_config ADD CONSTRAINT llm_config_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: llm_settings_chat_type_link llm_settings_chat_type_link_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'llm_settings_chat_type_link_pkey'
      AND conrelid = 'trendx_catalog.llm_settings_chat_type_link'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.llm_settings_chat_type_link ADD CONSTRAINT llm_settings_chat_type_link_pkey PRIMARY KEY (llm_setting_id, chat_type);
  END IF;
END $$;


--
-- Name: llm_settings llm_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'llm_settings_pkey'
      AND conrelid = 'trendx_catalog.llm_settings'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.llm_settings ADD CONSTRAINT llm_settings_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: llm_settings llm_settings_tenant_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'llm_settings_tenant_id_key'
      AND conrelid = 'trendx_catalog.llm_settings'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.llm_settings ADD CONSTRAINT llm_settings_tenant_id_key UNIQUE (tenant_id);
  END IF;
END $$;


--
-- Name: manual_dataset manual_dataset_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'manual_dataset_pkey'
      AND conrelid = 'trendx_catalog.manual_dataset'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.manual_dataset ADD CONSTRAINT manual_dataset_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: metric_definition_metadata metric_definition_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'metric_definition_metadata_pkey'
      AND conrelid = 'trendx_catalog.metric_definition_metadata'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.metric_definition_metadata ADD CONSTRAINT metric_definition_metadata_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: metric_definition metric_definition_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'metric_definition_pkey'
      AND conrelid = 'trendx_catalog.metric_definition'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.metric_definition ADD CONSTRAINT metric_definition_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: metric_exploration metric_exploration_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'metric_exploration_pkey'
      AND conrelid = 'trendx_catalog.metric_exploration'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.metric_exploration ADD CONSTRAINT metric_exploration_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: metric_exploration metric_exploration_raw_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'metric_exploration_raw_unique'
      AND conrelid = 'trendx_catalog.metric_exploration'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.metric_exploration ADD CONSTRAINT metric_exploration_raw_unique UNIQUE (tenant_id, customer_id, item_id, business_entity_id, business_entity_field_id, metric_definition_id);
  END IF;
END $$;


--
-- Name: ml_properties ml_properties_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ml_properties_pkey'
      AND conrelid = 'trendx_catalog.ml_properties'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.ml_properties ADD CONSTRAINT ml_properties_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: prediction_model_last_item_point prediction_model_last_item_point_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'prediction_model_last_item_point_pkey'
      AND conrelid = 'trendx_catalog.prediction_model_last_item_point'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.prediction_model_last_item_point ADD CONSTRAINT prediction_model_last_item_point_pkey PRIMARY KEY (prediction_model_id, item_id);
  END IF;
END $$;


--
-- Name: prediction_model prediction_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'prediction_model_pkey'
      AND conrelid = 'trendx_catalog.prediction_model'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.prediction_model ADD CONSTRAINT prediction_model_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: prediction_model_task_data prediction_model_task_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'prediction_model_task_data_pkey'
      AND conrelid = 'trendx_catalog.prediction_model_task_data'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.prediction_model_task_data ADD CONSTRAINT prediction_model_task_data_pkey PRIMARY KEY (prediction_model_id);
  END IF;
END $$;


--
-- Name: relation relation_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'relation_pkey'
      AND conrelid = 'trendx_catalog.relation'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.relation ADD CONSTRAINT relation_pkey PRIMARY KEY (business_entity_id, name, related_entity_id, direction);
  END IF;
END $$;


--
-- Name: segment_data segment_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'segment_data_pkey'
      AND conrelid = 'trendx_catalog.segment_data'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.segment_data ADD CONSTRAINT segment_data_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: trendz_system_property trendz_system_properties_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_system_properties_pkey'
      AND conrelid = 'trendx_catalog.trendz_system_property'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_system_property ADD CONSTRAINT trendz_system_properties_pkey PRIMARY KEY (property_key);
  END IF;
END $$;


--
-- Name: trendz_task_execution trendz_task_execution_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_execution_pkey'
      AND conrelid = 'trendx_catalog.trendz_task_execution'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_execution ADD CONSTRAINT trendz_task_execution_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: trendz_task_execution_progress_step trendz_task_execution_progress_step_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_execution_progress_step_pkey'
      AND conrelid = 'trendx_catalog.trendz_task_execution_progress_step'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_execution_progress_step ADD CONSTRAINT trendz_task_execution_progress_step_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: trendz_task_execution_request trendz_task_execution_request_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_execution_request_pkey'
      AND conrelid = 'trendx_catalog.trendz_task_execution_request'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_execution_request ADD CONSTRAINT trendz_task_execution_request_pkey PRIMARY KEY (task_id, execution_id);
  END IF;
END $$;


--
-- Name: trendz_task_execution_state_record trendz_task_execution_state_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_execution_state_record_pkey'
      AND conrelid = 'trendx_catalog.trendz_task_execution_state_record'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_execution_state_record ADD CONSTRAINT trendz_task_execution_state_record_pkey PRIMARY KEY (execution_id);
  END IF;
END $$;


--
-- Name: trendz_task trendz_task_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_pkey'
      AND conrelid = 'trendx_catalog.trendz_task'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task ADD CONSTRAINT trendz_task_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: trendz_task trendz_task_reference_type_reference_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_reference_type_reference_key_key'
      AND conrelid = 'trendx_catalog.trendz_task'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task ADD CONSTRAINT trendz_task_reference_type_reference_key_key UNIQUE (reference_type, reference_key);
  END IF;
END $$;


--
-- Name: trendz_task_sequence_item trendz_task_sequence_item_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_sequence_item_pkey'
      AND conrelid = 'trendx_catalog.trendz_task_sequence_item'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_sequence_item ADD CONSTRAINT trendz_task_sequence_item_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: trendz_task_sequence trendz_task_sequence_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_sequence_pkey'
      AND conrelid = 'trendx_catalog.trendz_task_sequence'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_sequence ADD CONSTRAINT trendz_task_sequence_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: trendz_task_scheduling_state_record trendz_task_state_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_state_record_pkey'
      AND conrelid = 'trendx_catalog.trendz_task_scheduling_state_record'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_scheduling_state_record ADD CONSTRAINT trendz_task_state_record_pkey PRIMARY KEY (task_id);
  END IF;
END $$;


--
-- Name: user_metadata user_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'user_metadata_pkey'
      AND conrelid = 'trendx_catalog.user_metadata'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.user_metadata ADD CONSTRAINT user_metadata_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: user_metadata user_metadata_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'user_metadata_unique'
      AND conrelid = 'trendx_catalog.user_metadata'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.user_metadata ADD CONSTRAINT user_metadata_unique UNIQUE (tenant_id, customer_id);
  END IF;
END $$;


--
-- Name: user_record user_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'user_record_pkey'
      AND conrelid = 'trendx_catalog.user_record'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.user_record ADD CONSTRAINT user_record_pkey PRIMARY KEY (tenant_id, customer_id, user_id);
  END IF;
END $$;


--
-- Name: view_assistance_chat_message view_assistance_chat_message_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'view_assistance_chat_message_pkey'
      AND conrelid = 'trendx_catalog.view_assistance_chat_message'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.view_assistance_chat_message ADD CONSTRAINT view_assistance_chat_message_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: view_assistance_chat view_assistance_chat_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'view_assistance_chat_pkey'
      AND conrelid = 'trendx_catalog.view_assistance_chat'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.view_assistance_chat ADD CONSTRAINT view_assistance_chat_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: view_assistance_token_usage view_assistance_token_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'view_assistance_token_usage_pkey'
      AND conrelid = 'trendx_catalog.view_assistance_token_usage'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.view_assistance_token_usage ADD CONSTRAINT view_assistance_token_usage_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: view_collection view_collection_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'view_collection_pkey'
      AND conrelid = 'trendx_catalog.view_collection'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.view_collection ADD CONSTRAINT view_collection_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: view_config view_config_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'view_config_pkey'
      AND conrelid = 'trendx_catalog.view_config'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.view_config ADD CONSTRAINT view_config_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: view_field view_field_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'view_field_pkey'
      AND conrelid = 'trendx_catalog.view_field'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.view_field ADD CONSTRAINT view_field_pkey PRIMARY KEY (id);
  END IF;
END $$;


--
-- Name: anomaly_cluster_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS anomaly_cluster_id_idx ON trendx_catalog.anomaly USING btree (cluster_id);


--
-- Name: anomaly_end_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS anomaly_end_ts_idx ON trendx_catalog.anomaly USING btree (end_ts);


--
-- Name: anomaly_item_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS anomaly_item_id_idx ON trendx_catalog.anomaly USING btree (item_id);


--
-- Name: anomaly_model_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS anomaly_model_id_idx ON trendx_catalog.anomaly USING btree (model_id);


--
-- Name: anomaly_score_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS anomaly_score_idx ON trendx_catalog.anomaly USING btree (score);


--
-- Name: anomaly_start_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS anomaly_start_ts_idx ON trendx_catalog.anomaly USING btree (start_ts);


--
-- Name: api_key_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS api_key_tenant_id_idx ON trendx_catalog.api_key USING btree (tenant_id);


--
-- Name: api_key_token_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS api_key_token_idx ON trendx_catalog.api_key USING btree (token);


--
-- Name: business_entity_field_business_entity_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS business_entity_field_business_entity_id_idx ON trendx_catalog.business_entity_field USING btree (business_entity_id);


--
-- Name: business_entity_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS business_entity_tenant_id_idx ON trendx_catalog.business_entity USING btree (tenant_id);


--
-- Name: cached_telemetry_business_entity_field_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_business_entity_field_id_idx ON trendx_catalog.cached_telemetry USING btree (business_entity_field_id);


--
-- Name: cached_telemetry_business_entity_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_business_entity_id_idx ON trendx_catalog.cached_telemetry USING btree (business_entity_id);


--
-- Name: cached_telemetry_date_aggregation_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_date_aggregation_type_idx ON trendx_catalog.cached_telemetry USING btree (date_aggregation_type);


--
-- Name: cached_telemetry_end_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_end_ts_idx ON trendx_catalog.cached_telemetry USING btree (end_ts);


--
-- Name: cached_telemetry_field_aggregation_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_field_aggregation_idx ON trendx_catalog.cached_telemetry USING btree (field_aggregation);


--
-- Name: cached_telemetry_function_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_function_idx ON trendx_catalog.cached_telemetry USING btree (function);


--
-- Name: cached_telemetry_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_id_idx ON trendx_catalog.cached_telemetry USING btree (id);


--
-- Name: cached_telemetry_item_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_item_id_idx ON trendx_catalog.cached_telemetry USING btree (item_id);


--
-- Name: cached_telemetry_point_cached_telemetry_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_point_cached_telemetry_id_idx ON trendx_catalog.cached_telemetry_point USING btree (cached_telemetry_id);


--
-- Name: cached_telemetry_point_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_point_ts_idx ON trendx_catalog.cached_telemetry_point USING btree (ts);


--
-- Name: cached_telemetry_start_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_start_ts_idx ON trendx_catalog.cached_telemetry USING btree (start_ts);


--
-- Name: cached_telemetry_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cached_telemetry_tenant_id_idx ON trendx_catalog.cached_telemetry USING btree (tenant_id);


--
-- Name: calculation_field_associated_entity_field_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS calculation_field_associated_entity_field_id_idx ON trendx_catalog.calculation_field USING btree (associated_entity_field_id);


--
-- Name: calculation_field_business_entity_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS calculation_field_business_entity_id_idx ON trendx_catalog.calculation_field USING btree (business_entity_id);


--
-- Name: calculation_field_calculation_field_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS calculation_field_calculation_field_type_idx ON trendx_catalog.calculation_field USING btree (calculation_field_type);


--
-- Name: calculation_field_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS calculation_field_customer_id_idx ON trendx_catalog.calculation_field USING btree (customer_id);


--
-- Name: calculation_field_enabled_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS calculation_field_enabled_idx ON trendx_catalog.calculation_field USING btree (enabled);


--
-- Name: calculation_field_language_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS calculation_field_language_idx ON trendx_catalog.calculation_field USING btree (language);


--
-- Name: calculation_field_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS calculation_field_name_idx ON trendx_catalog.calculation_field USING btree (name);


--
-- Name: calculation_field_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS calculation_field_tenant_id_idx ON trendx_catalog.calculation_field USING btree (tenant_id);


--
-- Name: cluster_example_cluster_info_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cluster_example_cluster_info_id_idx ON trendx_catalog.cluster_example USING btree (cluster_info_id);


--
-- Name: cluster_info_cluster_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cluster_info_cluster_id_idx ON trendx_catalog.cluster_info USING btree (cluster_id);


--
-- Name: cluster_info_cluster_model_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cluster_info_cluster_model_id_idx ON trendx_catalog.cluster_info USING btree (cluster_model_id);


--
-- Name: cluster_model_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cluster_model_customer_id_idx ON trendx_catalog.cluster_model USING btree (customer_id);


--
-- Name: cluster_model_dataset_config_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cluster_model_dataset_config_id_idx ON trendx_catalog.cluster_model USING btree (dataset_config_id);


--
-- Name: cluster_model_properties_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cluster_model_properties_id_idx ON trendx_catalog.cluster_model USING btree (properties_id);


--
-- Name: cluster_model_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS cluster_model_tenant_id_idx ON trendx_catalog.cluster_model USING btree (tenant_id);


--
-- Name: custom_prediction_model_model_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS custom_prediction_model_model_name_idx ON trendx_catalog.custom_prediction_model USING btree (model_name);


--
-- Name: dataset_config_business_entity_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS dataset_config_business_entity_id_idx ON trendx_catalog.dataset_config USING btree (business_entity_id);


--
-- Name: datasource_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS datasource_tenant_id_idx ON trendx_catalog.datasource USING btree (tenant_id);


--
-- Name: domain_tenant_pair_domain_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS domain_tenant_pair_domain_idx ON trendx_catalog.domain_tenant_pair USING btree (domain);


--
-- Name: domain_tenant_pair_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS domain_tenant_pair_tenant_id_idx ON trendx_catalog.domain_tenant_pair USING btree (tenant_id);


--
-- Name: latest_telemetry_calculation_field_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS latest_telemetry_calculation_field_id_idx ON trendx_catalog.latest_telemetry USING btree (calculation_field_id);


--
-- Name: latest_telemetry_item_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS latest_telemetry_item_id_idx ON trendx_catalog.latest_telemetry USING btree (item_id);


--
-- Name: latest_telemetry_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS latest_telemetry_key_idx ON trendx_catalog.latest_telemetry USING btree (key);


--
-- Name: llm_config_created_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS llm_config_created_ts_idx ON trendx_catalog.llm_config USING btree (created_ts);


--
-- Name: llm_config_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS llm_config_tenant_id_idx ON trendx_catalog.llm_config USING btree (tenant_id);


--
-- Name: llm_config_updated_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS llm_config_updated_ts_idx ON trendx_catalog.llm_config USING btree (updated_ts);


--
-- Name: llm_settings_chat_type_link_llm_config_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS llm_settings_chat_type_link_llm_config_id_idx ON trendx_catalog.llm_settings_chat_type_link USING btree (llm_config_id);


--
-- Name: llm_settings_chat_type_link_llm_setting_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS llm_settings_chat_type_link_llm_setting_id_idx ON trendx_catalog.llm_settings_chat_type_link USING btree (llm_setting_id);


--
-- Name: prediction_model_business_entity_field_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_business_entity_field_idx ON trendx_catalog.prediction_model USING btree (business_entity_field_id);


--
-- Name: prediction_model_business_entity_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_business_entity_idx ON trendx_catalog.prediction_model USING btree (business_entity_id);


--
-- Name: prediction_model_created_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_created_ts_idx ON trendx_catalog.prediction_model USING btree (created_ts);


--
-- Name: prediction_model_datasource_parameters_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_datasource_parameters_idx ON trendx_catalog.prediction_model USING btree (datasource_parameters);


--
-- Name: prediction_model_last_item_point_prediction_model_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_last_item_point_prediction_model_id_idx ON trendx_catalog.prediction_model_last_item_point USING btree (prediction_model_id);


--
-- Name: prediction_model_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_name_idx ON trendx_catalog.prediction_model USING btree (name);


--
-- Name: prediction_model_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_tenant_id_idx ON trendx_catalog.prediction_model USING btree (tenant_id);


--
-- Name: prediction_model_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_type_idx ON trendx_catalog.prediction_model USING btree (type);


--
-- Name: prediction_model_updated_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_updated_ts_idx ON trendx_catalog.prediction_model USING btree (updated_ts);


--
-- Name: prediction_model_user_combined_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS prediction_model_user_combined_idx ON trendx_catalog.prediction_model USING btree (tenant_id, customer_id);


--
-- Name: relation_business_entity_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS relation_business_entity_id_idx ON trendx_catalog.relation USING btree (business_entity_id);


--
-- Name: relation_related_entity_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS relation_related_entity_id_idx ON trendx_catalog.relation USING btree (related_entity_id);


--
-- Name: scored_point_anomaly_anomaly_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS scored_point_anomaly_anomaly_id_idx ON trendx_catalog.scored_point_anomaly USING btree (anomaly_id);


--
-- Name: scored_point_centroid_cluster_info_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS scored_point_centroid_cluster_info_id_idx ON trendx_catalog.scored_point_centroid USING btree (cluster_info_id);


--
-- Name: scored_point_cluster_cluster_example_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS scored_point_cluster_cluster_example_id_idx ON trendx_catalog.scored_point_cluster USING btree (cluster_example_id);


--
-- Name: scored_point_histogram_cluster_info_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS scored_point_histogram_cluster_info_id_idx ON trendx_catalog.scored_point_histogram USING btree (cluster_info_id);


--
-- Name: segment_data_item_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS segment_data_item_id_idx ON trendx_catalog.segment_data USING btree (item_id);


--
-- Name: segment_data_model_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS segment_data_model_id_idx ON trendx_catalog.segment_data USING btree (model_id);


--
-- Name: segment_data_range_end_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS segment_data_range_end_ts_idx ON trendx_catalog.segment_data USING btree (range_end_ts);


--
-- Name: segment_data_range_start_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS segment_data_range_start_ts_idx ON trendx_catalog.segment_data USING btree (range_start_ts);


--
-- Name: trendz_task_created_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_created_ts_idx ON trendx_catalog.trendz_task USING btree (created_ts);


--
-- Name: trendz_task_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_customer_id_idx ON trendx_catalog.trendz_task USING btree (customer_id);


--
-- Name: trendz_task_enabled_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_enabled_idx ON trendx_catalog.trendz_task USING btree (enabled);


--
-- Name: trendz_task_execution_created_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_created_ts_idx ON trendx_catalog.trendz_task_execution USING btree (created_ts);


--
-- Name: trendz_task_execution_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_customer_id_idx ON trendx_catalog.trendz_task_execution USING btree (customer_id);


--
-- Name: trendz_task_execution_duration_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_duration_ts_idx ON trendx_catalog.trendz_task_execution USING btree (duration);


--
-- Name: trendz_task_execution_finish_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_finish_ts_idx ON trendx_catalog.trendz_task_execution USING btree (finish_ts);


--
-- Name: trendz_task_execution_job_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_job_type_idx ON trendx_catalog.trendz_task_execution USING btree (job_type);


--
-- Name: trendz_task_execution_parent_step_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_parent_step_id_idx ON trendx_catalog.trendz_task_execution_progress_step USING btree (parent_step_id);


--
-- Name: trendz_task_execution_progress_step_execution_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_progress_step_execution_id_idx ON trendx_catalog.trendz_task_execution_progress_step USING btree (execution_id);


--
-- Name: trendz_task_execution_request_created_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_created_ts_idx ON trendx_catalog.trendz_task_execution_request USING btree (created_ts);


--
-- Name: trendz_task_execution_request_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_customer_id_idx ON trendx_catalog.trendz_task_execution_request USING btree (customer_id);


--
-- Name: trendz_task_execution_request_job_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_job_type_idx ON trendx_catalog.trendz_task_execution_request USING btree (job_type);


--
-- Name: trendz_task_execution_request_scheduled_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_scheduled_idx ON trendx_catalog.trendz_task_execution_request USING btree (scheduled);


--
-- Name: trendz_task_execution_request_state_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_state_ts_idx ON trendx_catalog.trendz_task_execution_request USING btree (state);


--
-- Name: trendz_task_execution_request_task_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_task_id_idx ON trendx_catalog.trendz_task_execution_request USING btree (task_id);


--
-- Name: trendz_task_execution_request_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_tenant_id_idx ON trendx_catalog.trendz_task_execution_request USING btree (tenant_id);


--
-- Name: trendz_task_execution_request_user_combined_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_user_combined_idx ON trendx_catalog.trendz_task_execution_request USING btree (tenant_id, customer_id, user_id);


--
-- Name: trendz_task_execution_request_user_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_request_user_id_idx ON trendx_catalog.trendz_task_execution_request USING btree (user_id);


--
-- Name: trendz_task_execution_start_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_start_ts_idx ON trendx_catalog.trendz_task_execution USING btree (start_ts);


--
-- Name: trendz_task_execution_state_record_last_update_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_state_record_last_update_ts_idx ON trendx_catalog.trendz_task_execution_state_record USING btree (last_update_ts);


--
-- Name: trendz_task_execution_state_record_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_state_record_state_idx ON trendx_catalog.trendz_task_execution_state_record USING btree (state);


--
-- Name: trendz_task_execution_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_status_idx ON trendx_catalog.trendz_task_execution USING btree (status);


--
-- Name: trendz_task_execution_task_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_task_id_idx ON trendx_catalog.trendz_task_execution USING btree (task_id);


--
-- Name: trendz_task_execution_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_tenant_id_idx ON trendx_catalog.trendz_task_execution USING btree (tenant_id);


--
-- Name: trendz_task_execution_user_combined_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_user_combined_idx ON trendx_catalog.trendz_task_execution USING btree (tenant_id, customer_id, user_id);


--
-- Name: trendz_task_execution_user_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_execution_user_id_idx ON trendx_catalog.trendz_task_execution USING btree (user_id);


--
-- Name: trendz_task_job_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_job_type_idx ON trendx_catalog.trendz_task USING btree (job_type);


--
-- Name: trendz_task_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_name_idx ON trendx_catalog.trendz_task USING btree (name);


--
-- Name: trendz_task_reference_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_reference_key_idx ON trendx_catalog.trendz_task USING btree (reference_key);


--
-- Name: trendz_task_reference_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_reference_type_idx ON trendx_catalog.trendz_task USING btree (reference_type);


--
-- Name: trendz_task_schedule_period_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_schedule_period_ts_idx ON trendx_catalog.trendz_task USING btree (schedule_period_ts);


--
-- Name: trendz_task_schedule_planned_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_schedule_planned_ts_idx ON trendx_catalog.trendz_task USING btree (schedule_planned_ts);


--
-- Name: trendz_task_schedule_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_schedule_type_idx ON trendx_catalog.trendz_task USING btree (schedule_type);


--
-- Name: trendz_task_scheduling_state_record_last_finish_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_scheduling_state_record_last_finish_ts_idx ON trendx_catalog.trendz_task_scheduling_state_record USING btree (last_finish_ts);


--
-- Name: trendz_task_scheduling_state_record_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_scheduling_state_record_state_idx ON trendx_catalog.trendz_task_scheduling_state_record USING btree (state);


--
-- Name: trendz_task_sequence_created_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_created_ts_idx ON trendx_catalog.trendz_task_sequence USING btree (created_ts);


--
-- Name: trendz_task_sequence_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_customer_id_idx ON trendx_catalog.trendz_task_sequence USING btree (customer_id);


--
-- Name: trendz_task_sequence_item_reference_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_item_reference_idx ON trendx_catalog.trendz_task_sequence_item USING btree (reference_type, reference_key);


--
-- Name: trendz_task_sequence_item_sequence_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_item_sequence_id_idx ON trendx_catalog.trendz_task_sequence_item USING btree (sequence_id);


--
-- Name: trendz_task_sequence_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_name_idx ON trendx_catalog.trendz_task_sequence USING btree (name);


--
-- Name: trendz_task_sequence_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_tenant_id_idx ON trendx_catalog.trendz_task_sequence USING btree (tenant_id);


--
-- Name: trendz_task_sequence_updated_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_updated_ts_idx ON trendx_catalog.trendz_task_sequence USING btree (updated_ts);


--
-- Name: trendz_task_sequence_user_combined_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_user_combined_idx ON trendx_catalog.trendz_task_sequence USING btree (tenant_id, customer_id, user_id);


--
-- Name: trendz_task_sequence_user_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_sequence_user_id_idx ON trendx_catalog.trendz_task_sequence USING btree (user_id);


--
-- Name: trendz_task_store_execution_count_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_store_execution_count_idx ON trendx_catalog.trendz_task USING btree (store_execution_count);


--
-- Name: trendz_task_store_execution_enabled_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_store_execution_enabled_idx ON trendx_catalog.trendz_task USING btree (store_execution_enabled);


--
-- Name: trendz_task_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_tenant_id_idx ON trendx_catalog.trendz_task USING btree (tenant_id);


--
-- Name: trendz_task_ttl_duration_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_ttl_duration_idx ON trendx_catalog.trendz_task USING btree (ttl_duration);


--
-- Name: trendz_task_ttl_enabled_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_ttl_enabled_idx ON trendx_catalog.trendz_task USING btree (ttl_enabled);


--
-- Name: trendz_task_updated_ts_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_updated_ts_idx ON trendx_catalog.trendz_task USING btree (updated_ts);


--
-- Name: trendz_task_user_combined_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_user_combined_idx ON trendx_catalog.trendz_task USING btree (tenant_id, customer_id, user_id);


--
-- Name: trendz_task_user_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS trendz_task_user_id_idx ON trendx_catalog.trendz_task USING btree (user_id);


--
-- Name: user_record_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS user_record_customer_id_idx ON trendx_catalog.user_record USING btree (customer_id);


--
-- Name: user_record_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS user_record_tenant_id_idx ON trendx_catalog.user_record USING btree (tenant_id);


--
-- Name: user_record_user_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS user_record_user_id_idx ON trendx_catalog.user_record USING btree (user_id);


--
-- Name: user_record_username_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS user_record_username_idx ON trendx_catalog.user_record USING btree (username);


--
-- Name: view_collection_collection_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_collection_collection_name_idx ON trendx_catalog.view_collection USING btree (collection_name);


--
-- Name: view_collection_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_collection_customer_id_idx ON trendx_catalog.view_collection USING btree (customer_id);


--
-- Name: view_collection_parent_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_collection_parent_id_idx ON trendx_catalog.view_collection USING btree (parent_id);


--
-- Name: view_collection_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_collection_tenant_id_idx ON trendx_catalog.view_collection USING btree (tenant_id);


--
-- Name: view_config_collection_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_config_collection_id_idx ON trendx_catalog.view_config USING btree (collection_id);


--
-- Name: view_config_customer_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_config_customer_id_idx ON trendx_catalog.view_config USING btree (customer_id);


--
-- Name: view_config_is_favorite_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_config_is_favorite_idx ON trendx_catalog.view_config USING btree (is_favorite);


--
-- Name: view_config_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_config_name_idx ON trendx_catalog.view_config USING btree (name);


--
-- Name: view_config_tenant_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_config_tenant_id_idx ON trendx_catalog.view_config USING btree (tenant_id);


--
-- Name: view_field_business_entity_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_field_business_entity_id_idx ON trendx_catalog.view_field USING btree (business_entity_id);


--
-- Name: view_field_dataset_config_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_field_dataset_config_idx ON trendx_catalog.view_field USING btree (dataset_config_id);


--
-- Name: view_field_entity_field_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_field_entity_field_id_idx ON trendx_catalog.view_field USING btree (entity_field_id);


--
-- Name: view_field_view_config_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS view_field_view_config_idx ON trendx_catalog.view_field USING btree (view_config_id);


--
-- Name: business_entity_field business_entity_field_business_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'business_entity_field_business_entity_id_fkey'
      AND conrelid = 'trendx_catalog.business_entity_field'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.business_entity_field ADD CONSTRAINT business_entity_field_business_entity_id_fkey FOREIGN KEY (business_entity_id) REFERENCES trendx_catalog.business_entity(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: cluster_example cluster_example_cluster_info_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cluster_example_cluster_info_id_fkey'
      AND conrelid = 'trendx_catalog.cluster_example'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cluster_example ADD CONSTRAINT cluster_example_cluster_info_id_fkey FOREIGN KEY (cluster_info_id) REFERENCES trendx_catalog.cluster_info(id);
  END IF;
END $$;


--
-- Name: cluster_info cluster_info_cluster_model_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cluster_info_cluster_model_id_fkey'
      AND conrelid = 'trendx_catalog.cluster_info'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cluster_info ADD CONSTRAINT cluster_info_cluster_model_id_fkey FOREIGN KEY (cluster_model_id) REFERENCES trendx_catalog.cluster_model(id);
  END IF;
END $$;


--
-- Name: cluster_model cluster_model_dataset_config_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cluster_model_dataset_config_id_fkey'
      AND conrelid = 'trendx_catalog.cluster_model'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cluster_model ADD CONSTRAINT cluster_model_dataset_config_id_fkey FOREIGN KEY (dataset_config_id) REFERENCES trendx_catalog.dataset_config(id);
  END IF;
END $$;


--
-- Name: cluster_model cluster_model_properties_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'cluster_model_properties_id_fkey'
      AND conrelid = 'trendx_catalog.cluster_model'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.cluster_model ADD CONSTRAINT cluster_model_properties_id_fkey FOREIGN KEY (properties_id) REFERENCES trendx_catalog.ml_properties(id);
  END IF;
END $$;


--
-- Name: trendz_task_sequence_item fk_sequence; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_sequence'
      AND conrelid = 'trendx_catalog.trendz_task_sequence_item'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_sequence_item ADD CONSTRAINT fk_sequence FOREIGN KEY (sequence_id) REFERENCES trendx_catalog.trendz_task_sequence(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: trendz_task_sequence_item fk_task_reference; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'fk_task_reference'
      AND conrelid = 'trendx_catalog.trendz_task_sequence_item'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_sequence_item ADD CONSTRAINT fk_task_reference FOREIGN KEY (reference_type, reference_key) REFERENCES trendx_catalog.trendz_task(reference_type, reference_key) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: llm_settings_chat_type_link llm_settings_chat_type_link_llm_config_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'llm_settings_chat_type_link_llm_config_id_fkey'
      AND conrelid = 'trendx_catalog.llm_settings_chat_type_link'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.llm_settings_chat_type_link ADD CONSTRAINT llm_settings_chat_type_link_llm_config_id_fkey FOREIGN KEY (llm_config_id) REFERENCES trendx_catalog.llm_config(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: llm_settings_chat_type_link llm_settings_chat_type_link_llm_setting_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'llm_settings_chat_type_link_llm_setting_id_fkey'
      AND conrelid = 'trendx_catalog.llm_settings_chat_type_link'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.llm_settings_chat_type_link ADD CONSTRAINT llm_settings_chat_type_link_llm_setting_id_fkey FOREIGN KEY (llm_setting_id) REFERENCES trendx_catalog.llm_settings(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: llm_settings llm_settings_default_llm_config_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'llm_settings_default_llm_config_id_fkey'
      AND conrelid = 'trendx_catalog.llm_settings'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.llm_settings ADD CONSTRAINT llm_settings_default_llm_config_id_fkey FOREIGN KEY (default_llm_config_id) REFERENCES trendx_catalog.llm_config(id) ON DELETE SET NULL;
  END IF;
END $$;


--
-- Name: relation relation_business_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'relation_business_entity_id_fkey'
      AND conrelid = 'trendx_catalog.relation'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.relation ADD CONSTRAINT relation_business_entity_id_fkey FOREIGN KEY (business_entity_id) REFERENCES trendx_catalog.business_entity(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: relation relation_related_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'relation_related_entity_id_fkey'
      AND conrelid = 'trendx_catalog.relation'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.relation ADD CONSTRAINT relation_related_entity_id_fkey FOREIGN KEY (related_entity_id) REFERENCES trendx_catalog.business_entity(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: trendz_task_execution_progress_step trendz_task_execution_progress_step_execution_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_execution_progress_step_execution_id_fkey'
      AND conrelid = 'trendx_catalog.trendz_task_execution_progress_step'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_execution_progress_step ADD CONSTRAINT trendz_task_execution_progress_step_execution_id_fkey FOREIGN KEY (execution_id) REFERENCES trendx_catalog.trendz_task_execution(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: trendz_task_execution_progress_step trendz_task_execution_progress_step_parent_step_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_execution_progress_step_parent_step_id_fkey'
      AND conrelid = 'trendx_catalog.trendz_task_execution_progress_step'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_execution_progress_step ADD CONSTRAINT trendz_task_execution_progress_step_parent_step_id_fkey FOREIGN KEY (parent_step_id) REFERENCES trendx_catalog.trendz_task_execution_progress_step(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: trendz_task_execution_request trendz_task_execution_request_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_execution_request_task_id_fkey'
      AND conrelid = 'trendx_catalog.trendz_task_execution_request'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_execution_request ADD CONSTRAINT trendz_task_execution_request_task_id_fkey FOREIGN KEY (task_id) REFERENCES trendx_catalog.trendz_task(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: trendz_task_execution trendz_task_execution_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'trendz_task_execution_task_id_fkey'
      AND conrelid = 'trendx_catalog.trendz_task_execution'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.trendz_task_execution ADD CONSTRAINT trendz_task_execution_task_id_fkey FOREIGN KEY (task_id) REFERENCES trendx_catalog.trendz_task(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- Name: view_assistance_chat_message view_assistance_chat_message_chat_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'view_assistance_chat_message_chat_id_fkey'
      AND conrelid = 'trendx_catalog.view_assistance_chat_message'::regclass
  ) THEN
    ALTER TABLE ONLY trendx_catalog.view_assistance_chat_message ADD CONSTRAINT view_assistance_chat_message_chat_id_fkey FOREIGN KEY (chat_id) REFERENCES trendx_catalog.view_assistance_chat(id) ON DELETE CASCADE;
  END IF;
END $$;


--
-- PostgreSQL database dump complete
--
