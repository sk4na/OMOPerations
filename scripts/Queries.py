class ETLQueries:
    """
    Class containing the different SQL queries used to interact with OMOP tables for MePRAM ETL
    """

    load_observation_table_query = """
    INSERT INTO cdm.observation(
        person_id, observation_concept_id, observation_date, 
        observation_type_concept_id, value_as_number, value_as_concept_id, value_source_value, observation_event_id, obs_event_field_concept_id, visit_occurrence_id, observation_source_value
    ) VALUES(
        :person_id, :observation_concept_id, :observation_date, 
        :observation_type_concept_id, :value_as_number, :value_as_concept_id, :value_source_value, :observation_event_id, :obs_event_field_concept_id, :visit_occurrence_id, :observation_source_value
    ) RETURNING observation_id
    """

    load_observation_hepatopatia = """
    INSERT INTO cdm.observation(
        person_id, observation_concept_id, observation_date, 
        observation_type_concept_id, value_as_number, value_as_concept_id, qualifier_concept_id, value_source_value, observation_event_id, obs_event_field_concept_id, visit_occurrence_id
    ) VALUES(
        :person_id, :observation_concept_id, :observation_date, 
        :observation_type_concept_id, :value_as_number, :value_as_concept_id, :qualifier_concept_id, :value_source_value, :observation_event_id, :obs_event_field_concept_id, :visit_occurrence_id
    ) RETURNING observation_id
    """

    load_condition_table_query = """
    INSERT INTO cdm.condition_occurrence(
        person_id, condition_concept_id, condition_start_date, 
        condition_type_concept_id, visit_occurrence_id, condition_source_value
        ) VALUES(
        :person_id, :condition_concept_id, :condition_start_date, 
        :condition_type_concept_id, :visit_occurrence_id, :condition_source_value
    ) RETURNING condition_occurrence_id
    """

    load_condition_foco_query = """
    INSERT INTO cdm.condition_occurrence(
        person_id, condition_concept_id, condition_start_date, 
        condition_type_concept_id, condition_status_concept_id, visit_occurrence_id, condition_source_value
        ) VALUES(
        :person_id, :condition_concept_id, :condition_start_date, 
        :condition_type_concept_id, :condition_status_concept_id, :visit_occurrence_id, :condition_source_value
    ) RETURNING condition_occurrence_id
    """

    load_measurement_table_query = """
    INSERT INTO cdm.measurement(
        person_id, measurement_concept_id, measurement_date, 
        measurement_type_concept_id, operator_concept_id, value_as_number, 
        value_as_concept_id, unit_concept_id, measurement_event_id, meas_event_field_concept_id, visit_occurrence_id, measurement_source_value, value_source_value
    ) VALUES(
        :person_id, :measurement_concept_id, :measurement_date, 
        :measurement_type_concept_id, :operator_concept_id, :value_as_number, 
        :value_as_concept_id, :unit_concept_id, :measurement_event_id, :meas_event_field_concept_id, :visit_occurrence_id, :measurement_source_value, :value_source_value
    ) RETURNING measurement_id
    """

    load_procedure_occurrence_table = """INSERT INTO cdm.procedure_occurrence(person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, visit_occurrence_id, procedure_source_value
    ) VALUES(:person_id, :procedure_concept_id, :procedure_date, :procedure_type_concept_id, :visit_occurrence_id, :procedure_source_value)"""

    load_episode_table = "INSERT INTO cdm.episode(person_id, episode_concept_id, episode_start_date, episode_type_concept_id, episode_object_concept_id) VALUES(:person_id, :episode_concept_id, :episode_start_date, :episode_type_concept_id, :episode_object_concept_id) RETURNING episode_id"

    load_episode_event_table = "INSERT INTO cdm.episode_event(episode_id, event_id, episode_event_field_concept_id) VALUES(:episode_id, :event_id, :episode_event_field_concept_id)"

    query_drug_exposure_table = """INSERT INTO cdm.drug_exposure(person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id, visit_occurrence_id, drug_source_value) 
    VALUES(:person_id, :drug_concept_id, :drug_exposure_start_date, :drug_exposure_end_date, :drug_type_concept_id, :visit_occurrence_id, :drug_source_value) RETURNING drug_exposure_id"""

    load_specimen_table_query = """INSERT INTO cdm.specimen(person_id, specimen_concept_id, specimen_date, specimen_type_concept_id)
    VALUES(:person_id, :specimen_concept_id, :specimen_date, :specimen_type_concept_id) RETURNING specimen_id"""

    get_fecha_ingreso_query = "SELECT observation_date FROM cdm.observation WHERE person_id = :person_id AND observation_concept_id = :fecha_ingreso_concept_id"

    get_care_site_id_query = "SELECT care_site_id FROM cdm.care_site WHERE care_site_source_value = :care_site_source_value"

    load_person_table_query = """INSERT INTO cdm.person(person_id, gender_concept_id, year_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, care_site_id, person_source_value, gender_source_value) 
    VALUES(:person_id, :gender_concept_id, :year_of_birth, :birth_datetime, 0, 0, :care_site_id, :person_source_value, :gender_source_value)"""

    get_visit_occurrence_query = "SELECT visit_occurrence_id FROM cdm.visit_occurrence WHERE person_id = :person_id"

    get_episode_id = "SELECT episode_id FROM cdm.episode where person_id = :person_id"

    get_valid_patients_for_relationship_table = """
        SELECT DISTINCT po.person_id
        FROM management.etl_tracking a
        JOIN management.etl_tracking b ON a.source_person_id = b.source_person_id
        JOIN management.person_origin po ON a.source_person_id = po.source_person_id
        JOIN cdm.episode e ON po.person_id = e.person_id
        WHERE a.form_name = :form_name_1
        AND b.form_name = :form_name_2
        AND e.episode_object_concept_id = 432250;
    """
    get_condition_occurrence_id_from_episode = """
        WITH relevant_episode AS (
            SELECT episode_id
            FROM cdm.episode
            WHERE person_id = :person_id
        )
        SELECT event_id
        FROM cdm.episode_event
        WHERE episode_id IN (SELECT episode_id FROM relevant_episode)
        AND episode_event_field_concept_id = 1147129;
    """
    get_drug_exposure_id_from_episode = """
        WITH relevant_episode AS (
            SELECT episode_id
            FROM cdm.episode
            WHERE person_id = :person_id
        )
        SELECT event_id
        FROM cdm.episode_event
        WHERE episode_id IN (SELECT episode_id FROM relevant_episode)
        AND episode_event_field_concept_id = 1147096;
    """
    filter_redcap_query = """
    UPDATE ETL_management
    SET quality_check = 'invalid'
    WHERE source_person_id = :source_person_id
    AND form_name = :form_name
    """
