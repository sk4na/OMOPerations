from sqlalchemy import create_engine, text
import logging
from Queries import ETLQueries


class OMOPDatabaseHandler:
    """Class that handles general database operations"""

    def __init__(self, db_credentials):
        try:
            self.engine = create_engine(
                f"postgresql://{db_credentials['user']}:{db_credentials['password']}@{db_credentials['server']}:{db_credentials['port']}/{db_credentials['dbname']}"
            )
        except Exception as e:
            logging.error(
                f"Couldn't connect to the database: {e}\nExiting ETL proces..."
            )
            raise

    def execute_query(self, query, params=None, return_id=False, return_all_rows=False):
        """Execute a query to the database, handling transactions properly.

        Args:
            query (str): SQL query to be executed.
            params (dict, optional): Parameters for query placeholders.
            return_id (bool, optional): Whether to return the id of the inserted row for INSERT queries.
            return_all_rows (bool, optional): Whether to return all rows from the query.

        Returns:
            Depending on the parameters, either the id of the inserted row, all results,
            a single result, or None in case of failure.
        """
        try:
            with self.engine.connect() as connection:
                with connection.begin() as transaction:
                    result = connection.execute(text(query), params)
                if "UPDATE" not in query:
                    if "INSERT" in query:
                        if return_id:
                            insert_id = result.fetchone()[0]
                            return insert_id
                        else:
                            return None
                    if return_all_rows:
                        return result.fetchall()
                    return result.fetchone()[0] if result.rowcount > 0 else None

        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return None

    def is_person_in_db(self, person_source_value):
        """Query the OMOP database to check if a patient is already inserted inside the OMOP database

        Args:
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.

        Returns:
            Bool: True if a patient has already some data inside the OMOP database, False if not.
        """
        query = "SELECT * FROM management.person_origin WHERE source_person_id = :person_source_value"
        result = self.execute_query(query, {"person_source_value": person_source_value})
        if result:
            return True
        else:
            return False

    def fetch_OMOP_concept_from_db(self, vocabulary, source_concept):
        """Query the OMOP database to get an OMOP concept ID for a given source concept and it's vocabulary of origin.

        Args:
            vocabulary (str): The vocabulary associated to the source concept.
            source_concept (str): The source concept we want to get the OMOP concept for.

        Returns:
            sqlalchemy.engine.Row: The OMOP concept ID associated to the given source concept.
        """
        combined_query = """
            SELECT CR.concept_id_2 
            FROM cdm.concept C
            JOIN cdm.concept_relationship CR ON C.concept_id = CR.concept_id_1
            WHERE C.vocabulary_id = :vocabulary 
            AND C.concept_code = :source_concept 
            AND CR.relationship_id = 'Maps to'
        """
        standard_concept_id = self.execute_query(
            combined_query, {"vocabulary": vocabulary, "source_concept": source_concept}
        )
        return standard_concept_id

    def is_form_processed(self, source_person_id, form_name):
        """Query the OMOP database to check if a RedCap form has already been processed for a given patient.

        Args:
            source_person_id (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            form_name (str): The name of the RedCap form for which we want to know if it has been already processed.

        Returns:
            Bool: True if a form has been already processed for a specific patient, False if not.
        """
        query = """
            SELECT COUNT(1) FROM management.ETL_tracking 
            WHERE source_person_id = :source_person_id AND form_name = :form_name
        """
        result = self.execute_query(
            query, {"source_person_id": source_person_id, "form_name": form_name}
        )
        return result > 0

    def update_form_tracking(self, source_person_id, form_name):
        """Query the OMOP database to update the ETL tracking table.

        Args:
            source_person_id (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            form_name (str): The name of the RedCap form for which we want to know if it has been already processed.

        """
        query = """
            INSERT INTO management.ETL_tracking (source_person_id, form_name, processed_date) 
            VALUES (:source_person_id, :form_name, CURRENT_DATE)
        """
        self.execute_query(
            query, {"source_person_id": source_person_id, "form_name": form_name}
        )

    def populate_person_origin_table(self, person_source_value):
        """Query the OMOP database to populate the custom 'person_origin' table used for the ETL process, and retrieve the OMOP person id assigned to each source patient.

        Args:
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.

        Returns:
            int: The patient's OMOP person id
        """
        if self.is_person_in_db(person_source_value):
            get_person_id_query = """
                SELECT person_id 
                FROM management.person_origin 
                WHERE source_person_id = :person_source_value
            """
            person_id = self.execute_query(
                get_person_id_query, {"person_source_value": person_source_value}
            )
            # logging.info(
            #    f"Patient {person_source_value} already included in the database. Person_id: {person_id}"
            # )
            return person_id
        else:
            get_max_person_id_query = (
                "SELECT MAX(person_id) FROM management.person_origin"
            )
            max_person_id = self.execute_query(get_max_person_id_query)
            if max_person_id:
                query = """
                    INSERT INTO management.person_origin 
                    VALUES('MEPRAM-Sepsis', :person_source_value, :person_id)
                """
                self.execute_query(
                    query,
                    {
                        "person_source_value": person_source_value,
                        "person_id": max_person_id + 1,
                    },
                )
                # logging.info(
                #    f"Patient {person_source_value} not included in the database. Added with person_id: {max_person_id + 1}"
                # )
                return max_person_id + 1
            else:
                query = """
                    INSERT INTO management.person_origin 
                    VALUES('MEPRAM-Sepsis', :person_source_value, 1)
                """
                self.execute_query(query, {"person_source_value": person_source_value})
                # logging.info(
                #    f"Patient {person_source_value} is the first patient to be included in the database. Added with person_id 1"
                # )
                return 1

    def populate_care_site_table(self, hospital_codes):
        """Query the OMOP database to populate the care_site table according to the DAG info from RedCap.

        Args:
            hospital_codes (dict): Python dictionary containing the Data Access Group names and their unique id from RedCap.  Given that each hospital has it's own unique DAG, this information is equivalent
            to the hospital identifying codes.
        """
        try:
            with self.engine.connect() as connection:
                existing_record_count = connection.execute(
                    text("SELECT COUNT(*) FROM cdm.care_site")
                ).scalar()
            if existing_record_count == 14:
                logging.info(
                    "Hospital codes already present in the care_site table. Skipping insertion."
                )
                return

            data_to_insert = [
                {
                    "care_site_name": hospital_code["data_access_group_name"],
                    "care_site_source_value": hospital_code["data_access_group_id"],
                }
                for hospital_code in hospital_codes
            ]

            query = """
                INSERT INTO cdm.care_site(care_site_name, care_site_source_value) 
                VALUES (:care_site_name, :care_site_source_value)
            """

            self.execute_query(query, data_to_insert)

            logging.info("Hospital codes successfully mapped into the care_site table!")
        except Exception as e:
            logging.error(
                f"Error mapping RedCap hospital codes (DAGs) to the care_site table: {e}"
            )

    def load_observation_period_table(
        self, person_id, start_date, end_date, period_type_concept_id
    ):
        """Insert an observation period for a patient into the OMOP CDM.

        Args:
            person_id (int): OMOP person_id of the patient.
            start_date (date or str): Observation period start date (YYYY-MM-DD if str).
            end_date (date or str): Observation period end date (YYYY-MM-DD if str).
            period_type_concept_id (int): Concept ID describing the type of observation period.

        Returns:
            None
        """
        query = """INSERT INTO cdm.observation_period(person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id) 
        VALUES(:person_id, :observation_period_start_date, :observation_period_end_date, :period_type_concept_id)"""

        self.execute_query(
            query,
            {
                "person_id": person_id,
                "observation_period_start_date": start_date,
                "observation_period_end_date": end_date,
                "period_type_concept_id": period_type_concept_id,
            },
        )

    def load_visit_ocurrence_table(
        self,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_end_date,
        visit_type_concept_id,
    ):
        """Insert a visit occurrence for a patient into the OMOP CDM.

        Args:
            person_id (int): OMOP person_id of the patient.
            visit_concept_id (int): Concept ID representing the visit (e.g., inpatient, outpatient).
            visit_start_date (date or str): Visit start date (YYYY-MM-DD if str).
            visit_end_date (date or str): Visit end date (YYYY-MM-DD if str).
            visit_type_concept_id (int): Concept ID describing the provenance/type of the visit record.

        Returns:
            Optional[int]: The newly created visit_occurrence_id if returned by the underlying query, otherwise None.
        """
        query = """INSERT INTO cdm.visit_occurrence(person_id, visit_concept_id, visit_start_date, visit_end_date, visit_type_concept_id) 
        VALUES(:person_id, :visit_concept_id, :visit_start_date, :visit_end_date, :visit_type_concept_id) RETURNING visit_occurrence_id"""

        self.execute_query(
            query,
            {
                "person_id": person_id,
                "visit_concept_id": visit_concept_id,
                "visit_start_date": visit_start_date,
                "visit_end_date": visit_end_date,
                "visit_type_concept_id": visit_type_concept_id,
            },
            return_id=True,
        )

    def populate_fact_relationship_table(
        self,
        relationship_type,
        previous_condition=None,
        previous_organism=None,
    ):
        """Populate the OMOP fact_relationship table for specific clinical relationships.

        Depending on the ``relationship_type``, this method creates forward and reverse links
        between Condition, Drug, and Observation records to represent relationships such as:
        an infectious disease episode (condition ↔ drug), a previous infection
        (condition ↔ causative organism), or a previous colonization (observation ↔ organism).

        Args:
            relationship_type (str): One of ``'infectious_disease_episode'``,
                ``'previous_infection'``, or ``'previous_colonization'``.
            previous_condition (Optional[int]): The condition or observation fact_id to relate
                (required for ``'previous_infection'`` and ``'previous_colonization'``).
            previous_organism (Optional[int]): The organism observation fact_id to relate
                (required for ``'previous_infection'`` and ``'previous_colonization'``).

        Returns:
            None
        """
        if relationship_type == "infectious_disease_episode":
            valid_patients = [
                row
                for row, in self.execute_query(
                    ETLQueries.get_valid_patients_for_relationship_table,
                    {"form_name_1": "sepsis", "form_name_2": "tratamiento_empirico"},
                    return_all_rows=True,
                )
            ]
            load_fact_relationship_1 = """
                INSERT INTO cdm.fact_relationship (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
                VALUES
                    ((SELECT concept_id FROM cdm.concept WHERE concept_name = 'Condition' AND vocabulary_id = 'Domain'),
                    :condition_id,
                    (SELECT concept_id FROM cdm.concept WHERE concept_name = 'Drug' AND vocabulary_id = 'Domain'),
                    :drug_id,
                    (SELECT concept_id FROM cdm.concept WHERE concept_name = 'Treated with' AND vocabulary_id = 'SNOMED')
                );
            """
            load_fact_relationship_2 = """
                INSERT INTO cdm.fact_relationship (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
                VALUES
                    ((SELECT concept_id FROM cdm.concept WHERE concept_name = 'Drug' AND vocabulary_id = 'Domain'),
                    :drug_id,
                    (SELECT concept_id FROM cdm.concept WHERE concept_name = 'Condition' AND vocabulary_id = 'Domain'),
                    :condition_id,
                    (SELECT concept_id FROM cdm.concept WHERE concept_name = 'Treats' AND vocabulary_id = 'SNOMED')
                );
            """
            for person_id in valid_patients:
                condition_id = self.execute_query(
                    ETLQueries.get_condition_occurrence_id_from_episode,
                    {"person_id": person_id},
                )
                drug_id = self.execute_query(
                    ETLQueries.get_drug_exposure_id_from_episode,
                    {"person_id": person_id},
                )
                self.execute_query(
                    load_fact_relationship_1,
                    {
                        "condition_id": condition_id,
                        "drug_id": drug_id,
                    },
                )
                self.execute_query(
                    load_fact_relationship_2,
                    {
                        "drug_id": drug_id,
                        "condition_id": condition_id,
                    },
                )
        elif relationship_type == "previous_infection":
            load_fact_relationship_1 = """
                INSERT INTO cdm.fact_relationship (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
                VALUES
                    ((SELECT concept_id FROM cdm.concept WHERE concept_name = 'Condition' AND vocabulary_id = 'Domain'),
                    :condition_id,
                    (SELECT concept_id FROM cdm.concept WHERE concept_name = 'Observation' AND vocabulary_id = 'Domain'),
                    :organism_id,
                    (SELECT relationship_concept_id FROM cdm.relationship WHERE relationship_name = 'Has causative agent (SNOMED)')
                );
            """
            load_fact_relationship_2 = """
                INSERT INTO cdm.fact_relationship (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
                VALUES
                    ((SELECT concept_id FROM cdm.concept WHERE concept_name = 'Observation' AND vocabulary_id = 'Domain'),
                    :organism_id,
                    (SELECT concept_id FROM cdm.concept WHERE concept_name = 'Condition' AND vocabulary_id = 'Domain'),
                    :condition_id,
                    (SELECT relationship_concept_id FROM cdm.relationship WHERE relationship_name = 'Causative agent of (SNOMED)')
                );
            """
            self.execute_query(
                load_fact_relationship_1,
                {
                    "condition_id": previous_condition,
                    "organism_id": previous_organism,
                },
            )
            self.execute_query(
                load_fact_relationship_2,
                {
                    "organism_id": previous_organism,
                    "condition_id": previous_condition,
                },
            )
        elif relationship_type == "previous_colonization":
            load_fact_relationship_1 = """
                INSERT INTO cdm.fact_relationship (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
                VALUES
                    ((SELECT concept_id FROM cdm.concept WHERE concept_name = 'Observation' AND vocabulary_id = 'Domain'),
                    :condition_id,
                    (SELECT concept_id FROM cdm.concept WHERE concept_name = 'Observation' AND vocabulary_id = 'Domain'),
                    :organism_id,
                    (SELECT relationship_concept_id FROM cdm.relationship WHERE relationship_name = 'Has causative agent (SNOMED)')
                );
            """
            load_fact_relationship_2 = """
                INSERT INTO cdm.fact_relationship (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
                VALUES
                    ((SELECT concept_id FROM cdm.concept WHERE concept_name = 'Observation' AND vocabulary_id = 'Domain'),
                    :organism_id,
                    (SELECT concept_id FROM cdm.concept WHERE concept_name = 'Observation' AND vocabulary_id = 'Domain'),
                    :condition_id,
                    (SELECT relationship_concept_id FROM cdm.relationship WHERE relationship_name = 'Causative agent of (SNOMED)')
                );
            """
            self.execute_query(
                load_fact_relationship_1,
                {
                    "condition_id": previous_condition,
                    "organism_id": previous_organism,
                },
            )
            self.execute_query(
                load_fact_relationship_2,
                {
                    "organism_id": previous_organism,
                    "condition_id": previous_condition,
                },
            )

    def has_desired_ancestor(self, desc_concept_id, ancestor_concept_id):
        """Check whether a descendant concept has a given ancestor concept in the OMOP hierarchy.

        Args:
            desc_concept_id (int): The concept_id of the descendant (e.g., a microorganism).
            ancestor_concept_id (int): The concept_id of the putative ancestor.

        Returns:
            bool: True if the descendant has the specified ancestor, False otherwise.
        """
        query = """
            WITH RECURSIVE AncestorSearch AS (
                -- Initial selection: directly related ancestors
                SELECT
                    descendant_concept_id,
                    ancestor_concept_id,
                    1 AS depth
                FROM
                    cdm.concept_ancestor
                WHERE
                    descendant_concept_id = 4302157

                UNION ALL

                -- Recursive step: move up the ancestry chain
                SELECT
                    ca.descendant_concept_id,
                    ca.ancestor_concept_id,
                    AncestorSearch.depth + 1
                FROM
                    cdm.concept_ancestor ca
                INNER JOIN AncestorSearch ON ca.descendant_concept_id = AncestorSearch.ancestor_concept_id
                WHERE AncestorSearch.depth < 4
            )
            SELECT
                CASE WHEN EXISTS (
                    SELECT 1
                    FROM AncestorSearch
                    WHERE ancestor_concept_id = 4214811
                )
                THEN 'Yes'
                ELSE 'No'
            END AS HasDesiredAncestor;
        """
        has_desired_ancestor = self.execute_query(
            query,
            {
                "microorganism_concept_id": desc_concept_id,
                "ancestor_concept_id": ancestor_concept_id,
            },
        )
        return True if has_desired_ancestor == "True" else False

    def populate_culture_origin_table(self, culture_source_id, measurement_id):
        """Insert a link between a source culture identifier and its measurement record.

        Args:
            culture_source_id (str): Identifier of the culture in the source system.
            measurement_id (int): OMOP measurement_id corresponding to the culture.

        Returns:
            None
        """
        query = """
            INSERT INTO management.culture_origin(source_culture_id, culture_measurement_id)
            VALUES(:culture_source_id, :measurement_id)
        """
        self.execute_query(
            query,
            {"culture_source_id": culture_source_id, "measurement_id": measurement_id},
        )

    def update_quality_check(self, invalid_data_dict):
        """Mark ETL-tracked forms as invalid based on field-level validation results.

        For each ``source_person_id``, this method updates the ``quality_check`` column
        in ``management.ETL_tracking`` to ``'invalid'`` once per affected form, deduplicating
        by form name within the provided field list.

        Args:
            invalid_data_dict (dict[str, list[str]]): Mapping from ``source_person_id`` to a list
                of strings of the form ``'<form_name>:<field_name>'`` indicating invalid fields.

        Returns:
            None
        """
        for source_person_id, form_field_list in invalid_data_dict.items():
            # Keep track of the form names processed for this source_person_id
            processed_form_names = set()

            # Iterate over the form fields in the list
            for form_field in form_field_list:
                # Split the form_field by ":" to get the form_name
                form_name = form_field.split(":")[0]

                # Only send the query if the form_name has not been processed yet
                if form_name not in processed_form_names:
                    # SQL query for updating the quality_check column
                    query = """
                    UPDATE management.ETL_tracking
                    SET quality_check = 'invalid'
                    WHERE source_person_id = :source_person_id
                    AND form_name = :form_name
                    """

                    # Parameters for the query
                    params = {
                        "source_person_id": source_person_id,
                        "form_name": form_name,
                    }

                    # Execute the query using the execute_query method
                    self.execute_query(query, params)

                    # Mark this form_name as processed
                    processed_form_names.add(form_name)
