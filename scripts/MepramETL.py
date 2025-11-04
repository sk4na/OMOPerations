import re
import pandas as pd
import logging
import traceback
from datetime import timedelta, datetime
import sys

from DatabaseHandler import OMOPDatabaseHandler
import RosettaParser
import REDCapDataExporter

from FilterRedCap import REDCapDataFilter

from config import DB_MEPRAM_PRIVILEGED
from Queries import ETLQueries

# Create a timestamped filename for the log
current_date = datetime.now().strftime("%m-%d-%Y_%H-%M")
log_filename = f"MepramETL_log_{current_date}.log"
logger = logging.getLogger("ETL")
logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def print_banner():
    banner = """
                   oooo        ooooo                                                               oooooooooooo ooooooooooooo ooooo        
                   `888.       .888'                                                               `888'      `8 8'  888   `8 `888'        
                    888b     d'888   .ooooo.   oo.ooooo.  oooo d8b  .oooo.   ooo. .oo.  .oo.        888              888       888         
                    8 Y88. .P  888  d88' `88b  888' `88b `888""8P `P  )88b  `888P"Y88bP"Y88b        888oooo8         888       888         
                    8  `888'   888  888ooo888  888   888  888      .oP"888   888   888   888        888    "         888       888         
                    8    Y     888  888    .o  888   888  888     d8(  888   888   888   888        888       o      888       888       o 
                   o8o        o888o `Y8bod8P'  888bod8P' d888b    `Y888""8o o888o o888o o888o      o888ooooood8     o888o     o888ooooood8 
                                                888                                                                                         
                                               o888o                                                                                        
                                                                                                                                                                                                                                                                                
                                                                                                                                            
                    8888888      8888888      8888888      8888888      8888888      8888888      8888888 8888888 8888888 8888888 8888888   
                                                                                                                                            
        """
    print(banner)


class OMOPLoader:
    """Class that handles the processing and uploading of the RedCap data to the OMOP database. Organized into separate
    methods for each RedCAP form"""

    def __init__(self, db_credentials, rosetta_file, atc2rxnorm_dict):
        try:
            self.db_handler = OMOPDatabaseHandler(db_credentials)
            self.rosetta_parser = RosettaParser.RosettaParser(rosetta_file)
            self.atc2rxnorm_dict = pd.read_csv(atc2rxnorm_dict, sep=";")
            self.default_values = {
                "microorganismo": 4259632,  # OMOP ConceptID: organism
                "antimicrobiano": 895275007,  # SNOMED-CT: antiinfective agent
            }
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    def signos_insert_measurement_or_condition(
        self,
        person_id,
        element_name,
        element_value,
        fecha_ingreso,
        measurement_query,
        condition_query,
        observation_query,
        element1_processed,
        visit_id,
        element1_name=None,
        element1_value=None,
    ):
        """Function used inside the 'process_form_signos' method as the worker function inside the for loop created to process the variables in pairs. This function processes the different variables from the 'signos' RedCap form, deciding if the data should be inserted
        into the measurement table or the condition table from the OMOP database. Variables are processed in pairs due to their interdependent nature when being mapped to the OMOP database.

        Args:
            person_id (int): The patient's person id inside the OMOP database.
            element_name (string): The name of the variable from RedCap for the 'signos' form.
            element_value (string): The value of the variable.
            fecha_ingreso (date): The date of the patient's arrival to the emergency department.
            measurement_query (str): SQL query used to INSERT the data inside the OMOP measurement table.
            condition_query (str): SQL query used to INSERT the data inside the OMOP condition table.
            element1_processed (bool): Boolean flag used to know if the first element of the variable pair was processed, because it had data, or not because it was empty.
            element1_name (str, optional): Name of the first element of the variable pair. Defaults to None.
        """
        if element_name in [
            "temperatura",
            "frec_cardiaca",
            "frec_respiratoria",
            "tension_arterial",
            "saturacion_o2",
        ]:
            self.db_handler.execute_query(
                measurement_query,
                {
                    "person_id": person_id,
                    "measurement_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "semantic_link"
                    ),
                    "measurement_date": fecha_ingreso,
                    "measurement_type_concept_id": "32809",
                    "operator_concept_id": None,
                    "value_as_number": element_value,
                    "value_as_concept_id": None,
                    "unit_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "units_link"
                    ),
                    "measurement_event_id": None,
                    "meas_event_field_concept_id": None,
                    "visit_occurrence_id": visit_id,
                    "measurement_source_value": element_name,
                    "value_source_value": element_value,
                },
            )
        else:
            if not element1_processed and element_name == "hipotermia_hipertermia":
                self.db_handler.execute_query(
                    measurement_query,
                    {
                        "person_id": person_id,
                        "measurement_concept_id": self.rosetta_parser.get_concept_id(
                            element1_name, "semantic_link"
                        ),
                        "measurement_date": fecha_ingreso,
                        "measurement_type_concept_id": "32809",
                        "operator_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "operator_link", element_value
                        ),
                        "value_as_number": self.rosetta_parser.get_concept_code(
                            element_name, "value_link", element_value
                        ),
                        "value_as_concept_id": None,
                        "unit_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "units_link", element_value
                        ),
                        "measurement_event_id": None,
                        "meas_event_field_concept_id": None,
                        "visit_occurrence_id": visit_id,
                        "measurement_source_value": element_name,
                        "value_source_value": element_value,
                    },
                )
                self.db_handler.execute_query(
                    condition_query,
                    {
                        "person_id": person_id,
                        "condition_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "semantic_link", element_value
                        ),
                        "condition_start_date": fecha_ingreso,
                        "condition_type_concept_id": "32809",
                        "visit_occurrence_id": visit_id,
                        "condition_source_value": element_value,
                    },
                )
            elif element1_processed and pd.isnull(element_value):
                match element1_name:
                    case "temperatura":
                        if int(element1_value) < 36:
                            element_value = "1"
                        elif int(element1_value) > 38:
                            element_value = "2"
                        else:
                            element_value = "0"
                    case "frec_cardiaca":
                        if int(element1_value) > 90:
                            element_value = "1"
                        else:
                            element_value = "0"
                    case "frec_respiratoria":
                        if int(element1_value) >= 22:
                            element_value = "1"
                        else:
                            element_value = "0"
                    case "tension_arterial":
                        if int(element1_value) <= 100:
                            element_value = "1"
                        else:
                            element_value = "0"
                    case "saturacion_o2":
                        if int(element1_value) <= 94:
                            element_value = "1"
                        else:
                            element_value = "0"
                            self.db_handler.execute_query(
                                observation_query,
                                {
                                    "person_id": person_id,
                                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                                        element_name, "semantic_link", element_value
                                    ),
                                    "observation_date": fecha_ingreso,
                                    "observation_type_concept_id": "32809",
                                    "value_as_number": None,
                                    "value_as_concept_id": self.rosetta_parser.get_concept_id(
                                        element_name, "value_link", element_value
                                    ),
                                    "qualifier_concept_id": None,
                                    "value_source_value": element_value,
                                    "observation_event_id": None,
                                    "obs_event_field_concept_id": None,
                                    "visit_occurrence_id": None,
                                    "observation_source_value": element_name,
                                },
                            )
                            return
                self.db_handler.execute_query(
                    condition_query,
                    {
                        "person_id": person_id,
                        "condition_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "semantic_link", element_value
                        ),
                        "condition_start_date": fecha_ingreso,
                        "condition_type_concept_id": "32809",
                        "visit_occurrence_id": visit_id,
                        "condition_source_value": element_value,
                    },
                )
            elif not pd.isnull(element_value):
                if element_name == "hipoxemia" and element_value == "0":
                    self.db_handler.execute_query(
                        observation_query,
                        {
                            "person_id": person_id,
                            "observation_concept_id": self.rosetta_parser.get_concept_id(
                                element_name, "semantic_link", element_value
                            ),
                            "observation_date": fecha_ingreso,
                            "observation_type_concept_id": "32809",
                            "value_as_number": None,
                            "value_as_concept_id": self.rosetta_parser.get_concept_id(
                                element_name, "value_link", element_value
                            ),
                            "value_source_value": element_value,
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                            "visit_occurrence_id": None,
                            "observation_source_value": element_name,
                        },
                    )
                else:
                    self.db_handler.execute_query(
                        condition_query,
                        {
                            "person_id": person_id,
                            "condition_concept_id": self.rosetta_parser.get_concept_id(
                                element_name, "semantic_link", element_value
                            ),
                            "condition_start_date": fecha_ingreso,
                            "condition_type_concept_id": "32809",
                            "visit_occurrence_id": visit_id,
                            "condition_source_value": element_value,
                        },
                    )

    def comorbilidad_insert_history_of_event_or_measurement(
        self,
        element_name,
        element_value,
        person_id,
        load_observation_query,
        load_measurement_query,
        fecha_ingreso,
        hepatopatia_gravedad,
    ):
        """Function used inside the 'process_form_comorbilidad' method. Since the same processing logic can be applied to each variable inside the form,
        a for loop is used inside the method which calls this function as the worker. This function then decides if the variable data should be inserted into the OMOP measurement table
        or the observation table as an history of event concept.

        Args:
            element_name (string): The name of the variable from RedCap for the 'signos' form.
            element_value (string): The value of the variable.
            person_id (int): The patient's person id inside the OMOP database.
            load_observation_query (tr): SQL query used to INSERT the data inside the OMOP observation table.
            load_measurement_query (str): SQL query used to INSERT the data inside the OMOP measurement table.
            fecha_ingreso (date): The date of the patient's arrival to the emergency department.
        """
        if element_name in ["escala_karnofsky", "indice_de_charlson"]:
            self.db_handler.execute_query(
                load_measurement_query,
                {
                    "person_id": person_id,
                    "measurement_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "semantic_link"
                    ),
                    "measurement_date": fecha_ingreso,
                    "measurement_type_concept_id": "32809",
                    "operator_concept_id": None,
                    "value_as_number": element_value,
                    "value_as_concept_id": None,
                    "unit_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "units_link"
                    ),
                    "measurement_event_id": None,
                    "meas_event_field_concept_id": None,
                    "visit_occurrence_id": None,
                    "measurement_source_value": element_name,
                    "value_source_value": element_value,
                },
            )
        elif element_name == "tipo_hepatopatia":
            if hepatopatia_gravedad == "1":
                qualifier = self.db_handler.fetch_OMOP_concept_from_db(
                    "SNOMED", "255604002"
                )
            elif hepatopatia_gravedad == "3":
                qualifier = self.db_handler.fetch_OMOP_concept_from_db(
                    "SNOMED", "371924009"
                )

            self.db_handler.execute_query(
                load_observation_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "semantic_link"
                    ),
                    "observation_date": fecha_ingreso,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "value_link", element_value
                    ),
                    "qualifier_concept_id": qualifier,
                    "value_source_value": element_value,
                    "observation_event_id": None,
                    "obs_event_field_concept_id": None,
                    "visit_occurrence_id": None,
                    "observation_source_value": element_name,
                },
            )
        elif element_value == "0":
            self.db_handler.execute_query(
                load_observation_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "semantic_link", element_value
                    ),
                    "observation_date": fecha_ingreso,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "value_link", element_value
                    ),
                    "qualifier_concept_id": None,
                    "value_source_value": element_value,
                    "observation_event_id": None,
                    "obs_event_field_concept_id": None,
                    "visit_occurrence_id": None,
                    "observation_source_value": element_name,
                },
            )
        else:
            if element_name != "tipo_cancer":
                if element_name == "causa_inmunosupresion":
                    self.db_handler.execute_query(
                        load_observation_query,
                        {
                            "person_id": person_id,
                            "observation_concept_id": self.rosetta_parser.get_concept_id(
                                element_name, "semantic_link"
                            ),
                            "observation_date": fecha_ingreso,
                            "observation_type_concept_id": "32809",
                            "value_as_number": None,
                            "value_as_concept_id": self.rosetta_parser.get_concept_id(
                                element_name, "value_link", element_value
                            ),
                            "qualifier_concept_id": None,
                            "value_source_value": element_value,
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                            "visit_occurrence_id": None,
                            "observation_source_value": element_name,
                        },
                    )
                else:
                    self.db_handler.execute_query(
                        load_observation_query,
                        {
                            "person_id": person_id,
                            "observation_concept_id": self.rosetta_parser.get_concept_id(
                                element_name, "semantic_link", element_value
                            ),
                            "observation_date": fecha_ingreso,
                            "observation_type_concept_id": "32809",
                            "value_as_number": None,
                            "value_as_concept_id": self.rosetta_parser.get_concept_id(
                                element_name, "value_link", element_value
                            ),
                            "qualifier_concept_id": None,
                            "value_source_value": element_value,
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                            "visit_occurrence_id": None,
                            "observation_source_value": element_name,
                        },
                    )
            else:
                self.db_handler.execute_query(
                    load_observation_query,
                    {
                        "person_id": person_id,
                        "observation_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "semantic_link"
                        ),
                        "observation_date": fecha_ingreso,
                        "observation_type_concept_id": "32809",
                        "value_as_number": None,
                        "value_as_concept_id": self.db_handler.fetch_OMOP_concept_from_db(
                            "ICD10CM", element_value
                        ),
                        "qualifier_concept_id": None,
                        "value_source_value": element_value,
                        "observation_event_id": None,
                        "obs_event_field_concept_id": None,
                        "visit_occurrence_id": None,
                        "observation_source_value": element_name,
                    },
                )

    def sepsis_insert_condition_or_measurement(
        self,
        element_name,
        element_value,
        person_id,
        load_condition_query,
        load_measurement_query,
        fecha_ingreso,
        visit_id,
    ):
        """Function used inside the 'process_form_sepsis' method. Since the same processing logic can be applied to each variable inside the form,
        a for loop is used inside the method which calls this function as the worker. This function then decides if the variable data should be inserted into the OMOP measurement table
        or the condition table as an history of event concept.

        Args:
            element_name (string): The name of the variable from RedCap for the 'signos' form.
            element_value (string): The value of the variable.
            person_id (int): The patient's person id inside the OMOP database.
            load_condition_query (tr): SQL query used to INSERT the data inside the OMOP condition table.
            load_measurement_query (str): SQL query used to INSERT the data inside the OMOP measurement table.
            fecha_ingreso (date): The date of the patient's arrival to the emergency department.
        """
        if element_name in [
            "sepsis",
            "shock_septico",
        ]:
            if element_value != "0":
                self.db_handler.execute_query(
                    load_condition_query,
                    {
                        "person_id": person_id,
                        "condition_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "semantic_link", element_value
                        ),
                        "condition_start_date": fecha_ingreso,
                        "condition_type_concept_id": "32809",
                        "visit_occurrence_id": visit_id,
                        "condition_source_value": element_value,
                    },
                )
            else:
                self.db_handler.execute_query(
                    ETLQueries.load_observation_table_query,
                    {
                        "person_id": person_id,
                        "observation_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "semantic_link", element_value
                        ),
                        "observation_date": fecha_ingreso,
                        "observation_type_concept_id": "32809",
                        "value_as_number": None,
                        "value_as_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "value_link", element_value
                        ),
                        "qualifier_concept_id": None,
                        "value_source_value": element_value,
                        "observation_event_id": None,
                        "obs_event_field_concept_id": None,
                        "visit_occurrence_id": None,
                        "observation_source_value": element_name,
                    },
                )
        elif element_name == "estado_mental_alterado":
            self.db_handler.execute_query(
                ETLQueries.load_observation_table_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "semantic_link", element_value
                    ),
                    "observation_date": fecha_ingreso,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "value_link", element_value
                    ),
                    "qualifier_concept_id": None,
                    "value_source_value": element_value,
                    "observation_event_id": None,
                    "obs_event_field_concept_id": None,
                    "visit_occurrence_id": None,
                    "observation_source_value": element_name,
                },
            )
        elif element_name == "foco":
            condition_id = self.db_handler.execute_query(
                ETLQueries.load_condition_foco_query,
                {
                    "person_id": person_id,
                    "condition_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "semantic_link", element_value
                    ),
                    "condition_start_date": fecha_ingreso,
                    "condition_type_concept_id": "32809",
                    "condition_status_concept_id": "32890",
                    "visit_occurrence_id": visit_id,
                    "condition_source_value": element_value,
                },
                return_id=True,
            )
            episode_id = self.db_handler.execute_query(
                ETLQueries.load_episode_table,
                {
                    "person_id": person_id,
                    "episode_concept_id": "32533",
                    "episode_start_date": fecha_ingreso,
                    "episode_type_concept_id": "32809",
                    "episode_object_concept_id": "432250",
                },
                return_id=True,
            )
            self.db_handler.execute_query(
                ETLQueries.load_episode_event_table,
                {
                    "episode_id": episode_id,
                    "event_id": condition_id,
                    "episode_event_field_concept_id": "1147129",
                },
            )

        elif element_name == "lactato_serico":
            self.db_handler.execute_query(
                load_measurement_query,
                {
                    "person_id": person_id,
                    "measurement_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "semantic_link", element_value
                    ),
                    "measurement_date": fecha_ingreso,
                    "measurement_type_concept_id": "32809",
                    "operator_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "operator_link", element_value
                    ),
                    "value_as_number": self.rosetta_parser.get_concept_code(
                        element_name, "value_link", element_value
                    ),
                    "value_as_concept_id": None,
                    "unit_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "units_link", element_value
                    ),
                    "measurement_event_id": None,
                    "meas_event_field_concept_id": None,
                    "visit_occurrence_id": visit_id,
                    "measurement_source_value": element_name,
                    "value_source_value": element_value,
                },
            )

        elif element_name == "vasopresores":
            if element_value != "0":
                self.db_handler.execute_query(
                    ETLQueries.load_procedure_occurrence_table,
                    {
                        "person_id": person_id,
                        "procedure_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "semantic_link", element_value
                        ),
                        "procedure_date": fecha_ingreso,
                        "procedure_type_concept_id": "32809",
                        "visit_occurrence_id": visit_id,
                        "procedure_source_value": element_value,
                    },
                )
            else:
                self.db_handler.execute_query(
                    ETLQueries.load_observation_table_query,
                    {
                        "person_id": person_id,
                        "observation_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "semantic_link", element_value
                        ),
                        "observation_date": fecha_ingreso,
                        "observation_type_concept_id": "32809",
                        "value_as_number": None,
                        "value_as_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "value_link", element_value
                        ),
                        "qualifier_concept_id": None,
                        "value_source_value": element_value,
                        "observation_event_id": None,
                        "obs_event_field_concept_id": None,
                        "visit_occurrence_id": None,
                        "observation_source_value": element_name,
                    },
                )
        else:
            self.db_handler.execute_query(
                load_measurement_query,
                {
                    "person_id": person_id,
                    "measurement_concept_id": self.rosetta_parser.get_concept_id(
                        element_name,
                        "semantic_link",
                    ),
                    "measurement_date": fecha_ingreso,
                    "measurement_type_concept_id": "32809",
                    "operator_concept_id": None,
                    "value_as_number": element_value,
                    "value_as_concept_id": None,
                    "unit_concept_id": self.rosetta_parser.get_concept_id(
                        element_name, "units_link"
                    ),
                    "measurement_event_id": None,
                    "meas_event_field_concept_id": None,
                    "visit_occurrence_id": visit_id,
                    "measurement_source_value": element_name,
                    "value_source_value": element_value,
                },
            )

    def get_rxnorm_from_atc(self, atc_code):
        """Function that returns the corresponding RxNorm concept for a given ATC code.

        Args:
            atc_code (string): ATC code that we want to translate to RxNorm

        Raises:
            ValueError: No matching RxNorm concept for the desired atc_code

        Returns:
            string: RxNorm code
        """
        df = self.atc2rxnorm_dict[(self.atc2rxnorm_dict["atc_code"] == atc_code)]
        if not df.empty:
            return str(df["conceptId"].iloc[0])
        else:
            error_msg = f"No matching RxNorm concept for ATC code: {atc_code}."
            logger.error(error_msg)
            raise ValueError(error_msg)

    def process_form_paciente(self, data_row, person_source_value, person_id):
        """Method that takes as an input a RedCap record from the form 'paciente' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        care_site_source_value = person_source_value.split("-")[0]
        year_of_birth = data_row.fecha_nacimiento.split("-")[0]
        care_site_id = self.db_handler.execute_query(
            ETLQueries.get_care_site_id_query,
            {"care_site_source_value": care_site_source_value},
        )

        self.db_handler.execute_query(
            ETLQueries.load_person_table_query,
            {
                "person_id": person_id,
                "gender_concept_id": self.rosetta_parser.get_concept_id(
                    "sexo",
                    "semantic_link",
                    data_row.sexo,
                ),
                "year_of_birth": year_of_birth,
                "birth_datetime": data_row.fecha_nacimiento,
                "care_site_id": care_site_id,
                "person_source_value": person_source_value,
                "gender_source_value": data_row.sexo,
            },
        )
        visit_id = self.db_handler.load_visit_ocurrence_table(
            person_id,
            "9203",
            data_row.fecha_ingreso_urgencias,
            data_row.fecha_ingreso_urgencias,
            "32809",
        )
        self.db_handler.execute_query(
            ETLQueries.load_observation_table_query,
            {
                "person_id": person_id,
                "observation_concept_id": self.rosetta_parser.get_concept_id(
                    "fecha_ingreso_urgencias", "semantic_link"
                ),
                "observation_date": data_row.fecha_ingreso_urgencias,
                "observation_type_concept_id": "32809",
                "value_as_number": None,
                "value_as_concept_id": None,
                "value_source_value": data_row.fecha_ingreso_urgencias,
                "observation_event_id": None,
                "obs_event_field_concept_id": None,
                "visit_occurrence_id": visit_id,
                "observation_source_value": None,
            },
        )
        if pd.notnull(data_row.codigo_postal):
            self.db_handler.execute_query(
                ETLQueries.load_observation_table_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        "codigo_postal",
                        "semantic_link",
                    ),
                    "observation_date": data_row.fecha_ingreso_urgencias,
                    "observation_type_concept_id": "32809",
                    "value_as_number": data_row.codigo_postal,
                    "value_as_concept_id": None,
                    "value_source_value": data_row.codigo_postal,
                    "observation_event_id": None,
                    "obs_event_field_concept_id": None,
                    "visit_occurrence_id": visit_id,
                    "observation_source_value": None,
                },
            )
        if pd.notnull(data_row.paciente_residencia):
            self.db_handler.execute_query(
                ETLQueries.load_observation_table_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        "paciente_residencia",
                        "semantic_link",
                    ),
                    "observation_date": data_row.fecha_ingreso_urgencias,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": self.rosetta_parser.get_concept_id(
                        "paciente_residencia",
                        "value_link",
                        data_row.paciente_residencia,
                    ),
                    "value_source_value": data_row.paciente_residencia,
                    "observation_event_id": None,
                    "obs_event_field_concept_id": None,
                    "visit_occurrence_id": visit_id,
                    "observation_source_value": None,
                },
            )
        if pd.notnull(data_row.mujer_gestante):
            self.db_handler.execute_query(
                ETLQueries.load_observation_table_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        "mujer_gestante", "semantic_link"
                    ),
                    "observation_date": data_row.fecha_ingreso_urgencias,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": self.rosetta_parser.get_concept_id(
                        "mujer_gestante",
                        "value_link",
                        data_row.mujer_gestante,
                    ),
                    "value_source_value": data_row.mujer_gestante,
                    "observation_event_id": None,
                    "obs_event_field_concept_id": None,
                    "visit_occurrence_id": visit_id,
                    "observation_source_value": None,
                },
            )
        self.db_handler.load_observation_period_table(
            person_id,
            data_row.fecha_nacimiento,
            data_row.fecha_ingreso_urgencias,
            "32809",
        )

    def process_form_sintomas(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'sintomas' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        fecha_ingreso_concept_id = self.rosetta_parser.get_concept_id(
            "fecha_ingreso_urgencias",
            "semantic_link",
        )
        sintoma_concept_id = self.rosetta_parser.get_concept_id(
            "sintoma", "semantic_link", data_row.sintoma
        )
        fecha_ingreso = self.db_handler.execute_query(
            ETLQueries.get_fecha_ingreso_query,
            {
                "person_id": person_id,
                "fecha_ingreso_concept_id": fecha_ingreso_concept_id,
            },
        )

        duracion_sintoma = (
            int(data_row.duracion_sintoma)
            if not pd.isnull(data_row.duracion_sintoma)
            else 0
        )

        fecha_evento_sintoma = fecha_ingreso - timedelta(duracion_sintoma)
        visit_id = self.db_handler.execute_query(
            ETLQueries.get_visit_occurrence_query, {"person_id": person_id}
        )
        self.db_handler.execute_query(
            ETLQueries.load_condition_table_query,
            {
                "person_id": person_id,
                "condition_concept_id": sintoma_concept_id,
                "condition_start_date": fecha_evento_sintoma,
                "condition_type_concept_id": "32809",
                "visit_occurrence_id": visit_id,
                "condition_source_value": data_row.sintoma,
            },
        )

    def process_form_signos(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'signos' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        fecha_ingreso = self.db_handler.execute_query(
            ETLQueries.get_fecha_ingreso_query,
            {
                "person_id": person_id,
                "fecha_ingreso_concept_id": self.rosetta_parser.get_concept_id(
                    "fecha_ingreso_urgencias", "semantic_link"
                ),
            },
        )
        visit_id = self.db_handler.execute_query(
            ETLQueries.get_visit_occurrence_query, {"person_id": person_id}
        )
        column_names = data_row._fields[1:]  # Skip 'record_id'
        data_row = data_row._asdict()

        # Iterate over pairs
        for i in range(0, len(column_names), 2):
            element1_name = column_names[i]
            element1_value = data_row[element1_name]
            element2_name = column_names[i + 1]
            element2_value = data_row[element2_name]

            element1_processed = False
            # Process the first element if it's not null
            if not (pd.isnull(element1_value) and pd.isnull(element2_value)):
                if pd.notnull(element1_value):
                    element1_processed = True
                    self.signos_insert_measurement_or_condition(
                        person_id,
                        element1_name,
                        element1_value,
                        fecha_ingreso,
                        ETLQueries.load_measurement_table_query,
                        ETLQueries.load_condition_table_query,
                        ETLQueries.load_observation_table_query,
                        element1_processed,
                        visit_id,
                    )

                self.signos_insert_measurement_or_condition(
                    person_id,
                    element2_name,
                    element2_value,
                    fecha_ingreso,
                    ETLQueries.load_measurement_table_query,
                    ETLQueries.load_condition_table_query,
                    ETLQueries.load_observation_table_query,
                    element1_processed,
                    visit_id,
                    element1_name,
                    element1_value,
                )

    def process_form_comorbilidad(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'comorbilidad' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        fecha_ingreso = self.db_handler.execute_query(
            ETLQueries.get_fecha_ingreso_query,
            {
                "person_id": person_id,
                "fecha_ingreso_concept_id": self.rosetta_parser.get_concept_id(
                    "fecha_ingreso_urgencias", "semantic_link"
                ),
            },
        )
        column_names = data_row._fields[1:]  # Skip 'record_id'
        data_row = data_row._asdict()
        hepatopatia_gravedad = None

        # Iterate over element inside form
        for i in range(len(column_names)):
            element_name = column_names[i]
            element_value = data_row[element_name]
            if not pd.isnull(element_value):
                if element_name == "hepatopatia":
                    hepatopatia_gravedad = element_value

                self.comorbilidad_insert_history_of_event_or_measurement(
                    element_name,
                    element_value,
                    person_id,
                    ETLQueries.load_observation_table_query,
                    ETLQueries.load_measurement_table_query,
                    fecha_ingreso,
                    hepatopatia_gravedad,
                )

    def process_form_sepsis(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'sepsis' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        fecha_ingreso = self.db_handler.execute_query(
            ETLQueries.get_fecha_ingreso_query,
            {
                "person_id": person_id,
                "fecha_ingreso_concept_id": self.rosetta_parser.get_concept_id(
                    "fecha_ingreso_urgencias", "semantic_link"
                ),
            },
        )
        visit_id = self.db_handler.execute_query(
            ETLQueries.get_visit_occurrence_query, {"person_id": person_id}
        )
        column_names = data_row._fields[1:]  # Skip 'record_id'
        data_row = data_row._asdict()

        # Iterate over element inside form
        for i in range(len(column_names)):
            element_name = column_names[i]
            element_value = data_row[element_name]

            if element_name == "foco" and pd.isnull(element_value):
                element_value = "12"

            if not pd.isnull(element_value):
                self.sepsis_insert_condition_or_measurement(
                    element_name,
                    element_value,
                    person_id,
                    ETLQueries.load_condition_table_query,
                    ETLQueries.load_measurement_table_query,
                    fecha_ingreso,
                    visit_id,
                )

    def process_form_factores_riesgo(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'factores_riesgo' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        fecha_ingreso = self.db_handler.execute_query(
            ETLQueries.get_fecha_ingreso_query,
            {
                "person_id": person_id,
                "fecha_ingreso_concept_id": self.rosetta_parser.get_concept_id(
                    "fecha_ingreso_urgencias", "semantic_link"
                ),
            },
        )
        column_names = data_row._fields[1:]  # Skip 'record_id'
        data_row = data_row._asdict()

        # Iterate over element inside form
        for i in range(len(column_names)):
            element_name = column_names[i]
            element_value = data_row[element_name]
            if not pd.isnull(element_value):
                self.db_handler.execute_query(
                    ETLQueries.load_observation_table_query,
                    {
                        "person_id": person_id,
                        "observation_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "semantic_link", element_value
                        ),
                        "observation_date": fecha_ingreso,
                        "observation_type_concept_id": "32809",
                        "value_as_number": None,
                        "value_as_concept_id": self.rosetta_parser.get_concept_id(
                            element_name, "value_link", element_value
                        ),
                        "value_source_value": element_value,
                        "observation_event_id": None,
                        "obs_event_field_concept_id": None,
                        "visit_occurrence_id": None,
                        "observation_source_value": element_name,
                    },
                )

    def process_form_infecciones_previas(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'infecciones_previas' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        fecha_ingreso_concept_id = self.rosetta_parser.get_concept_id(
            "fecha_ingreso_urgencias",
            "semantic_link",
        )

        fecha_ingreso = self.db_handler.execute_query(
            ETLQueries.get_fecha_ingreso_query,
            {
                "person_id": person_id,
                "fecha_ingreso_concept_id": fecha_ingreso_concept_id,
            },
        )
        if pd.isnull(data_row.fecha_infeccion):
            fecha_infeccion_default = fecha_ingreso - timedelta(days=365)
            fecha_infeccion = fecha_infeccion_default
        else:
            fecha_infeccion = data_row.fecha_infeccion

        condition_id = self.db_handler.execute_query(
            ETLQueries.load_condition_table_query,
            {
                "person_id": person_id,
                "condition_concept_id": self.rosetta_parser.get_concept_id(
                    "sindrome_infeccioso",
                    "semantic_link",
                    (
                        data_row.sindrome_infeccioso
                        if not pd.isnull(data_row.sindrome_infeccioso)
                        else "13"
                    ),
                ),
                "condition_start_date": fecha_infeccion,
                "condition_type_concept_id": "32809",
                "visit_occurrence_id": None,
                "condition_source_value": data_row.sindrome_infeccioso,
            },
            return_id=True,
        )
        specimen_id = self.db_handler.execute_query(
            ETLQueries.load_specimen_table_query,
            {
                "person_id": person_id,
                "specimen_concept_id": self.rosetta_parser.get_concept_id(
                    "especimen_infecciones_previas",
                    "semantic_link",
                ),
                "specimen_date": fecha_infeccion,
                "specimen_type_concept_id": "32809",
            },
            return_id=True,
        )
        culture_id = self.db_handler.execute_query(
            ETLQueries.load_measurement_table_query,
            {
                "person_id": person_id,
                "measurement_concept_id": self.rosetta_parser.get_concept_id(
                    "cultivo_infecciones_previas", "semantic_link"
                ),
                "measurement_date": fecha_infeccion,
                "measurement_type_concept_id": "32809",
                "operator_concept_id": None,
                "value_as_number": None,
                "value_as_concept_id": self.rosetta_parser.get_concept_id(
                    "cultivo_infecciones_previas", "value_link"
                ),
                "unit_concept_id": None,
                "measurement_event_id": specimen_id,
                "meas_event_field_concept_id": "1147051",
                "visit_occurrence_id": None,
                "measurement_source_value": "cultivo_infecciones_previas",
                "value_source_value": data_row.cultivo_infecciones_previas,
            },
            return_id=True,
        )
        moo_concept_id = (
            (
                self.db_handler.fetch_OMOP_concept_from_db(
                    "SNOMED", data_row.microorganism_infec_prev
                ),
            )
            if not pd.isnull(data_row.microorganism_infec_prev)
            else self.default_values["microorganismo"]
        )

        moo_in_culture_id = self.db_handler.execute_query(
            ETLQueries.load_observation_table_query,
            {
                "person_id": person_id,
                "observation_concept_id": moo_concept_id,
                "observation_date": fecha_infeccion,
                "observation_type_concept_id": "32809",
                "value_as_number": None,
                "value_as_concept_id": None,
                "value_source_value": data_row.microorganism_infec_prev,
                "observation_event_id": culture_id,
                "obs_event_field_concept_id": "1147140",
                "visit_occurrence_id": None,
                "observation_source_value": None,
            },
            return_id=True,
        )
        if (
            not pd.isnull(data_row.bmr_infec_previa)
            and data_row.bmr_infec_previa == "1"
        ):
            bmr_id = self.db_handler.execute_query(
                ETLQueries.load_observation_table_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        "bmr_infec_previa",
                        "semantic_link",
                        data_row.bmr_infec_previa,
                    ),
                    "observation_date": fecha_infeccion,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": None,
                    "value_source_value": data_row.bmr_infec_previa,
                    "observation_event_id": moo_in_culture_id,
                    "obs_event_field_concept_id": "1147167",
                    "visit_occurrence_id": None,
                    "observation_source_value": None,
                },
                return_id=True,
            )

            feno_resist_values = [
                re.search(r"___(\d+)$", field).group(1)
                for field, value in data_row._asdict().items()
                if field.startswith("feno") and value == "1"
            ]
            for feno_resist_value in feno_resist_values:
                if feno_resist_value == "1":
                    if self.db_handler.has_desired_ancestor(moo_concept_id, "4214811"):
                        feno_concept_id = "3009403"
                    else:
                        feno_concept_id = self.rosetta_parser.get_concept_id(
                            "feno_resist_infec_prev", "semantic_link", feno_resist_value
                        )
                else:
                    feno_concept_id = self.rosetta_parser.get_concept_id(
                        "feno_resist_infec_prev", "semantic_link", feno_resist_value
                    )
                self.db_handler.execute_query(
                    ETLQueries.load_measurement_table_query,
                    {
                        "person_id": person_id,
                        "measurement_concept_id": feno_concept_id,
                        "measurement_date": fecha_infeccion,
                        "measurement_type_concept_id": "32809",
                        "operator_concept_id": None,
                        "value_as_number": None,
                        "value_as_concept_id": self.rosetta_parser.get_concept_id(
                            "feno_resist_infec_prev", "value_link", feno_resist_value
                        ),
                        "unit_concept_id": None,
                        "measurement_event_id": bmr_id,
                        "meas_event_field_concept_id": "1147167",
                        "visit_occurrence_id": None,
                        "measurement_source_value": "feno_resist_infec_prev",
                        "value_source_value": feno_resist_value,
                    },
                )

        self.db_handler.populate_fact_relationship_table(
            relationship_type="previous_infection",
            previous_condition=condition_id,
            previous_organism=moo_in_culture_id,
        )

    def process_form_colonizaciones_previas(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'colonizaciones_previas' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        fecha_ingreso_concept_id = self.rosetta_parser.get_concept_id(
            "fecha_ingreso_urgencias",
            "semantic_link",
        )

        fecha_ingreso = self.db_handler.execute_query(
            ETLQueries.get_fecha_ingreso_query,
            {
                "person_id": person_id,
                "fecha_ingreso_concept_id": fecha_ingreso_concept_id,
            },
        )
        if pd.isnull(data_row.fecha_colonizacion):
            fecha_colonizacion_default = fecha_ingreso - timedelta(days=365)
            fecha_colonizacion = fecha_colonizacion_default
        else:
            fecha_colonizacion = data_row.fecha_colonizacion
        condition_id = self.db_handler.execute_query(
            ETLQueries.load_observation_table_query,
            {
                "person_id": person_id,
                "observation_concept_id": self.rosetta_parser.get_concept_id(
                    "portador_agente_infeccioso",
                    "semantic_link",
                ),
                "observation_date": fecha_colonizacion,
                "observation_type_concept_id": "32809",
                "value_as_number": None,
                "value_as_concept_id": None,
                "visit_occurrence_id": None,
                "value_source_value": "portador_agente_infeccioso",
                "observation_event_id": None,
                "obs_event_field_concept_id": None,
                "visit_occurrence_id": None,
                "observation_source_value": None,
            },
            return_id=True,
        )
        specimen_id = self.db_handler.execute_query(
            ETLQueries.load_specimen_table_query,
            {
                "person_id": person_id,
                "specimen_concept_id": self.rosetta_parser.get_concept_id(
                    "especimen_coloniza_previas",
                    "semantic_link",
                ),
                "specimen_date": fecha_colonizacion,
                "specimen_type_concept_id": "32809",
            },
            return_id=True,
        )
        culture_id = self.db_handler.execute_query(
            ETLQueries.load_measurement_table_query,
            {
                "person_id": person_id,
                "measurement_concept_id": self.rosetta_parser.get_concept_id(
                    "cultivo_colonizaciones_previas", "semantic_link"
                ),
                "measurement_date": fecha_colonizacion,
                "measurement_type_concept_id": "32809",
                "operator_concept_id": None,
                "value_as_number": None,
                "value_as_concept_id": self.rosetta_parser.get_concept_id(
                    "cultivo_colonizaciones_previas", "value_link"
                ),
                "unit_concept_id": None,
                "measurement_event_id": specimen_id,
                "meas_event_field_concept_id": "1147051",
                "visit_occurrence_id": None,
                "measurement_source_value": "cultivo_colonizaciones_previas",
                "value_source_value": data_row.cultivo_colonizaciones_previas,
            },
            return_id=True,
        )
        moo_concept_id = (
            (
                self.db_handler.fetch_OMOP_concept_from_db(
                    "SNOMED", data_row.microorganismo_colonizador
                ),
            )
            if not pd.isnull(data_row.microorganismo_colonizador)
            else self.default_values["microorganismo"]
        )
        moo_in_culture_id = self.db_handler.execute_query(
            ETLQueries.load_observation_table_query,
            {
                "person_id": person_id,
                "observation_concept_id": moo_concept_id,
                "observation_date": fecha_colonizacion,
                "observation_type_concept_id": "32809",
                "value_as_number": None,
                "value_as_concept_id": None,
                "value_source_value": data_row.microorganismo_colonizador,
                "observation_event_id": culture_id,
                "obs_event_field_concept_id": "1147140",
                "visit_occurrence_id": None,
                "observation_source_value": None,
            },
            return_id=True,
        )
        if not pd.isnull(data_row.bmr_colonizador) and data_row.bmr_colonizador == "1":
            bmr_id = self.db_handler.execute_query(
                ETLQueries.load_observation_table_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        "bmr_colonizador",
                        "semantic_link",
                        data_row.bmr_colonizador,
                    ),
                    "observation_date": fecha_colonizacion,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": None,
                    "value_source_value": data_row.bmr_colonizador,
                    "observation_event_id": moo_in_culture_id,
                    "obs_event_field_concept_id": "1147167",
                    "visit_occurrence_id": None,
                    "observation_source_value": None,
                },
                return_id=True,
            )

            feno_resist_values = [
                re.search(r"___(\d+)$", field).group(1)
                for field, value in data_row._asdict().items()
                if field.startswith("feno") and value == "1"
            ]
            for feno_resist_value in feno_resist_values:
                if feno_resist_value == "1":
                    if self.db_handler.has_desired_ancestor(moo_concept_id, "4214811"):
                        feno_concept_id = "3009403"
                    else:
                        feno_concept_id = self.rosetta_parser.get_concept_id(
                            "fenotipo_resist_colo", "semantic_link", feno_resist_value
                        )
                else:
                    feno_concept_id = self.rosetta_parser.get_concept_id(
                        "fenotipo_resist_colo", "semantic_link", feno_resist_value
                    )
                self.db_handler.execute_query(
                    ETLQueries.load_measurement_table_query,
                    {
                        "person_id": person_id,
                        "measurement_concept_id": feno_concept_id,
                        "measurement_date": fecha_colonizacion,
                        "measurement_type_concept_id": "32809",
                        "operator_concept_id": None,
                        "value_as_number": None,
                        "value_as_concept_id": self.rosetta_parser.get_concept_id(
                            "fenotipo_resist_colo", "value_link", feno_resist_value
                        ),
                        "unit_concept_id": None,
                        "measurement_event_id": bmr_id,
                        "meas_event_field_concept_id": "1147167",
                        "visit_occurrence_id": None,
                        "measurement_source_value": "fenotipo_resist_colo",
                        "value_source_value": feno_resist_value,
                    },
                )

        self.db_handler.populate_fact_relationship_table(
            relationship_type="previous_colonization",
            previous_condition=condition_id,
            previous_organism=moo_in_culture_id,
        )

    def process_form_tratamiento_antibiotico_previo(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'antibiotico_previo' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        fecha_ingreso_concept_id = self.rosetta_parser.get_concept_id(
            "fecha_ingreso_urgencias",
            "semantic_link",
        )
        if pd.isnull(data_row.fecha_administracion_antib):
            fecha_ingreso = self.db_handler.execute_query(
                ETLQueries.get_fecha_ingreso_query,
                {
                    "person_id": person_id,
                    "fecha_ingreso_concept_id": fecha_ingreso_concept_id,
                },
            )
            fecha_tratamiento_default = fecha_ingreso - timedelta(days=90)
            fecha_tratamiento = fecha_tratamiento_default
        else:
            fecha_tratamiento = datetime.strptime(
                data_row.fecha_administracion_antib, "%Y-%m-%d"
            ).date()

        if not pd.isnull(data_row.dias_trat_antimicrobiano):
            fecha_finalizacion_tratamiento = fecha_tratamiento + timedelta(
                int(data_row.dias_trat_antimicrobiano) - 1
            )
        else:
            fecha_finalizacion_tratamiento = fecha_tratamiento + timedelta(29)

        self.db_handler.execute_query(
            ETLQueries.query_drug_exposure_table,
            {
                "person_id": person_id,
                "drug_concept_id": (
                    self.get_rxnorm_from_atc(data_row.antimicrobiano_previo)
                    if not pd.isnull(data_row.antimicrobiano_previo)
                    else self.db_handler.fetch_OMOP_concept_from_db(
                        "SNOMED", self.default_values["antimicrobiano"]
                    )
                ),
                "drug_exposure_start_date": (
                    data_row.fecha_administracion_antib
                    if not pd.isnull(data_row.fecha_administracion_antib)
                    else fecha_tratamiento_default
                ),
                "drug_exposure_end_date": fecha_finalizacion_tratamiento,
                "drug_type_concept_id": "32809",
                "visit_occurrence_id": None,
                "drug_source_value": data_row.antimicrobiano_previo,
            },
        )

    def process_form_tratamiento_empirico(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'tratamiento_empirico' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        visit_id = self.db_handler.execute_query(
            ETLQueries.get_visit_occurrence_query, {"person_id": person_id}
        )
        fecha_ingreso_concept_id = self.rosetta_parser.get_concept_id(
            "fecha_ingreso_urgencias",
            "semantic_link",
        )
        episode_id = self.db_handler.execute_query(
            ETLQueries.get_episode_id, {"person_id": person_id}
        )
        fecha_ingreso = self.db_handler.execute_query(
            ETLQueries.get_fecha_ingreso_query,
            {
                "person_id": person_id,
                "fecha_ingreso_concept_id": fecha_ingreso_concept_id,
            },
        )

        atb_id = self.db_handler.execute_query(
            ETLQueries.query_drug_exposure_table,
            {
                "person_id": person_id,
                "drug_concept_id": (
                    self.get_rxnorm_from_atc(data_row.antimicrobiano_empirico)
                    if not pd.isnull(data_row.antimicrobiano_empirico)
                    else self.db_handler.fetch_OMOP_concept_from_db(
                        "SNOMED", self.default_values["antimicrobiano"]
                    )
                ),
                "drug_exposure_start_date": fecha_ingreso,
                "drug_exposure_end_date": fecha_ingreso,
                "drug_type_concept_id": "32809",
                "visit_occurrence_id": visit_id,
                "drug_source_value": data_row.antimicrobiano_empirico,
            },
            return_id=True,
        )

        self.db_handler.execute_query(
            ETLQueries.load_episode_event_table,
            {
                "episode_id": episode_id,
                "event_id": atb_id,
                "episode_event_field_concept_id": "1147096",
            },
        )

    def process_form_hemocultivo(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'hemocultivo' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        episode_id = self.db_handler.execute_query(
            ETLQueries.get_episode_id, {"person_id": person_id}
        )
        visit_id = self.db_handler.execute_query(
            ETLQueries.get_visit_occurrence_query, {"person_id": person_id}
        )
        specimen_id = self.db_handler.execute_query(
            ETLQueries.load_specimen_table_query,
            {
                "person_id": person_id,
                "specimen_concept_id": self.rosetta_parser.get_concept_id(
                    "especimen_hemocultivo_urg",
                    "semantic_link",
                ),
                "specimen_date": data_row.fecha_hemocultivo,
                "specimen_type_concept_id": "32809",
            },
            return_id=True,
        )
        self.db_handler.execute_query(
            ETLQueries.load_episode_event_table,
            {
                "episode_id": episode_id,
                "event_id": specimen_id,
                "episode_event_field_concept_id": "1147051",
            },
        )

        culture_id = self.db_handler.execute_query(
            ETLQueries.load_measurement_table_query,
            {
                "person_id": person_id,
                "measurement_concept_id": self.rosetta_parser.get_concept_id(
                    "hemo_positivo_si_no", "semantic_link"
                ),
                "measurement_date": data_row.fecha_hemocultivo,
                "measurement_type_concept_id": "32809",
                "operator_concept_id": None,
                "value_as_number": None,
                "value_as_concept_id": self.rosetta_parser.get_concept_id(
                    "hemo_positivo_si_no",
                    "value_link",
                    data_row.hemo_positivo_si_no,
                ),
                "unit_concept_id": None,
                "measurement_event_id": specimen_id,
                "meas_event_field_concept_id": "1147051",
                "visit_occurrence_id": visit_id,
                "measurement_source_value": "hemo_positivo_si_no",
                "value_source_value": data_row.hemo_positivo_si_no,
            },
            return_id=True,
        )
        self.db_handler.populate_culture_origin_table(
            data_row.id_hemocultivo, culture_id
        )  # almacenar en tabla culture_origin el id del cultivo junto con el measurement ID de ese cultivo
        self.db_handler.execute_query(
            ETLQueries.load_episode_event_table,
            {
                "episode_id": episode_id,
                "event_id": culture_id,
                "episode_event_field_concept_id": "1147140",
            },
        )
        if data_row.hemo_positivo_si_no == "1":
            moo_concept_id = (
                (
                    self.db_handler.fetch_OMOP_concept_from_db(
                        "SNOMED", data_row.microorganismo
                    ),
                )
                if not pd.isnull(data_row.microorganismo)
                else self.default_values["microorganismo"]
            )

            moo_in_culture_id = self.db_handler.execute_query(
                ETLQueries.load_observation_table_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": moo_concept_id,
                    "observation_date": data_row.fecha_hemocultivo,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": None,
                    "value_source_value": data_row.microorganismo,
                    "observation_event_id": culture_id,
                    "obs_event_field_concept_id": "1147140",
                    "visit_occurrence_id": visit_id,
                    "observation_source_value": None,
                },
                return_id=True,
            )
            self.db_handler.execute_query(
                ETLQueries.load_episode_event_table,
                {
                    "episode_id": episode_id,
                    "event_id": moo_in_culture_id,
                    "episode_event_field_concept_id": "1147167",
                },
            )
            if not pd.isnull(data_row.bmr_etiologia) and data_row.bmr_etiologia == "1":
                bmr_id = self.db_handler.execute_query(
                    ETLQueries.load_observation_table_query,
                    {
                        "person_id": person_id,
                        "observation_concept_id": self.rosetta_parser.get_concept_id(
                            "bmr_etiologia",
                            "semantic_link",
                            data_row.bmr_etiologia,
                        ),
                        "observation_date": data_row.fecha_hemocultivo,
                        "observation_type_concept_id": "32809",
                        "value_as_number": None,
                        "value_as_concept_id": None,
                        "value_source_value": data_row.bmr_etiologia,
                        "observation_event_id": moo_in_culture_id,
                        "obs_event_field_concept_id": "1147167",
                        "visit_occurrence_id": visit_id,
                        "observation_source_value": None,
                    },
                    return_id=True,
                )
                self.db_handler.execute_query(
                    ETLQueries.load_episode_event_table,
                    {
                        "episode_id": episode_id,
                        "event_id": bmr_id,
                        "episode_event_field_concept_id": "1147167",
                    },
                )
                feno_resist_values = [
                    re.search(r"___(\d+)$", field).group(1)
                    for field, value in data_row._asdict().items()
                    if field.startswith("feno") and value == "1"
                ]
                for feno_resist_value in feno_resist_values:
                    if feno_resist_value == "1":
                        if self.db_handler.has_desired_ancestor(
                            moo_concept_id, "4214811"
                        ):
                            feno_concept_id = "3009403"
                        else:
                            feno_concept_id = self.rosetta_parser.get_concept_id(
                                "fenotipo_resistencia",
                                "semantic_link",
                                feno_resist_value,
                            )
                    else:
                        feno_concept_id = self.rosetta_parser.get_concept_id(
                            "fenotipo_resistencia", "semantic_link", feno_resist_value
                        )

                    feno_id = self.db_handler.execute_query(
                        ETLQueries.load_measurement_table_query,
                        {
                            "person_id": person_id,
                            "measurement_concept_id": feno_concept_id,
                            "measurement_date": data_row.fecha_hemocultivo,
                            "measurement_type_concept_id": "32809",
                            "operator_concept_id": None,
                            "value_as_number": None,
                            "value_as_concept_id": self.rosetta_parser.get_concept_id(
                                "fenotipo_resistencia", "value_link", feno_resist_value
                            ),
                            "unit_concept_id": None,
                            "measurement_event_id": bmr_id,
                            "meas_event_field_concept_id": "1147167",
                            "visit_occurrence_id": visit_id,
                            "measurement_source_value": "fenotipo_resistencia",
                            "value_source_value": feno_resist_value,
                        },
                        return_id=True,
                    )
                    self.db_handler.execute_query(
                        ETLQueries.load_episode_event_table,
                        {
                            "episode_id": episode_id,
                            "event_id": feno_id,
                            "episode_event_field_concept_id": "1147140",
                        },
                    )

    def process_form_otros_cultivos(self, data_row, person_id):
        """Method that takes as an input a RedCap record from the form 'otros_cultivos' and processes it, using the rosetta file, and uploads each variable data into the appropiate
        OMOP database table.

        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
            person_source_value (str): The patient's identifyer used in the source data. For example, this could be a RedCap record id.
            person_id (int): The OMOP person id for a given patient
        """
        visit_id = self.db_handler.execute_query(
            ETLQueries.get_visit_occurrence_query, {"person_id": person_id}
        )
        episode_id = self.db_handler.execute_query(
            ETLQueries.get_episode_id, {"person_id": person_id}
        )
        specimen_id = self.db_handler.execute_query(
            ETLQueries.load_specimen_table_query,
            {
                "person_id": person_id,
                "specimen_concept_id": self.rosetta_parser.get_concept_id(
                    "especimen_otros_cultivos_urg",
                    "semantic_link",
                    data_row.tipo_cultivo,
                ),
                "specimen_date": data_row.fecha_otros_cultivos,
                "specimen_type_concept_id": "32809",
            },
            return_id=True,
        )
        self.db_handler.execute_query(
            ETLQueries.load_episode_event_table,
            {
                "episode_id": episode_id,
                "event_id": specimen_id,
                "episode_event_field_concept_id": "1147051",
            },
        )
        culture_id = self.db_handler.execute_query(
            ETLQueries.load_measurement_table_query,
            {
                "person_id": person_id,
                "measurement_concept_id": self.rosetta_parser.get_concept_id(
                    "tipo_cultivo", "semantic_link", data_row.tipo_cultivo
                ),
                "measurement_date": data_row.fecha_otros_cultivos,
                "measurement_type_concept_id": "32809",
                "operator_concept_id": None,
                "value_as_number": None,
                "value_as_concept_id": self.rosetta_parser.get_concept_id(
                    "tipo_cultivo",
                    "value_link",
                ),
                "unit_concept_id": None,
                "measurement_event_id": specimen_id,
                "meas_event_field_concept_id": "1147051",
                "visit_occurrence_id": visit_id,
                "measurement_source_value": "tipo_cultivo",
                "value_source_value": data_row.tipo_cultivo,
            },
            return_id=True,
        )
        self.db_handler.execute_query(
            ETLQueries.load_episode_event_table,
            {
                "episode_id": episode_id,
                "event_id": culture_id,
                "episode_event_field_concept_id": "1147140",
            },
        )
        moo_concept_id = (
            (
                self.db_handler.fetch_OMOP_concept_from_db(
                    "SNOMED", data_row.microorganismo_otros_cult
                ),
            )
            if not pd.isnull(data_row.microorganismo_otros_cult)
            else self.default_values["microorganismo"]
        )

        moo_in_culture_id = self.db_handler.execute_query(
            ETLQueries.load_observation_table_query,
            {
                "person_id": person_id,
                "observation_concept_id": moo_concept_id,
                "observation_date": data_row.fecha_otros_cultivos,
                "observation_type_concept_id": "32809",
                "value_as_number": None,
                "value_as_concept_id": None,
                "value_source_value": data_row.microorganismo_otros_cult,
                "observation_event_id": culture_id,
                "obs_event_field_concept_id": "1147140",
                "visit_occurrence_id": visit_id,
                "observation_source_value": None,
            },
            return_id=True,
        )
        self.db_handler.execute_query(
            ETLQueries.load_episode_event_table,
            {
                "episode_id": episode_id,
                "event_id": moo_in_culture_id,
                "episode_event_field_concept_id": "1147167",
            },
        )
        if (
            not pd.isnull(data_row.bmr_etiologia_otros)
            and data_row.bmr_etiologia_otros == "1"
        ):
            bmr_id = self.db_handler.execute_query(
                ETLQueries.load_observation_table_query,
                {
                    "person_id": person_id,
                    "observation_concept_id": self.rosetta_parser.get_concept_id(
                        "bmr_etiologia_otros",
                        "semantic_link",
                        data_row.bmr_etiologia_otros,
                    ),
                    "observation_date": data_row.fecha_otros_cultivos,
                    "observation_type_concept_id": "32809",
                    "value_as_number": None,
                    "value_as_concept_id": None,
                    "value_source_value": data_row.bmr_etiologia_otros,
                    "observation_event_id": moo_in_culture_id,
                    "obs_event_field_concept_id": "1147167",
                    "visit_occurrence_id": visit_id,
                    "observation_source_value": None,
                },
                return_id=True,
            )
            self.db_handler.execute_query(
                ETLQueries.load_episode_event_table,
                {
                    "episode_id": episode_id,
                    "event_id": bmr_id,
                    "episode_event_field_concept_id": "1147167",
                },
            )
            feno_resist_values = [
                re.search(r"___(\d+)$", field).group(1)
                for field, value in data_row._asdict().items()
                if field.startswith("feno") and value == "1"
            ]
            for feno_resist_value in feno_resist_values:
                if feno_resist_value == "1":
                    if self.db_handler.has_desired_ancestor(moo_concept_id, "4214811"):
                        feno_concept_id = "3009403"
                    else:
                        feno_concept_id = self.rosetta_parser.get_concept_id(
                            "fenotipo_resistencia_otros",
                            "semantic_link",
                            feno_resist_value,
                        )
                else:
                    feno_concept_id = self.rosetta_parser.get_concept_id(
                        "fenotipo_resistencia_otros", "semantic_link", feno_resist_value
                    )

                feno_id = self.db_handler.execute_query(
                    ETLQueries.load_measurement_table_query,
                    {
                        "person_id": person_id,
                        "measurement_concept_id": feno_concept_id,
                        "measurement_date": data_row.fecha_otros_cultivos,
                        "measurement_type_concept_id": "32809",
                        "operator_concept_id": None,
                        "value_as_number": None,
                        "value_as_concept_id": self.rosetta_parser.get_concept_id(
                            "fenotipo_resistencia_otros",
                            "value_link",
                            feno_resist_value,
                        ),
                        "unit_concept_id": None,
                        "measurement_event_id": bmr_id,
                        "meas_event_field_concept_id": "1147167",
                        "visit_occurrence_id": visit_id,
                        "measurement_source_value": "fenotipo_resistencia_otros",
                        "value_source_value": feno_resist_value,
                    },
                    return_id=True,
                )
                self.db_handler.execute_query(
                    ETLQueries.load_episode_event_table,
                    {
                        "episode_id": episode_id,
                        "event_id": feno_id,
                        "episode_event_field_concept_id": "1147140",
                    },
                )

    def load_data(self, data_row):
        """Main method for loading the RedCap data into the OMOP dtabase. This method takes as an input a RedCap record, and depending on the name of the RedCap form,
        calls the appropiate processing method.
        Args:
            data_row (named_tuple): RedCap data for a specific form and for a single patient.
        """
        form_name = type(data_row).__name__
        person_source_value = data_row.record_id
        person_id = self.db_handler.populate_person_origin_table(person_source_value)

        match form_name:
            case "paciente":
                self.process_form_paciente(data_row, person_source_value, person_id)
            case "sintomas":
                self.process_form_sintomas(data_row, person_id)
            case "signos":
                self.process_form_signos(data_row, person_id)
            case "comorbilidad":
                self.process_form_comorbilidad(data_row, person_id)
            case "sepsis":
                self.process_form_sepsis(data_row, person_id)
            case "factores_de_riesgo_de_infeccion_por_bacteria_multi":
                self.process_form_factores_riesgo(data_row, person_id)
            case "infecciones_previas":
                self.process_form_infecciones_previas(data_row, person_id)
            case "colonizaciones_previas":
                self.process_form_colonizaciones_previas(data_row, person_id)
            case "tratamiento_antibiotico_previo":
                self.process_form_tratamiento_antibiotico_previo(data_row, person_id)
            case "tratamiento_empirico":
                self.process_form_tratamiento_empirico(data_row, person_id)
            case "hemocultivo_de_urgencias":
                self.process_form_hemocultivo(data_row, person_id)
            case "otros_cultivos_en_urgencias":
                self.process_form_otros_cultivos(data_row, person_id)


def main():
    try:
        logger.info("ETL process started")
        base_url = DB_MEPRAM_PRIVILEGED.base_url
        api_token = DB_MEPRAM_PRIVILEGED.api_token
        db_credentials = DB_MEPRAM_PRIVILEGED.db_credentials
        exporter = REDCapDataExporter.REDCapDataExporter(base_url, api_token)
        OMOP_loader = OMOPLoader(db_credentials, "rosetta.csv", "atc2rxnorm.csv")
        filter = REDCapDataFilter(base_url, api_token, "filtering")
        hospital_codes = exporter.export_hospital_codes()
        ETL_forms_dict = {
            "paciente": [
                "record_id",
                "fecha_ingreso_urgencias",
                "fecha_nacimiento",
                "sexo",
                "codigo_postal",
                "mujer_gestante",
                "paciente_residencia",
            ],
            "sintomas": "ALL",
            "signos": "ALL",
            "comorbilidad": [
                "record_id",
                "infarto",
                "insuficiencia_cardiaca",
                "evp",
                "e_cerebrovascular",
                "demencia",
                "e_pulmonar_cronica",
                "ulcera_peptica",
                "colagenopatia",
                "hemiplejia",
                "erc",
                "neoplasia",
                "tipo_cancer",
                "linfoma",
                "leucemia",
                "sida",
                "hepatopatia",
                "tipo_hepatopatia",
                "diabetes",
                "indice_de_charlson",
                "inmunosupresion",
                "causa_inmunosupresion",
                "escala_karnofsky",
            ],
            "sepsis": "ALL",
            "factores_de_riesgo_de_infeccion_por_bacteria_multi": "ALL",
            "infecciones_previas": "ALL",
            "colonizaciones_previas": "ALL",
            "tratamiento_antibiotico_previo": "ALL",
            "tratamiento_empirico": "ALL",
            "hemocultivo_de_urgencias": "ALL",
            "otros_cultivos_en_urgencias": "ALL",
        }
        print_banner()
        execute_against_db = input(
            f"[ /!\ ] Estas ejecutando el ETL contra el RedCap {base_url} y almacenando en la base de datos {db_credentials['dbname']} localizada en el servidor {db_credentials['server']}. ¿Quiere continuar? [y/n]"
        )
        if execute_against_db == "y":
            pass
        else:
            print("Exiting...")
            sys.exit()
        only_save_invalid_records = input(
            "[ (?) ] Deseas solo guardar resultados del filtrado, sin realizar ETL? [y/n]"
        )
        if only_save_invalid_records == "y":
            print("[+] Performing data filtering...")
            invalid_data = filter.filter_data(list(ETL_forms_dict.keys()))
            print("[+] Filtering completed, exiting...")
            sys.exit()
        else:
            print("[+] Executing ETL...")

            print(ETL_forms_dict)
            OMOP_loader.db_handler.populate_care_site_table(hospital_codes)
            print("[+] Performing data filtering...")
            invalid_data = filter.filter_data(list(ETL_forms_dict.keys()))
            print(
                "[+] Filtering completed, now uploading forms to the OMOP database..."
            )

            for idx, form_name in enumerate(ETL_forms_dict):

                if ETL_forms_dict[form_name] != "ALL":
                    redcap_data = exporter.export_form(
                        form_name, fields=ETL_forms_dict[form_name]
                    )
                else:
                    redcap_data = exporter.export_form(form_name)

                if redcap_data.empty:
                    logger.warning(f"No data exported for form {form_name}")
                    continue

                current_record_id = None
                processed = False
                for row in redcap_data.itertuples(index=False, name=form_name):
                    # Check if we are processing a form where all values = 0 is significant
                    is_significant_form = form_name in [
                        "signos",
                        "factores_de_riesgo_de_infeccion_por_bacteria_multi",
                        "comorbilidad",
                    ]
                    is_empty_or_zero = all(
                        pd.isna(value) or value == "0" for value in row[1:]
                    )
                    is_completely_empty = all(pd.isna(value) for value in row[1:])

                    # Skipping based on conditions
                    if (not is_significant_form and is_empty_or_zero) or (
                        is_significant_form and is_completely_empty
                    ):
                        logger.warning(
                            f"Skipping form {form_name} for patient {row.record_id} since it is all empty: {row}"
                        )
                        continue
                    try:
                        if row.record_id != current_record_id:
                            if current_record_id is not None and processed:
                                OMOP_loader.db_handler.update_form_tracking(
                                    current_record_id, form_name
                                )

                            current_record_id = row.record_id
                            processed = False

                        if OMOP_loader.db_handler.is_form_processed(
                            current_record_id, form_name
                        ):
                            continue

                        OMOP_loader.load_data(row)
                        processed = True

                    except Exception as e:
                        logger.error(
                            f"Error loading data for row {row}: {traceback.format_exc()}"
                        )

                if current_record_id is not None and processed:
                    OMOP_loader.db_handler.update_form_tracking(
                        current_record_id, form_name
                    )
                print(f"Form {form_name} uploaded! Progress: {((idx+1)/12 *100):2f}%")

            OMOP_loader.db_handler.update_quality_check(invalid_data)
            print("[+] Populating fact_relationship table...")
            OMOP_loader.db_handler.populate_fact_relationship_table(
                relationship_type="infectious_disease_episode"
            )
            logger.info("ETL process finished")
    except Exception:
        logger.error(f"Error in main function: {traceback.format_exc()}")


if __name__ == "__main__":
    main()
