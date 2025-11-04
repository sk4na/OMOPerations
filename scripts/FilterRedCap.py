import logging
import pandas as pd
import numpy as np
from datetime import datetime
import traceback
from config import DB_MEPRAM_PRIVILEGED
import os
from redcap import Project, RedcapError

logger = logging.getLogger("RedCAP_filter")

api_token = DB_MEPRAM_PRIVILEGED.api_token
base_url = "https://redcap.isciii.es/api/"
mandatory_vars = {
    "paciente": [
        "fecha_ingreso_urgencias",
        "fecha_nacimiento",
        "sexo",
        "codigo_postal",
        "mujer_gestante",
        "paciente_residencia",
    ],
    "comorbilidad": [
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
        "linfoma",
        "leucemia",
        "sida",
        "hepatopatia",
        "diabetes",
        "inmunosupresion",
        "causa_inmunosupresion",
        "situacion_funcional_basal",
    ],
    "sintomas": [
        "sintoma",
        "duracion_sintoma",
    ],
    "signos": [
        "hipotermia_hipertermia",
        "taquicardia",
        "taquipnea",
        "hipotension",
        "hipoxemia",
    ],
    "sepsis": [
        "foco",
        "sepsis",
        "shock_septico",
    ],
    "factores_de_riesgo": [
        "hospit_ano_previo",
        "hospit_mes_previo",
        "hospit_ano_previo_uci",
        "cirugia_previa_sin_implant",
        "cirugia_previa_con_implant",
        "asistencia_sanitaria_prev",
        "hemodialisis_permanente",
        "dialisis_peritoneal",
        "cateter_venoso",
        "sonda_urinaria",
        "sonda_nasogastrica",
        "derivacion_ventriculoper",
        "valvula_prot_cardiaca",
        "portador_otros_disposit",
    ],
    "infecciones_previas": [
        "fecha_infeccion",
        "sindrome_infeccioso",
        "microorganism_infec_prev",
        "bmr_infec_previa",
        "feno_resist_infec_prev___",
    ],
    "colonizaciones_previas": [
        "fecha_colonizacion",
        "microorganismo_colonizador",
        "bmr_colonizador",
        "fenotipo_resist_colo___",
    ],
    "tratamiento_antibiotico_previo": [
        "antimicrobiano_previo",
        "via_administ_antib_prev",
    ],
    "tratamiento_empirico": ["antimicrobiano_empirico"],
    "hemocultivo_de_urgencias": [
        "id_hemocultivo",
        "fecha_hemocultivo",
        "hemo_positivo_si_no",
        "microorganismo",
        "bmr_etiologia",
        "fenotipo_resistencia___",
    ],
    "otros_cultivos_en_urgencias": [
        "tipo_cultivo",
        "microorganismo_otros_cult",
        "bmr_etiologia_otros",
        "fenotipo_resistencia_otros___",
    ],
}


class REDCapDataFilter:
    """Class that handles the filtering of RedCAP data"""

    def __init__(self, base_url, api_token, output_dir):
        """Initialize the REDCap project connection and filtering rules.

        Args:
            base_url (str): Base URL of the REDCap API endpoint.
            api_token (str): Project API token granting access to records.
            output_dir (str): Local directory where processed and excluded CSVs will be saved.

        Raises:
            Exception: If the REDCap project cannot be initialized.
        """
        try:
            self.project = Project(base_url, api_token)
            self.output_dir = output_dir
            self.mandatory_vars = {
                "paciente": [
                    "fecha_ingreso_urgencias",
                    "fecha_nacimiento",
                    "sexo",
                    "codigo_postal",
                    "mujer_gestante",
                    "paciente_residencia",
                ],
                "comorbilidad": [
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
                    "linfoma",
                    "leucemia",
                    "sida",
                    "hepatopatia",
                    "diabetes",
                    "inmunosupresion",
                    "causa_inmunosupresion",
                    "situacion_funcional_basal",
                ],
                "sintomas": [
                    "sintoma",
                    "duracion_sintoma",
                ],
                "signos": [
                    "hipotermia_hipertermia",
                    "taquicardia",
                    "taquipnea",
                    "hipotension",
                    "hipoxemia",
                ],
                "sepsis": [
                    "foco",
                    "sepsis",
                    "shock_septico",
                ],
                "factores_de_riesgo": [
                    "hospit_ano_previo",
                    "hospit_mes_previo",
                    "hospit_ano_previo_uci",
                    "cirugia_previa_sin_implant",
                    "cirugia_previa_con_implant",
                    "asistencia_sanitaria_prev",
                    "hemodialisis_permanente",
                    "dialisis_peritoneal",
                    "cateter_venoso",
                    "sonda_urinaria",
                    "sonda_nasogastrica",
                    "derivacion_ventriculoper",
                    "valvula_prot_cardiaca",
                    "portador_otros_disposit",
                ],
                "infecciones_previas": [
                    "fecha_infeccion",
                    "sindrome_infeccioso",
                    "microorganism_infec_prev",
                    "bmr_infec_previa",
                    "feno_resist_infec_prev___",
                ],
                "colonizaciones_previas": [
                    "fecha_colonizacion",
                    "microorganismo_colonizador",
                    "bmr_colonizador",
                    "fenotipo_resist_colo___",
                ],
                "tratamiento_antibiotico_previo": [
                    "antimicrobiano_previo",
                    "via_administ_antib_prev",
                ],
                "tratamiento_empirico": ["antimicrobiano_empirico"],
                "hemocultivo_de_urgencias": [
                    "id_hemocultivo",
                    "fecha_hemocultivo",
                    "hemo_positivo_si_no",
                    "microorganismo",
                    "bmr_etiologia",
                    "fenotipo_resistencia___",
                ],
                "otros_cultivos_en_urgencias": [
                    "tipo_cultivo",
                    "microorganismo_otros_cult",
                    "bmr_etiologia_otros",
                    "fenotipo_resistencia_otros___",
                ],
            }
            self.exceptions_dict = {
                "paciente": {
                    "mujer_gestante": lambda df: df[
                        (df["mujer_gestante"].isna()) & (df["sexo"] != "0")
                    ]
                },
                "comorbilidad": {
                    "tipo_hepatopatia": lambda df: df[
                        (df["tipo_hepatopatia"].isna())
                        & (df["hepatopatia"].isin(["1", "3"]))
                    ],
                    "causa_inmunosupresion": lambda df: df[
                        (df["causa_inmunosupresion"].isna())
                        & (df["inmunosupresion"] == "1")
                    ],
                    "tipo_cancer": lambda df: df[
                        (df["tipo_cancer"].isna()) & (df["neoplasia"].isin(["2", "6"]))
                    ],
                },
                "infecciones_previas": {
                    "bmr_infec_previa": lambda df: df[
                        (df["bmr_infec_previa"].isna())
                        & (df["microorganism_infec_prev"].notna())
                    ],
                },
                "colonizaciones_previas": {
                    "bmr_colonizador": lambda df: df[
                        (df["bmr_colonizador"].isna())
                        & (df["microorganismo_colonizador"].notna())
                    ],
                },
                "hemocultivo_de_urgencias": {
                    "microorganismo": lambda df: df[
                        (df["microorganismo"].isna())
                        & (df["hemo_positivo_si_no"] == "1")
                    ],
                    "bmr_etiologia": lambda df: df[
                        (df["bmr_etiologia"].isna()) & (df["microorganismo"].notna())
                    ],
                },
                "otros_cultivos_en_urgencias": {
                    "bmr_etiologia_otros": lambda df: df[
                        (df["bmr_etiologia_otros"].isna())
                        & (df["microorganismo_otros_cult"].notna())
                    ],
                },
            }
        except Exception as e:
            logger.error(f"Error initializing REDCap project: {e}")
            raise

    def get_checkbox_vars(self, df, base_column_name):
        """Helper function to handle checkbox variable names"""
        checkbox_columns = [
            col for col in df.columns if col.startswith(base_column_name)
        ]
        return checkbox_columns

    def export_form(self, form_name):
        """Export a REDCap form as a pandas DataFrame, limited to complete records.

        The export filters records with ``[{form_name}_complete] = 2`` and drops
        REDCap meta-columns (repeat instrument/instance and the completion field).

        Args:
            form_name (str): The REDCap instrument (form) name to export.

        Returns:
            pandas.DataFrame: The exported data with selected meta-columns removed.
            None: If an error occurs during export (error is logged).
        """
        try:
            df = self.project.export_records(
                forms=[form_name],
                format_type="df",
                raw_or_label="raw",
                filter_logic=f"[{form_name}_complete] = 2",
                df_kwargs={"dtype": "object"},
            )
            fields_to_drop = [
                "redcap_repeat_instrument",
                "redcap_repeat_instance",
                f"{form_name}_complete",
            ]
            return df.drop(columns=[col for col in fields_to_drop if col in df.columns])
        except Exception:
            logger.error(f"Error exporting form {form_name}: {traceback.format_exc()}")

    def process_df(self, df, form_name, mandatory_vars, exceptions_dict):
        """Apply filtering rules to a form DataFrame and track validation failures.

        - Removes rows that are entirely NaN (excluding ``record_id``).
        - Removes rows that are all NaN or '0' for most forms (see in-code exceptions).
        - Checks mandatory variables for presence or exception conditions.
        - Captures per-record failed checks and returns excluded rows.

        Args:
            df (pandas.DataFrame): Raw DataFrame exported from REDCap for the form.
            form_name (str): Name of the form being processed.
            mandatory_vars (dict): Mapping of form -> list of mandatory variable names.
            exceptions_dict (dict): Mapping of form -> {variable: callable(df)->DataFrame}
                specifying exception-based rules for mandatory checks.

        Returns:
            tuple[pandas.DataFrame, dict, pandas.DataFrame]:
                - pandas.DataFrame: The filtered DataFrame after exclusions.
                - dict: ``{record_id: [failed_var, ...]}`` with failed mandatory checks.
                - pandas.DataFrame: DataFrame of excluded rows for auditing.
        """
        if df.empty:
            logger.warning("No data to process. DataFrame is empty.")
            return df, {}  # Return an empty dictionary if no data to process

        # Dictionary to store failed mandatory checks for the current form
        failed_mandatory_checks = {}

        # Log the initial number of records
        initial_count = len(df)
        logger.info(f"Processing {initial_count} records for form '{form_name}'")

        # Exclude 'record_id' from NaN checks
        non_id_columns = df.columns.difference(["record_id"])

        # Drop rows with all NaN values (excluding the 'record_id' column)
        nan_rows = df[non_id_columns].isna().all(axis=1)
        if nan_rows.any():
            logger.info(
                f"Excluded {nan_rows.sum()} rows due to all NaN values. Record IDs: {df.loc[nan_rows, 'record_id'].tolist()}"
            )
        df = df[~nan_rows]

        # Drop rows where all values are NaN or 0 (excluding the 'record_id' column)
        if form_name not in [
            "comorbilidad",
            "signos",
            "sepsis",
            "factores_de_riesgo_de_infeccion_por_bacteria_multi",
        ]:
            nan_or_zero_rows = df[non_id_columns].apply(
                lambda row: all(pd.isna(x) or x == 0 or x == "0" for x in row), axis=1
            )
            if nan_or_zero_rows.any():
                logger.info(
                    f"Excluded {nan_or_zero_rows.sum()} rows due to all NaN or 0 values. Record IDs: {df.loc[nan_or_zero_rows, 'record_id'].tolist()}"
                )
            df = df[~nan_or_zero_rows]

        # Initialize a set to accumulate the indices of rows that should be excluded
        rows_to_exclude = set()

        if form_name in mandatory_vars:
            for var in mandatory_vars[form_name]:
                if var.endswith("___"):  # Es variable tipo checkbox
                    bmr_var = previous_var_name
                    feno_vars = self.get_checkbox_vars(df, var)
                    filtered_df = df[
                        (
                            df[feno_vars].apply(
                                lambda x: (x == "0").all() or x.isna().all(), axis=1
                            )
                        )
                        & (df[bmr_var] == "1")
                    ]
                    if not filtered_df.empty:
                        # Log the failed mandatory check for checkbox variables
                        for record_id in filtered_df["record_id"]:
                            if record_id not in failed_mandatory_checks:
                                failed_mandatory_checks[record_id] = []
                            # Add all failed checkbox vars to the failed_mandatory_checks
                            failed_mandatory_checks[record_id].append(var)

                        logger.info(
                            f"Excluded {len(filtered_df)} rows where all feno_resist fields were 0 and '{bmr_var}' was 1. Record IDs: {filtered_df['record_id'].tolist()}"
                        )

                        # Accumulate the indices of failing rows
                        rows_to_exclude.update(filtered_df.index)

                elif var in exceptions_dict.get(form_name, {}):
                    previous_var_name = var
                    condition = exceptions_dict[form_name][var]
                    failed_mandatory_rows = condition(df)
                    if not failed_mandatory_rows.empty:
                        # Log the failed mandatory check
                        for record_id in failed_mandatory_rows["record_id"]:
                            if record_id not in failed_mandatory_checks:
                                failed_mandatory_checks[record_id] = []
                            failed_mandatory_checks[record_id].append(var)

                        logger.info(
                            f"Excluded {len(failed_mandatory_rows)} rows due to failing mandatory variable '{var}' with exception logic. Record IDs: {failed_mandatory_rows['record_id'].tolist()}"
                        )

                        # Accumulate the indices of failing rows
                        rows_to_exclude.update(failed_mandatory_rows.index)

                else:
                    previous_var_name = var
                    failed_mandatory_rows = df[df[var].isna()]
                    if not failed_mandatory_rows.empty:
                        # Log the failed mandatory check
                        for record_id in failed_mandatory_rows["record_id"]:
                            if record_id not in failed_mandatory_checks:
                                failed_mandatory_checks[record_id] = []
                            failed_mandatory_checks[record_id].append(var)

                        logger.info(
                            f"Excluded {len(failed_mandatory_rows)} rows due to missing mandatory variable '{var}'. Record IDs: {failed_mandatory_rows['record_id'].tolist()}"
                        )

                        # Accumulate the indices of failing rows
                        rows_to_exclude.update(failed_mandatory_rows.index)

        excluded_rows = pd.concat([df.loc[list(rows_to_exclude)]])

        # After all variables are processed, filter the DataFrame once
        df = df.drop(rows_to_exclude)

        # Log the final number of records
        final_count = len(df)
        logger.info(f"Final record count after filtering: {final_count}")

        return df, failed_mandatory_checks, excluded_rows

    def filter_form(self, form_name, mandatory_vars, exceptions_dict):
        """Export, filter, and clean a single REDCap form.

        Args:
            form_name (str): Name of the form to process.
            mandatory_vars (dict): Mapping of form -> mandatory variable list.
            exceptions_dict (dict): Mapping of form -> exception rules for checks.

        Returns:
            tuple[pandas.DataFrame, dict, pandas.DataFrame]:
                - pandas.DataFrame: Filtered form data with empty columns dropped.
                - dict: ``{record_id: [failed_var, ...]}`` of failed checks.
                - pandas.DataFrame: Excluded rows for that form.
        """
        df = self.export_form(form_name)
        df, failed_mandatory_checks, excluded_rows = self.process_df(
            df, form_name, mandatory_vars, exceptions_dict
        )
        df = df.dropna(axis=1, how="all")
        return df, failed_mandatory_checks, excluded_rows

    def filter_data(self, form_list):
        """Process a list of REDCap forms, persist outputs, and aggregate failures.

        For each form:
        - Optionally flags records with missing QSOFA/SOFA inputs.
        - Exports and filters the form.
        - Saves ``*_processed.csv`` and ``*_excluded.csv`` to ``output_dir``.
        After all forms:
        - Builds ``aggregated_failed_checks.csv`` with per-record, per-form failures.

        Args:
            form_list (list[str]): Ordered list of forms to export and filter.

        Returns:
            dict: Aggregated failures as ``{record_id: [ "form:variable", ... ]}``.
        """
        aggregated_failed_checks = {}
        sofa_bad_record_ids = []
        qsofa_bad_record_ids = []
        for form_name in form_list:
            if form_name == "signos":
                self.filter_bad_calc_fields(form_name, qsofa_bad_record_ids)
            elif form_name == "sepsis":
                self.filter_bad_calc_fields(
                    form_name, qsofa_bad_record_ids, sofa_bad_record_ids
                )

            try:
                filtered_df, failed_mandatory_checks, excluded_records_df = (
                    self.filter_form(
                        form_name, self.mandatory_vars, self.exceptions_dict
                    )
                )
                output_path_excluded = os.path.join(
                    self.output_dir, f"{form_name}_excluded.csv"
                )
                excluded_records_df.to_csv(output_path_excluded, index=False)

                # Merge the failed checks from the current form into the global dictionary
                for record_id, failed_vars in failed_mandatory_checks.items():
                    if record_id not in aggregated_failed_checks:
                        aggregated_failed_checks[record_id] = []
                    aggregated_failed_checks[record_id].extend(
                        [f"{form_name}:{var}" for var in failed_vars]
                    )

                output_path = os.path.join(
                    self.output_dir, f"{form_name}_processed.csv"
                )
                filtered_df.to_csv(output_path, index=False)
                print(f"Saved filtered DataFrame for {form_name} at {output_path}")
            except Exception as e:
                logger.error(
                    f"Failed to process form {form_name}: {traceback.format_exc()}"
                )

        # Optional: Log the entire aggregated_failed_checks dictionary
        logger.info(f"Aggregated failed mandatory checks: {aggregated_failed_checks}")

        # Create a list to hold the final data for CSV export
        csv_data = []

        # Iterate through the aggregated_failed_checks and prepare the CSV data
        for record_id, variables in aggregated_failed_checks.items():
            hospital, _ = record_id.split(
                "-"
            )  # Split record_id to get hospital and id_paciente
            for variable in variables:
                form, var = variable.split(":")
                csv_data.append(
                    {
                        "hospital": hospital,
                        "id_paciente": record_id,
                        "formulario": form,
                        "variables": var,
                    }
                )

        # Convert the list to a DataFrame
        df_csv = pd.DataFrame(csv_data)

        # Sort the DataFrame by the 'hospital' column
        df_csv = df_csv.sort_values(by="hospital")

        # Save the DataFrame to a CSV file
        csv_output_path = os.path.join(self.output_dir, "aggregated_failed_checks.csv")
        df_csv.to_csv(csv_output_path, index=False)

        return aggregated_failed_checks

    def filter_bad_calc_fields(
        self, form_name, qsofa_bad_record_ids=None, sofa_bad_record_ids=None
    ):
        """Identify records with missing inputs for QSOFA/SOFA and collect their IDs.

        Depending on ``form_name``:
        - ``'signos'``: Checks required QSOFA inputs and appends IDs to ``qsofa_bad_record_ids``.
        - ``'sepsis'``: Checks required SOFA inputs (append to ``sofa_bad_record_ids``)
            and QSOFA inputs (append to ``qsofa_bad_record_ids``).

        Args:
            form_name (str): Either ``'signos'`` or ``'sepsis'``.
            qsofa_bad_record_ids (list[str], optional): Accumulator for record_ids with missing QSOFA inputs.
            sofa_bad_record_ids (list[str], optional): Accumulator for record_ids with missing SOFA inputs.

        Raises:
            ValueError: If ``form_name`` is not supported.

        Returns:
            None
        """
        qsofa_signos_must_have_vars = ["taquipnea", "hipotension"]
        qsofa_sepsis_must_have_vars = ["estado_mental_alterado"]
        sofa_sepsis_must_have_vars = [
            "respiracion",
            "snc_glasgow",
            "cardiovascular",
            "bilirrubina",
            "plaquetas",
            "creatinina",
        ]
        data = self.export_form(form_name)

        match form_name:
            case "signos":
                # Handle QSOFA-related missing values
                missing_qsofa_rows = data[
                    data[qsofa_signos_must_have_vars].isna().any(axis=1)
                ]
                # Accumulate unique record_ids
                qsofa_bad_record_ids.extend(
                    x
                    for x in missing_qsofa_rows["record_id"].tolist()
                    if x not in qsofa_bad_record_ids
                )

            case "sepsis":
                # Handle SOFA-related missing values
                missing_sofa_rows = data[
                    data[sofa_sepsis_must_have_vars].isna().any(axis=1)
                ]
                sofa_bad_record_ids.extend(
                    x
                    for x in missing_sofa_rows["record_id"].tolist()
                    if x not in sofa_bad_record_ids
                )

                # Handle QSOFA-related missing values
                missing_qsofa_rows = data[
                    data[qsofa_sepsis_must_have_vars].isna().any(axis=1)
                ]
                qsofa_bad_record_ids.extend(
                    x
                    for x in missing_qsofa_rows["record_id"].tolist()
                    if x not in qsofa_bad_record_ids
                )

            case _:
                # Raise an exception for unsupported form names
                raise ValueError(f"Unsupported form name: {form_name}")
