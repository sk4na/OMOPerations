import logging
import traceback
from redcap import Project, RedcapError


class REDCapDataExporter:
    """Class that handles the exporting of RedCAP data"""

    def __init__(self, base_url, api_token):
        try:
            self.project = Project(base_url, api_token)
        except Exception as e:
            logging.error(f"Error initializing REDCap project: {e}")
            raise

    def export_form(self, form_name, fields=None):
        """Exports all patient's RedCap data by form name, and specific fields if desired, only if they are complete.

        Args:
            form_name (str): RedCap form name
            fields (array[str], optional): Array of the name of the fields that we want to export from the form. Defaults to None.

        Returns:
            DataFrame: Pandas dataframe containing the desired form data, excluding the RedCap default fields 'redcap_repeat_instrument', 'redcap_repeat_instance', and 'formname_complete'
        """
        try:
            df = self.project.export_records(
                forms=[form_name],
                format_type="df",
                raw_or_label="raw",
                filter_logic=f"[{form_name}_complete] = 2 or [{form_name}_complete] = 1",
                df_kwargs={"dtype": "object", "usecols": fields},
            )
            fields_to_drop = [
                "redcap_repeat_instrument",
                "redcap_repeat_instance",
                f"{form_name}_complete",
            ]
            return df.drop(columns=[col for col in fields_to_drop if col in df.columns])
        except Exception:
            logging.error(f"Error exporting form {form_name}: {traceback.format_exc()}")

    def export_hospital_codes(self):
        """Export RedCap DAG info. Given that each hospital has it's own unique DAG, this information is equivalent
        to the hospital identifying codes.

        Returns:
            dict: Python dictionary containing the DAG names and it's unique RedCap ID.
        """
        try:
            hospital_code_dict = self.project.export_dags()
            return hospital_code_dict
        except RedcapError as e:
            print(f"Error fetching hospital codes: {e}")
            return
