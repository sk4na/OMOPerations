import pandas as pd
import logging


class RosettaParser:
    """Class that handles operations on the Rosetta ETL file"""

    def __init__(self, rosetta_file):
        try:
            self.rosetta = pd.read_csv(rosetta_file, sep=";").fillna("NONE")
        except Exception as e:
            logging.error(f"Error loading rosetta file {rosetta_file}: {e}")
            self.rosetta = pd.DataFrame()

    def get_concept_id(self, variable, branch, value="NONE"):
        """Get OMOP concept ID from the rosetta file by RedCap variable name, semantics branch name and value.

        Args:
            variable (str): Name of the RedCap variable that we want to get the OMOP concept id for.
            branch (str): Name of the semantics branch associated to the OMOP concept id we want to get for the variable.
            value (str, optional): Value of the RedCap vairable. Defaults to "NONE".

        Raises:
            ValueError: No matching concept id for the specific variable, value and branch input.

        Returns:
            str: OMOP concept id from the rosetta file.
        """
        df = self.rosetta[
            (self.rosetta["variable"] == variable)
            & (self.rosetta["source_value"] == value)
            & (self.rosetta["branch"] == branch)
        ]
        if not df.empty:
            return df["concept_id"].iloc[0]
        else:
            error_msg = f"No matching concept id for variable '{variable}', value '{value}', and branch '{branch}' inside the rosetta file."
            logging.error(error_msg)
            raise ValueError(error_msg)

    def get_concept_code(self, variable, branch, value="NONE"):
        """Get concept code from the rosetta file by RedCap variable name, semantics branch name and value.

        Args:
            variable (str): Name of the RedCap variable that we want to get the concept code for.
            branch (str): Name of the semantics branch associated to the concept code we want to get for the variable.
            value (str, optional): Value of the RedCap vairable. Defaults to "NONE".

        Raises:
            ValueError: No matching concept id for the specific variable, value and branch input.

        Returns:
            str: Concept code from the rosetta file.
        """
        df = self.rosetta[
            (self.rosetta["variable"] == variable)
            & (self.rosetta["source_value"] == value)
            & (self.rosetta["branch"] == branch)
        ]
        if not df.empty:
            return df["concept_code"].iloc[0]
        else:
            error_msg = f"No matching concept code for variable '{variable}', value '{value}', and branch '{branch}' inside the rosetta file."
            logging.error(error_msg)
            raise ValueError(error_msg)
