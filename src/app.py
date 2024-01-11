import click
import pandas as pd
import logging
from .utils import standardize_value, fuzzy_match
import dask.dataframe as dd

logging.basicConfig(
    format="%(asctime)s -> %(levelname)s : %(message)s", level=logging.INFO)


class Reconciler:
    def __init__(self, source_path, target_path, output_path, comparison_columns=None, threshold=1):
        """
        Initialize the Reconciler.

        Parameters:
        - source_path (str): Path to the source CSV file.
        - target_path (str): Path to the target CSV file.
        - output_path (str): Path to the output CSV file.
        - comparison_columns (list): List of columns for reconciliation.
        - threshold (int): Levenshtein distance threshold for fuzzy matching.
        """
        self.source_path = source_path
        self.target_path = target_path
        self.output_path = output_path
        self.comparison_columns = comparison_columns
        self.threshold = threshold
        self.discrepancy_details = []

    def _identify_missing_records(self, source_ddf, target_ddf):
        """
        Identify missing records in source and target DataFrames.

        Parameters:
        - source_ddf (Dask DataFrame): Source Dask DataFrame.
        - target_ddf (Dask DataFrame): Target Dask DataFrame.

        Returns:
        - tuple: Missing records in target and source, respectively.
        """
        if not self.comparison_columns:
            missing_in_target = set(
                source_ddf["ID"].compute()) - set(target_ddf["ID"].compute())
            missing_in_source = set(
                target_ddf["ID"].compute()) - set(source_ddf["ID"].compute())
        else:
            missing_in_target = set(
                source_ddf[self.comparison_columns].compute()) - set(target_ddf[self.comparison_columns].compute())
            missing_in_source = set(
                target_ddf[self.comparison_columns].compute()) - set(source_ddf[self.comparison_columns].compute())

        return missing_in_target, missing_in_source

    def _analyze_discrepancies(self, merged_df, sourced_dff):
        """
        Analyze discrepancies in the merged DataFrame.

        Parameters:
        - merged_df (Dask DataFrame): Merged Pandas DataFrame.

        Returns:
        - list: List of discrepancy details.
        """

        for index, row in merged_df.iterrows():
            for column in self.comparison_columns or sourced_dff.columns.difference(["ID"]):

                source_value = row[column+"_x"]
                target_value = row[column+"_y"]
                if not fuzzy_match(source_value=source_value, target_value=target_value):
                    self.discrepancy_details.append({
                        "Type": "Field Discrepancy",
                        "Record Identifier": row["ID"],
                        "Field": column,
                        "Source Value": source_value,
                        "Target Value": target_value,
                    })

        return self.discrepancy_details

    def reconcile(self):
        """
        Perform reconciliation and generate a report.
        """
        try:
            # Read CSV files using Dask
            source_ddf = dd.read_csv(self.source_path)
            target_ddf = dd.read_csv(self.target_path)

            # Identify missing records
            missing_in_target, missing_in_source = self._identify_missing_records(
                source_ddf, target_ddf)

            # Merge Dask DataFrame
            merged_ddf = dd.merge(source_ddf, target_ddf, on="ID", how="inner")
            merged_df = merged_ddf.compute()

            # Identify and analyze discrepancies
            self.discrepancy_details = self._analyze_discrepancies(
                merged_df, source_ddf)
            print(self.discrepancy_details)
            # Generate report data
            report_data = [{"Type": "Missing in Target", "Record Identifier": record_id}
                           for record_id in missing_in_target]
            report_data += [{"Type": "Missing in Source", "Record Identifier": record_id}
                            for record_id in missing_in_source]
            report_data += self.discrepancy_details

            # Generate report
            report_df = pd.DataFrame(report_data)
            report_df.to_csv(self.output_path, index=False)

            logging.info(click.style(
                f"""Reconciliation completed: \n - Records missing in target: {len(missing_in_target)} \n - Records missing in source: {len(missing_in_source)} \n - Records with field discrepancies: {len(self.discrepancy_details)}""", fg="green"))
            logging.info(
                f"Reconciliation Report saved to: {self.output_path.name}")

        except Exception as e:
            logging.error(click.style(
                f"Error during reconciliation: {e}", fg="red"))
