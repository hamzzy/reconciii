import click
import csv
import os
from Levenshtein import distance


def standardize_value(value):
    return str(value).strip().lower()


def validate_file(ctx, param, value):
   
    if not is_csv_file(value):
        click.echo(click.style(
            f"Invalid file format. Expected a CSV file.", fg="red"))
    return value

def is_csv_file(file_path):
    try:
        with open(file_path, 'r') as file:
            dialect = csv.Sniffer().sniff(file.read(4096))
            return  True
    except csv.Error:
        return False
def fuzzy_match(source_value, target_value, threshold=0.9):
    standardized_source_value = standardize_value(source_value)
    standardized_target_value = standardize_value(target_value)

    levenshtein_distance = distance(
        standardized_source_value, standardized_target_value)
    max_distance = max(len(standardized_source_value), len(
        standardized_target_value)) * (1 - threshold)

    return levenshtein_distance <= max_distance


def validate_comaparison(ctx, param, value):
    if value:
        if ("ID" or "id") in value[0].split(","):
            raise click.BadParameter(click.style(
                f"ID already use as inital value for comparison", fg="red"))
        else:
            return value[0].split(",")
