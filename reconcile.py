import click
from src.app import Reconciler
from src.utils import validate_comaparison, validate_file
from dask.diagnostics import ProgressBar
@click.command(
    help="Generate a reconciling report between two CSV files"
)
@click.option("--source", "-s", type=click.Path(exists=True, dir_okay=True, readable=True), help="Path to the source CSV file", required=True,callback=validate_file)
@click.option("--target", "-t", type=click.Path(exists=True, dir_okay=True, readable=True), help="Path to the target CSV file", required=True,callback=validate_file)
@click.option("--output", "-o", type=click.File("w"), default="reconciliation_report.csv", help="Path to the output CSV file")
@click.option("--comparison-columns","-c",type=(str),multiple=True, help="Any other column to use in reconciliation",callback=validate_comaparison)

def run(source, target, output, comparison_columns):
    reconciler = Reconciler(source, target, output, comparison_columns)
    return reconciler.reconcile()
    
if __name__ == '__main__':
  with ProgressBar():
      run()

