import os
import sys
import click
import pandas as pd
import subprocess
import pathlib
import csv
import pprint

use_pandas = False

## Will make this configurable
changestovcf_exec = './changestovcf.pl'

## Will make this configurable
## Can make this file type specific
##
COLUMNS = ['ChangeUID', 'IndexString', 'Coverage', 'MinQualityScore', 'MaxQualityScore', 'AverageQualityScore',
           'DistinctCoverage', 'MutPct', 'Chrom', 'Start', 'End', 'BaseFrom', 'BaseTo', 'LookupKey', 'ENSG',
           'GeneName']

QUALIFIED_COLUMNS_LOOKUP = None

## Will make this configurable
## The number of rows that are considered the header portion
## of the file and should be stripped prior to invoking the
## changestovcf executable.
HEADER_ROW_COUNT = 1

def get_qualified_columns_lookup():
    """
    """
    lookup = {}

    for column_name in COLUMNS:
        lookup[column_name] = True

    return lookup

def filter_file_using_pandas(infile, outfile, verbose):

    filtered_columns_df = pd.read_table(infile, sep='\t', low_memory=False, usecols=COLUMNS)

    if verbose:
        print(filtered_columns_df)

    filtered_columns_df.to_csv(outfile, index=False)

    print("Wrote output file CSV file '%s'" % outfile)


def filter_file_without_pandas(infile, outfile, verbose):

    line_ctr = 0;

    qualified_column_lookup = get_qualified_columns_lookup()

    qualified_column_number_lookup = {}

    qualified_column_number_list = []

    filtered_record_list = []

    qualified_header_list = []

    # pp = pprint.PrettyPrinter(width=41, compact=True)

    # pp.pprint(qualified_column_lookup)

    with open(infile, 'r') as csvfile:

        reader = csv.reader(csvfile, delimiter='\t')

        row_ctr = 0

        for row in reader:

            row_ctr += 1

            # print("Row number %d" %  row_ctr)

            if row_ctr == 1:

                # This is the header so want to derive the column numbers for our
                # fields of interest.
                header_ctr = 0;

                for header in row:

                    if header in qualified_column_lookup:

                        qualified_column_number_lookup[header_ctr] = header

                        qualified_column_number_list.append(header_ctr)

                        qualified_header_list.append(header)

                    header_ctr += 1

                # pp.pprint(qualified_column_number_list)
                # pp.pprint(qualified_column_number_lookup)
                continue

            else:

                # print("Processing line %d" % row_ctr)

                record = []

                for field_num in qualified_column_number_list:

                    record.append(row[field_num])

                filtered_record_list.append(record)


    with open(outfile, 'w') as csvfile:

        writer = csv.writer(csvfile, delimiter='\t')

        writer.writerow(qualified_header_list)

        for row in filtered_record_list:

            writer.writerow(row)

    print("Wrote output file CSV file '%s'" % outfile)




def get_file_line_count(fname):
    """

    :param fname: file for which line count should be determined

    :return: int
    """
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def run_changestovcf(infile, outfile, count, verbose):

    cmd = "tail -" + str(count) + ' ' + infile + ' | perl ' + changestovcf_exec + ' > ' + outfile
    if verbose:
        print("Will attempt to execute : '%s'" % cmd)

    p = subprocess.Popen(cmd, shell=True)

    (stdout, stderr) = p.communicate()

    p_status = p.wait()

    if p_status == 0:
        print("VCF file '%s' is ready" % outfile)
    else:
        print("Encountered some problem during invocation of '%s' : %s " % (changestovcf_exec, stderr))

def create_infile_symlink(infile, symlink_file):
    """
    :param infile: The source file
    :param symlink_file: The destination file
    """

    try:
        os.symlink(infile, symlink_file)
    except:
        print("Encountered some error while attempt to create symlink %s -> %s" % (infile, symlink_file))
        raise



@click.command()
@click.argument('infile')
@click.argument('sample_id')
@click.option('--verbose', default=False, is_flag=True)
@click.option('--create_symlink', default=True, is_flag=True)
@click.option('--outdir', default='./')
@click.option('--header_row_count', default=1)
def main(infile, sample_id, verbose, create_symlink, outdir, header_row_count):
    """

    INFILE : The input file to be processed.

    SAMPLE_ID: The sample identifier.


    :return: None


    """
    assert isinstance(infile, str)
    assert isinstance(sample_id, str)
    assert isinstance(header_row_count, int)

    if not outdir == './':
        pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
        if verbose:
            print("output directory '%s' was created" %  outdir)


    if not os.path.isfile(infile):
        print("'%s' is not a file" % infile)
        sys.exit(1)


    if verbose:
        print("The input file is %s" % infile)
        print("The sample_id is %s" % sample_id)

    infile_basename = os.path.basename(infile)

    infile_basename_lc = infile_basename.lower()

    file_type = 'Unknown'

    if "allchanges.txt" in infile_basename_lc:
        file_type = 'Unfiltered'
    elif "plasmachanges.txt" in infile_basename_lc:
        file_type = 'Filtered'
    elif "vv_changes.txt" in infile_basename_lc:
        file_type = 'MAF-Filtered'
    else:
        print("Unsupported file type")

    if verbose:
        print("Found file type '%s'" % file_type)


    if verbose:
        print("Including only the following column names:")
        for name in COLUMNS:
            print(name)

    # Retrieve subset of columns filtered by column
    # names specified in the columns list.

    outfile = os.path.join(outdir, sample_id + "_" + file_type + ".csv")

    if use_pandas:
        filter_file_using_pandas(infile, outfile, verbose)
    else:
        filter_file_without_pandas(infile, outfile, verbose)


    line_count = get_file_line_count(infile)

    tail_count = line_count -1

    out_vcf_file = os.path.join(outdir, sample_id + "_" + file_type + ".vcf")

    run_changestovcf(outfile, out_vcf_file, tail_count, verbose)

    if create_symlink:

        symlink_file  = outdir + '/' + infile_basename

        create_infile_symlink(infile, symlink_file)


if __name__ == "__main__":
    main()
