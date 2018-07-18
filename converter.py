import os
import sys
import click
import pandas as pd
import subprocess
import pathlib

## Will make this configurable
changestovcf_exec = './changestovcf.pl'

## Will make this configurable
## Can make this file type specific
##
COLUMNS = ['ChangeUID', 'IndexString', 'Coverage', 'MinQualityScore', 'MaxQualityScore', 'AverageQualityScore',
           'DistinctCoverage', 'MutPct', 'Chrom', 'Start', 'End', 'BaseFrom', 'BaseTo', 'LookupKey', 'ENSG',
           'GeneName']

## Will make this configurable
## The number of rows that are considered the header portion
## of the file and should be stripped prior to invoking the
## changestovcf executable.
HEADER_ROW_COUNT = 1

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

    filtered_columns_df = pd.read_table(infile, sep='\t', low_memory=False, usecols=COLUMNS)

    if verbose:
        print(filtered_columns_df)

    outfile = os.path.join(outdir, sample_id + "_" + file_type + ".csv")

    filtered_columns_df.to_csv(outfile, index=False)

    print("Wrote output file CSV file '%s'" % outfile)


    line_count = get_file_line_count(infile)

    tail_count = line_count -1

    out_vcf_file = os.path.join(outdir, sample_id + "_" + file_type + ".vcf")
    
    run_changestovcf(outfile, out_vcf_file, tail_count, verbose)

    if create_symlink:

        symlink_file  = outdir + '/' + infile_basename

        create_infile_symlink(infile, symlink_file)
    
        
if __name__ == "__main__":
    main()
