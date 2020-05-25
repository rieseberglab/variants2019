
This repository contains scripts to call variants on AWS with the
bunnies platform.


Setup
================

1. Setup your AWS credentials setup properly. See Bunnies README for details.

       export AWS_PROFILE=<myawsprofile>

1. If this is the first time you download this repo, initialize submodules

       git submodule init
	   git submodule update --recursive

1. Setup your python pip environment to install python dependencies. Follow the    instructions inside `requirements.txt`. When you use the variant calling
   scripts, make sure this environment is activated.


Experiment setup
=================

The variant calling scripts will use the AWS platform to create a snp call.
The main script is `variants/__main__.py`. You can invoke it, assuming your
python environment is setup correctly, with:

    python -m variants --help

1. _SAMPLENAMES.tsv_: The first thing you will need to specify is the list of sample names that you
want to include in your snp call. Create a simple tab-separated file in which the first column is the name of a sample to include. The list of all known FASTQ sample names is available as the `SAMPLENAME` column in `sample-info/samples/sequence_sources_apr_2020.tsv`. You can also use `sample-info/samples/species_info_apr_2020.json`, which provides a bit more information, like the name of the species for that sample.

1. _SAMPLES.JSON_: You will need to convert that list of sample names (from 1.) into a format that the variant calling script can understand. For this purpose, you use `scripts/inputs-by-sample-name.py`:

       ./scripts/inputs-by-sample-name.py SAMPLENAMES.tsv > SAMPLES.JSON


