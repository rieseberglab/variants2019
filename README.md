
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

Bunnies Configuration
=====================

The variant calling scripts make use of bunnies extensively, so you
will need to configure your bunnies settings for the files to be
created and written in the right place.

You need the following required files in the root of the repository. If you have already created
these files in a different bunnies project, you can copy them over to this project:

   - `cluster-settings.json`: created by running `bunnies/scripts/setup-tasks.sh`.
   - `storage-settings.json`: created manually.
   - `environment-settings.json`: created via `bunnies env setup`
   - `key-pair-settings.json`: created by running `bunnies/scripts/setup-key-pair.sh`.
   - `network-settings.json`: created by running `bunnies/scripts/setup-network.sh`

### storage-settings.json

A very important setting is the location where the files produced by the build will be output.
This is done with the `storage-settings.json` file. It looks as follows:

```
{
	"storage": {
        "tmp_bucket": "reprod-temp-bucket",
        "write_url": "s3://ubc-sunflower-genome/build/",
	    "read_urls": ["s3://rieseberg-bunnies-build/"]
    }
}
```

The `tmp_bucket` key points to a bucket in which temporary uploaded
files will go. The files in there can be periodically removed a few
days after they have been created.

The `write_url` designates the location where the any newly created
output will go.

The `read_urls` is an list of s3 prefixes where bunnies should look
for finding existing object that were built on previous runs.

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


Examples
=========

Build bams for all the samples provided. If there are multiple sequences for a given sample name, they will
be merged into a single output. At the end of the run, the final outputs will be printed.

    python -m variants SAMPLESJSON --computeenv myenv --dry-run --stage bam --reference ha412
