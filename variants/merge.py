import bunnies
import bunnies.unmarshall
import logging
import bunnies.config as config

from .constants import KIND_PREFIX, SAMPLE_NAME_RE

log = logging.getLogger(__name__)


class Merge(bunnies.Transform):
    """
    merge one or more bam files and modify the readgroup with the
    provided information. bams are merged in the order provided.
    """
    MERGE_IMAGE = "rieseberglab/analytics:7-2.5.8"
    VERSION = "1"

    __slots__ = ("sample_name",)
    kind = KIND_PREFIX + "Merge"

    def __init__(self, sample_name=None, aligned_bams=None, manifest=None):
        super().__init__("merge", version=self.VERSION, image=self.MERGE_IMAGE)

        if manifest is not None:
            inputs, params = manifest['inputs'], manifest['params']
            sample_name = params.get('sample_name')
            aligned_bams = []
            for i in range(0, params.get('num_bams')):
                aligned_bams.append(inputs.get(str(i)).node)

        if not SAMPLE_NAME_RE.match(sample_name):
            raise ValueError("sample name %r does not match %s" % (
                sample_name, SAMPLE_NAME_RE.pattern))

        self.sample_name = sample_name
        self.params["sample_name"] = sample_name
        self.params["num_bams"] = len(aligned_bams)

        if not aligned_bams:
            raise ValueError("merging requires 1 or more aligned bam inputs")
        if not sample_name:
            raise ValueError("you must specify the sample name to write")

        # fixme verify that all bams have the same reference
        for i, bam in enumerate(aligned_bams):
            # print(self.sample_name, bam)
            self.add_input(str(i), bam, desc="aligned input #%d" % (i,))

    def get_reference(self):
        if not self.inputs:
            return None
        bam0 = self.inputs["0"].node
        return bam0.ref

    @classmethod
    def task_template(cls, compute_env):
        scratchdisk = compute_env.get_disk('scratch')
        if not scratchdisk:
            raise Exception("Merge tasks require a scratch disk")

        return {
            'jobtype': 'batch',
            'image': cls.MERGE_IMAGE
        }

    def task_resources(self, **kwargs):
        # adjust resources based on inputs and job parameters
        return {
            'vcpus': 2,
            'memory': 4000,
            'timeout': 1*3600
        }

    def run(self, **params):
        """ this runs in the image """
        import os
        import os.path
        import sys

        workdir = params['workdir']

        s3_output_prefix = self.output_prefix()

        local_output_dir = os.path.join(workdir, "output")
        local_input_dir = os.path.join(workdir, "input")

        # download input samples
        os.makedirs(local_output_dir, exist_ok=True)
        os.makedirs(local_input_dir, exist_ok=True)

        all_srcs = []
        all_dests = []
        for inputi, inputval in self.inputs.items():
            aligned_target = inputval.ls()
            bam_src, bam_dest = aligned_target['bam']['url'], os.path.join(local_input_dir, "input_%s.bam" % (inputi,))
            bai_src, bai_dest = aligned_target['bai']['url'], os.path.join(local_input_dir, "input_%s.bai" % (inputi,))
            bunnies.transfers.s3_download_file(bai_src, bai_dest)
            bunnies.transfers.s3_download_file(bam_src, bam_dest)
            all_srcs.append({"bam": bam_src, "bai": bai_src})
            all_dests += [bam_dest, bai_dest]

        merge_args = [
            os.path.join(params["scriptdir"], "scripts", "lane_merger.sh"),
            "--samtools", "/usr/bin/samtools",
            "--sambamba", "/usr/local/bin/sambamba_v0.6.6",
            "--samplename", self.sample_name,
            "--tmpdir",   workdir,
            "--delete-old",
            os.path.join(local_output_dir, self.sample_name) + ".bam",  # output.bam
        ] + all_dests

        bunnies.run_cmd(merge_args, stdout=sys.stdout, stderr=sys.stderr, cwd=workdir)

        with open(os.path.join(local_output_dir, self.sample_name + ".bam.merged.txt"), "w") as merge_manifest:
            for src in all_srcs:
                merge_manifest.write("\t".join([
                    self.sample_name,
                    src['bam'],
                    os.path.join(s3_output_prefix, self.sample_name + ".bam")
                ]) + "\n")

        bunnies.run_cmd(["find", local_output_dir], stdout=sys.stdout, stderr=sys.stderr, cwd=workdir)
        pfx = self.sample_name

        def _check_output_file(fname, is_optional=False):
            try:
                inpath = os.path.join(local_output_dir, fname)
                output_url = os.path.join(s3_output_prefix, fname)
                st_size = os.stat(inpath).st_size
                bunnies.transfers.s3_upload_file(inpath, output_url)
                return {"size": st_size, "url": output_url}
            except FileNotFoundError:
                if is_optional:
                    return None
                raise Exception("missing file: " + inpath)

        output = {
            "bam": _check_output_file(pfx + ".bam", False),
            "bai": _check_output_file(pfx + ".bam.bai", False),
            "bam_md5": _check_output_file(pfx + ".bam.md5", False),
            "dupmetrics": _check_output_file(pfx + ".dupmetrics.txt", True),
            "bamstats": _check_output_file(pfx + ".bamstats.txt", False),
            "merge_manifest": _check_output_file(pfx + ".bam.merged.txt", False)
        }
        return output


    def output_prefix(self, bucket=None):
        bucket = bucket or config['storage']['build_bucket']
        return "s3://%(bucket)s/%(name)s.%(version)s-%(sample_name)s-%(cid)s/" % {
            'name': self.name,
            'bucket': bucket,
            'version': self.version,
            'sample_name': self.params['sample_name'],
            'cid': self.canonical_id
        }

bunnies.unmarshall.register_kind(Merge)
