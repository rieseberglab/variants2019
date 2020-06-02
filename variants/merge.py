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

        # verify that all bams have the same reference
        ref = None
        ref_idx = None

        for i, bam in enumerate(aligned_bams):

            if ref is None:
                ref = (i, bam.ref)
            else:
                if bam.ref != ref[1]:
                    log.error("%s %s", bam.ref, repr(bam.ref))
                    raise ValueError("input %d has a different reference than input %d" % (i, ref[0]))

            if ref_idx is None:
                ref_idx = (i, bam.ref_idx)
            else:
                if bam.ref_idx != ref_idx[1]:
                    raise ValueError("input %d has a different reference index than input %d" % (i, ref_idx[0]))

            self.add_input(str(i), bam, desc="aligned input #%d" % (i,))

    @property
    def ref(self):
        if not self.inputs:
            return None
        bam0 = self.inputs["0"].node
        return bam0.ref

    @property
    def ref_idx(self):
        if not self.inputs:
            return None
        bam0 = self.inputs["0"].node
        return bam0.ref_idx

    @classmethod
    def task_template(cls, compute_env):
        scratchdisk = compute_env.get_disk('scratch') or compute_env.get_disk('localscratch')
        if not scratchdisk:
            raise Exception("Merge tasks require a scratch disk")

        return {
            'jobtype': 'batch',
            'image': cls.MERGE_IMAGE
        }

    def task_resources(self, attempt=1, **kwargs):
        input_size = 0
        for inputi, inputval in self.inputs.items():
            aligned_target = inputval.ls()
            bam_url, bam_size = aligned_target['bam']['url'], aligned_target['bam']['size']
            input_size += bam_size

        gbs = float(input_size) / (1024*1024*1024)

        log.info("merge %s has %5.3f gbs of input", self.params['sample_name'], gbs)

        if self.params['num_bams'] <= 1:
            # trivial merge -- only rewrites headers (if necessary) and compute md5
            return {
                'vcpus': 2,
                'memory': 4000,
                'timeout': max(int(gbs*(5*60)), 3600) # 5 min per gb (min 1h)
            }

        # combine all bams + make headers + mark dups + sort
        # give an extra 20GB memory for each successive attempt
        return {
            'vcpus': 8,
            'memory': min(int(16000 * self.params['num_bams']), 62*1024) + (20*1024*(attempt - 1)),
            'timeout': max(int(gbs*(20*60)), 3600) # 20m per gb (min 1h)
        }

    def run(self, resources=None, **params):
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

        num_threads = resources['vcpus']
        memory_mb = resources['memory']
        merge_args = [
            os.path.join(params["scriptdir"], "scripts", "lane_merger.sh"),
            "--samtools", "/usr/bin/samtools",
            "--sambamba", "/usr/local/bin/sambamba_v0.6.6",
            "--samplename", self.sample_name,
            "--tmpdir",   workdir,
            "--threads", str(num_threads),
            "--maxheap", str(memory_mb - 200),
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

        bunnies.run_cmd(["ls", "-lh",  local_output_dir], stdout=sys.stdout, stderr=sys.stderr, cwd=workdir)
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


    def output_prefix(self, write_url=None):
        return "%(repo)s%(name)s.%(version)s-%(sample_name)s-%(cid)s/" % {
            'repo': self.repo_path(write_url=write_url),
            'name': self.name,
            'version': self.version,
            'sample_name': self.params['sample_name'],
            'cid': self.canonical_id
        }

bunnies.unmarshall.register_kind(Merge)
