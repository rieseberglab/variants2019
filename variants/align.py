import bunnies
import bunnies.unmarshall
import logging
import bunnies.config as config
from .constants import KIND_PREFIX, SAMPLE_NAME_RE

log = logging.getLogger(__name__)


class Align(bunnies.Transform):
    """
    Align a paired-end fastq or sra file against a reference genome
    """
    ALIGN_IMAGE = "rieseberglab/analytics:7-2.5.7"
    VERSION = "1"

    __slots__ = ("sample_name", "r1", "r2", "ref", "ref_idx")

    kind = KIND_PREFIX + "Align"

    def __init__(self, sample_name=None, r1=None, r2=None, ref=None, ref_idx=None, lossy=False, manifest=None):
        super().__init__("align", version=self.VERSION, image=self.ALIGN_IMAGE, manifest=manifest)

        if manifest is not None:
            inputs, params = manifest['inputs'], manifest['params']
            r1 = inputs['r1'].node
            # R2 is optional for SRAs
            r2 = inputs['r2'].node if 'r2' in inputs else None
            ref = inputs['ref'].node
            ref_idx = inputs['ref_idx'].node

            lossy = params['lossy']
            sample_name = params['sample_name']

        if None in (sample_name, r1, ref, ref_idx):
            raise Exception("invalid parameters for alignment")

        if not SAMPLE_NAME_RE.match(sample_name):
            raise ValueError("sample name %r does not match %s" % (
                sample_name, SAMPLE_NAME_RE.pattern))

        self.sample_name = sample_name
        self.r1 = r1
        self.r2 = r2
        self.ref = ref
        self.ref_idx = ref_idx

        self.add_input("r1", r1,    desc="forward reads")
        if r2:
            self.add_input("r2", r2,    desc="reverse reads")

        self.add_input("ref", ref,  desc="reference fasta")
        self.add_input("ref_idx", ref_idx, desc="reference index")
        self.params["lossy"] = bool(lossy)
        self.params["sample_name"] = sample_name

    @classmethod
    def task_template(cls, compute_env):
        scratchdisk = compute_env.get_disk('scratch')
        if not scratchdisk:
            raise Exception("Align tasks require a scratch disk")

        return {
            'jobtype': 'batch',
            'image': cls.ALIGN_IMAGE
        }

    def task_resources(self, **kwargs):
        # adjust resources based on inputs and job parameters
        return {
            'vcpus': 4,
            'memory': 12000,
            'timeout': 4*3600
        }

    def output_prefix(self, bucket=None):
        bucket = bucket or config['storage']['build_bucket']
        return "s3://%(bucket)s/%(name)s-%(version)s-%(sample_name)-%(cid)s/" % {
            'name': self.name,
            'bucket': bucket,
            'version': self.version,
            'sample_name': self.params['sample_name'],
            'cid': self.canonical_id
        }

    def run(self, **params):
        """ this runs in the image """
        import os
        import sys
        import tempfile
        import json

        def cache_remote_file(url, md5_digest, casdir):
            return bunnies.run_cmd([
                "cas", "-put", url, "-get", "md5:" + md5_digest, casdir
            ]).stdout.decode('utf-8').strip()

        workdir = params['workdir']
        s3_output_prefix = self.output_prefix()
        local_output_dir = os.path.join(workdir, "output")

        cas_dir = "/scratch/cas"
        os.makedirs(cas_dir, exist_ok=True)
        os.makedirs(local_output_dir, exist_ok=True)

        #
        # download reference in /scratch
        # /scratch is shared with other jobs in the same compute environment
        #
        ref_target = self.ref.ls()
        ref_idx_target = self.ref_idx.ls()
        ref_path = cache_remote_file(ref_target['url'], ref_target['digests']['md5'], cas_dir)
        _ = cache_remote_file(ref_idx_target['url'], ref_idx_target['digests']['md5'], cas_dir)

        align_args = [
            "align",
            "-cas", cas_dir
        ]
        if self.params['lossy']:
            align_args.append("-lossy")

        r1_target = self.r1.ls()
        r2_target = self.r2.ls() if self.r2 else None

        # write jobfile
        jobfile_doc = {
            self.params['sample_name']: {
                "name": self.params['sample_name'],
                "locations": [
                    [r1_target['url'], "md5:" + r1_target['digests']['md5']],
                    [r2_target['url'], "md5:" + r2_target['digests']['md5']] if r2_target else ["", ""]
                ]
            }
        }
        log.info("align job: %s", repr(jobfile_doc))
        with tempfile.NamedTemporaryFile(suffix=".job.txt", mode="wt",
                                         prefix=self.params['sample_name'], dir=workdir, delete=False) as jobfile_fd:
            json.dump(jobfile_doc, jobfile_fd)

        # num_threads = params['resources']['vcpus']
        align_args += [
            "-r", ref_path,
            "-i", jobfile_fd.name,
            "-o", s3_output_prefix,
            "-w", workdir,
            "-m",       # autodetect readgroup info
            "-d", "1",  # mark duplicates
            "-stats"
        ]

        bunnies.run_cmd(align_args, stdout=sys.stdout, stderr=sys.stderr, cwd=workdir)

        def _check_output_file(field, url, is_optional=False):
            try:
                meta = bunnies.utils.get_blob_meta(url)
                return {"size": meta['ContentLength'], "url": url}

            except bunnies.exc.NoSuchFile:
                if is_optional:
                    return None
                raise Exception("output %s missing: %s" % (field, url))

        sn = self.params['sample_name']

        def od(x):
            return os.path.join(s3_output_prefix, x)

        output = {
            "bam": _check_output_file("bam", "%s.bam" % od(sn)),
            "bamstats": _check_output_file("bamstats", "%s.bamstats.txt" % od(sn)),
            "bai": _check_output_file("bai", "%s.bai" % od(sn)),
            "illuminametrics": _check_output_file("illuminametrics", "%s.illuminametrics.txt" % od(sn)),
            "dupmetrics": _check_output_file("dupmetrics", "%s.dupmetrics.txt" % od(sn)),
            "bam_md5": _check_output_file("bam.md5", "%s.bam.md5" % od(sn))
        }

        return output


bunnies.unmarshall.register_kind(Align)
