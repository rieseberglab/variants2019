import bunnies
import bunnies.unmarshall
import logging
import bunnies.config as config

from .constants import KIND_PREFIX, SAMPLE_NAME_RE

log = logging.getLogger(__name__)


class Genotype(bunnies.Transform):
    """
    Call HaplotypeCaller on the input.
    """
    GENOTYPE_IMAGE = "rieseberglab/analytics:9-3.0.0"
    VERSION = "1"

    __slots__ = ("sample_name", "sample_bam", "ref", "ref_idx")
    kind = KIND_PREFIX + "Genotype"

    def __init__(self, sample_name=None, sample_bam=None, bgzip=True,
                 hc_options=None, merge_options=None, manifest=None):
        super().__init__("genotype", version=self.VERSION, image=self.GENOTYPE_IMAGE)
        """
        Run HaplotypeCaller on all reads for a given sample bam.
        bgzip=True|False whether the output is block-gzipped/indexed
        hc_options=[ list of extra arguments to pass to haplotype caller ]
        merge_options=[ list of extra arguments to pass to gathergvcfs ]
        """

        ref = None
        ref_idx = None

        if manifest is not None:
            inputs, params = manifest['inputs'], manifest['params']
            sample_name = params.get('sample_name')
            sample_bam = inputs['sample_bam'].node
            ref = sample_bam.ref
            ref_idx = sample_bam.ref_idx

            bgzip = params['bgzip']
            sample_name = params['sample_name']
            merge_options = params['merge_options']
            hc_options = params['hc_options']

        if not SAMPLE_NAME_RE.match(sample_name):
            raise ValueError("sample name %r does not match %s" % (
                sample_name, SAMPLE_NAME_RE.pattern))

        if not sample_bam:
            raise ValueError("genotyping requires 1 aligned bam input")
        if not sample_name:
            raise ValueError("you must specify the sample name")

        if not ref:
            ref = sample_bam.ref
        if not ref_idx:
            ref_idx = sample_bam.ref_idx

        if not ref or not ref_idx:
            raise ValueError("genotyping requires reference and index")

        # save params
        self.params["bgzip"] = bool(bgzip)
        self.sample_name = self.params["sample_name"] = sample_name
        self.sample_bam = sample_bam
        self.add_input("sample_bam", sample_bam, desc="aligned reads for sample %s" % (sample_name,))
        self.ref, self.ref_idx = ref, ref_idx
        self.params['merge_options'] = list(merge_options) if merge_options else []
        self.params['hc_options'] = list(hc_options) if hc_options else []

    @classmethod
    def task_template(cls, compute_env):
        scratchdisk = compute_env.get_disk('scratch') or compute_env.get_disk('localscratch')
        if not scratchdisk:
            raise Exception("Merge tasks require a scratch disk")

        return {
            'jobtype': 'batch',
            'image': cls.GENOTYPE_IMAGE
        }

    def task_resources(self, attempt=1, **kwargs):
        ref_target = self.ref.ls()
        bam_target = self.sample_bam.ls()

        ref_size = ref_target['size']
        bam_size = bam_target['bam']['size']
        gbs = (ref_size + bam_size) / (1024 * 1024 * 1024)

        log.info("genotyping %s: %5.3f gbs of input data", self.params['sample_name'], gbs)

        # FIXME -- if the failures are for timeouts, raise the time, not the
        #          ram/cpu.
        if attempt == 1:
            return {
                'vcpus': 28,
                'memory': 156 * 1024,
                'timeout': max(int(gbs*(40*60)), 3600) # 30m per gb (min 1h)
            }
        elif attempt == 2:
            # less concurrency -- same memory
            return {
                'vcpus': 24,
                'memory': 156 * 1024,
                'timeout': max(int(gbs*(40*60)), 3600)
            }
        elif attempt == 3:
            # conservative amount of threads -- way more memory
            return {
                'vcpus': 32,
                'memory': 240 * 1024,
                'timeout': max(int(gbs*(40*60)), 3600)
            }
        else:
            # cpu waste -- but high available memory
            return {
                'vcpus': 20,
                'memory': 240 * 1024,
                'timeout': max(int(gbs*(40*60)), 3600)
            }

    def run(self, resources=None, **params):
        """ this runs in the image """
        import os
        import os.path
        import sys

        def cache_remote_file(url, md5_digest, casdir):
            return bunnies.run_cmd([
                "cas", "-put", url, "-get", "md5:" + md5_digest, casdir
            ]).stdout.decode('utf-8').strip()

        workdir = params['workdir']

        s3_output_prefix = self.output_prefix()

        local_input_dir = os.path.join(workdir, "input")
        local_output_dir = os.path.join(workdir, "output")

        os.makedirs(local_output_dir, exist_ok=True)
        os.makedirs(local_input_dir, exist_ok=True)

        cas_dir = "/localscratch/cas"
        os.makedirs(cas_dir, exist_ok=True)

        #
        # download reference in scratch space shared with other jobs
        # in the same compute environment
        #
        ref_target = self.ref.ls()
        ref_idx_target = self.ref_idx.ls()
        ref_path = cache_remote_file(ref_target['url'], ref_target['digests']['md5'], cas_dir)
        _ = cache_remote_file(ref_idx_target['url'], ref_idx_target['digests']['md5'], cas_dir)
        bam_target = self.sample_bam.ls()

        log.info("genotyping BAM sample %s: bam=%s (size=%5.3fGiB)...",
                 self.params, bam_target['bam']['url'], bam_target['bam']['size']/(1024*1024*1024))

        # download the bai too
        bam_path = os.path.join(local_input_dir, os.path.basename(bam_target['bam']['url']))
        bai_path = os.path.join(local_input_dir, os.path.basename(bam_target['bai']['url']))
        bunnies.transfers.s3_download_file(bam_target['bam']['url'], bam_path)
        bunnies.transfers.s3_download_file(bam_target['bai']['url'], bai_path)

        num_threads = resources['vcpus']
        memory_mb = resources['memory']

        mb_per_worker = (memory_mb - 200) // num_threads
        java_heap = "-Xmx%dm" % (mb_per_worker,)
        vc_args = [
            "vc",
            "-o", s3_output_prefix,
            "-i", bam_path,
            "-w", workdir,
            "-r", ref_path,
            "-n", str(num_threads),
            "-minbp", "0",
            "-nsegments", str(num_threads*5),
            "-bgzip",
            "-gatk4",
            "-javaoptions", java_heap
        ]
        if self.params['hc_options']:
            vc_args += ["-vcoptions", " ".join(self.params['hc_options'])]

        bunnies.run_cmd(vc_args, stdout=sys.stdout, stderr=sys.stderr, cwd=workdir)
        bunnies.run_cmd(["ls", "-lh",  local_output_dir], stdout=sys.stdout, stderr=sys.stderr, cwd=workdir)

        def _check_output_file(fname, is_optional=False):
            try:
                output_url = os.path.join(s3_output_prefix, fname)
                meta = bunnies.get_blob_meta(output_url)
                return {
                    "size": meta['ContentLength'],
                    "url": output_url,
                    "etag": meta['ETag']
                }
            except FileNotFoundError:
                if is_optional:
                    return None
                raise Exception("missing file: " + output_url)

        pfx = self.sample_name
        output = {
            "gvcf":          _check_output_file(pfx + ".g.vcf.gz", True),
            "gvcf_idx":      _check_output_file(pfx + ".g.vcf.gz.tbi", True),
            "input_bed":     _check_output_file(pfx + ".input.bed", False),
            "output_bed":    _check_output_file(pfx + ".scatter.bed", True),
            "scatter_log":   _check_output_file(pfx + ".scatter.log", True)
        }
        return output

    def output_prefix(self, bucket=None):
        bucket = bucket or config['storage']['build_bucket']
        is_allele_specific = "AS_StandardAnnotation" in self.params['hc_options']

        return "s3://%(bucket)s/%(name)s.%(version)s-%(sample_name)s-AS%(allelespec)s-%(cid)s/" % {
            'name': self.name,
            'allelespec': "1" if is_allele_specific else "0",
            'bucket': bucket,
            'version': self.version,
            'sample_name': self.sample_name,
            'cid': self.canonical_id
        }


bunnies.unmarshall.register_kind(Genotype)
