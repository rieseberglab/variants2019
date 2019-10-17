# -*- charset: utf-8; -*-

import bunnies
import sys
import logging

from .align import Align
from .merge import Merge


log = logging.getLogger(__package__)


def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""
    root = logging.getLogger(__package__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)


def InputFile(url, desc="", digests=None):
    """
    Factory method to wrap various file URL forms into a Bunnies file
    """
    if url.startswith("s3://"):
        return bunnies.S3Blob(url, desc=desc, digests=digests)
    else:
        return bunnies.ExternalFile(url, desc=desc, digests=digests)
