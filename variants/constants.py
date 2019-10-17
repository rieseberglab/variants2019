import re

KIND_PREFIX = "variants."


# Valid sample name
SAMPLE_NAME_RE = re.compile("^[a-zA-Z0-9._-]+$")
