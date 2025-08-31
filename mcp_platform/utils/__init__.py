"""
MCP Platform Utilities
"""

import os
from pathlib import Path
from typing import List

# Directory constants
ROOT_DIR = Path(__file__).parent.parent.parent
PACKAGE_DIR = Path(__file__).parent.parent
TEMPLATES_DIR = PACKAGE_DIR / "template" / "templates"
TESTS_DIR = ROOT_DIR / "tests"


def get_custom_templates_dir() -> Path:
    """Get custom templates directory from environment variable.

    Returns:
        Path to custom templates directory or None if not set
    """
    custom_dir = os.environ.get("MCP_CUSTOM_TEMPLATES_DIR")
    if custom_dir:
        return Path(custom_dir).expanduser().resolve()
    return None


def get_all_template_directories() -> List[Path]:
    """Get all template directories (built-in + custom).

    Returns:
        List of Path objects for template directories, with custom directory first
        if it exists to allow override behavior
    """
    directories = []

    # Add custom templates directory first (for override precedence)
    custom_dir = get_custom_templates_dir()
    if custom_dir and custom_dir.exists():
        directories.append(custom_dir)

    # Add built-in templates directory
    if TEMPLATES_DIR.exists():
        directories.append(TEMPLATES_DIR)

    return directories


# Note: Visual formatting utilities have been moved to
# mcp_platform.core.response_formatter
# Import them directly from there to avoid circular dependencies


class SubProcessRunDummyResult:
    """
    Mimics subprocess.run command's dummy response
    """

    def __init__(self, args=None, returncode=0, stdout=None, stderr=None):
        self.args = args
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    def check_returncode(self):
        if self.returncode != 0:
            from mcp_platform.utils.sh_compat import ShCalledProcessError
            raise ShCalledProcessError(
                self.returncode, self.args, self.stdout, self.stderr
            )
