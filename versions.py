#!/usr/bin/env python3
"""Recreate the versions.json file.

Create a temporary virtual environment in .venv, install doctr-versions-menu
and run it. The .venv folder is removed unless --keep-venv is given.

Usage:

        python3 versions.py [OPTIONS] [DVMVERSION]

OPTIONS:

        --keep-venv     Keep the .venv folder after completing the script
        --help          Display this help

DVMVERSION:

    By default, the latest stable release of the doctr-versions-menu package is
    used to generate versions.json. You may give a pip-compatible version
    specification, e.g. `doctr-versions-menu~=1.0` or
    'git+https://github.com/goerz/doctr_versions_menu.git@master#egg=doctr_versions_menu'
    as the last command line argument to specify another version. The latter
    example for using the master version can also be achieved by specifying
    `doctr-versions-menu==master`
"""
# This script is intended to be placed in the root of a project's gh-pages
# branch
import os
import shutil
import subprocess
import sys
import venv
from pathlib import Path


DOCTR_VERSIONS_ENV_VARS = {}  # set by doctr-versions-menu

DVM_REPO = 'git+https://github.com/goerz/doctr_versions_menu.git'


def main(argv=None):
    """Recreate the versions.json file."""
    if argv is None:
        argv = sys.argv
    if '--help' in argv:
        print(__doc__)
        return 0
    dvm_version = 'doctr-versions-menu'
    if not argv[-1].endswith('versions.py') and not argv[-1].startswith('--'):
        dvm_version = argv.pop()
    if dvm_version.endswith('=master'):
        dvm_version = DVM_REPO + '@master#egg=doctr_versions_menu'
    venvdir = Path(__file__).parent / '.venv'
    builder = venv.EnvBuilder(with_pip=True)
    builder.create(venvdir)
    env = DOCTR_VERSIONS_ENV_VARS.copy()
    env.update(os.environ)  # overrides DOCTR_VERSIONS_ENV_VARS
    try:
        subprocess.run(
            [Path('.venv') / 'bin' / 'pip', 'install', dvm_version],
            cwd=venvdir.parent,
            check=True,
        )
        subprocess.run(
            [Path('.venv') / 'bin' / 'doctr-versions-menu', '--debug'],
            cwd=venvdir.parent,
            check=True,
            env=env,
        )
        return 0
    except subprocess.CalledProcessError:
        return 1
    finally:
        if '--keep-venv' not in argv:
            if venvdir.is_dir():
                shutil.rmtree(venvdir)


if __name__ == "__main__":
    sys.exit(main())
