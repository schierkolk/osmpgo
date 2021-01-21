from setuptools import setup, find_packages
from setuptools.command.install import install
import subprocess
import platform
import sys
import versioneer


def get_virtualenv_path():
    """Used to work out path to install compiled binaries to."""
    if hasattr(sys, 'real_prefix'):
        return sys.prefix

    if hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix:
        return sys.prefix

    if 'conda' in sys.prefix:
        return sys.prefix

    return None


def compile_and_install_software():
    """Used the subprocess module to compile/install the C software."""
    src_path = 'osmpgo/osmconvert/osmconvert.c'
    venv = get_virtualenv_path()
    if platform.system() == 'Linux':
        print('Linux')
        cmd = f'cc {src_path} -lz -O3 -o {venv}/bin/osmconvert'
        try:
            print(cmd)
            subprocess.check_call(cmd, shell=True)

        except subprocess.CalledProcessError as e:
            print(e.output)
            print(cmd)


class CustomInstall(install):
    """Custom handler for the 'install' command."""
    def run(self):
        compile_and_install_software()
        super().run()


setup(
    name='osmpgo',
    version=versioneer.get_version(),
    author="Chris Schierkolk",
    author_email="Chris.Schierkolk@maxar.com",
    description="Mutliprocessing for OSM creation",
    packages=find_packages(),
    cmdclass={'install': CustomInstall,
              'get_cmdclass': versioneer.get_cmdclass()},
    include_package_data = True,
    package_data={'': ['data/*.csv']},
    install_requires=[
        'Click',
        'geopandas',
    ],
    entry_points='''
        [console_scripts]
        osmpgo=osmpgo.cli:cli
    ''',
)
