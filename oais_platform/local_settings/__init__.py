"""
Override settings file for the Django project.

This package provides a way to override the settings of the project.
Useful to customize the settings when dealing with multiple deployments.
This is a package and not a single-file module so that it can be mounted
as a volume of docker/k8s.

It's supposed to be loaded *at the end* of the standard settings.py with
the `from local_settings import *` syntax.

See https://gitlab.cern.ch/digitalmemory/openshift-deploy for further
information on this setup and examples.
"""

__all__ = []
