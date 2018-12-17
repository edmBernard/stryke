"""
    A Python Binding of the Stryke Library https://github.com/edmBernard/Stryke. It allow to write Orc file defined https://orc.apache.org
"""

__version__ = "0.1"

from .stryke import template
from .stryke import multifile
from .stryke import thread
from .stryke import custom
from .stryke import WriterOptions

__all__ = ['thread', 'multifile', 'template', 'custom', 'WriterOptions']
