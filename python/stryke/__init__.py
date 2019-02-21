"""
    A Python Binding of the Stryke Library https://github.com/edmBernard/Stryke. It allow to write Orc file defined https://orc.apache.org
"""

__version__ = "0.1"

from .stryke import simple
from .stryke import dispatch
from .stryke import sequential
from .stryke import thread
from .stryke import custom
from .stryke import WriterOptions

__all__ = ['simple', 'dispatch', 'sequential', 'thread', 'custom', 'WriterOptions']
