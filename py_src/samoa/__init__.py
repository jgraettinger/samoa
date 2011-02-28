
from _samoa import *
del _samoa

# explictly load to ensure c++ conversions from pysamoa::future are registered
import coroutine

