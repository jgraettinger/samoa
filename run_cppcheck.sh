#!/bin/sh
cppcheck -j8 -q  --enable=all  -I cpp_src/ -I build/generated/cpp_src/ cpp_src/ bindings/
