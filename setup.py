
import ez_setup
ez_setup.use_setuptools()

import os
import setuptools
def enum_files(base_dir, *extension):
    return [os.path.join(base_dir, i) for i in \
        os.listdir(base_dir) if i.endswith(extension)]

extensions = [

    # samoa._samoa
    setuptools.extension.Extension(
        name = 'samoa._samoa',
        sources = enum_files('samoa', '.cpp'),
        include_dirs = ['.'],
        libraries = ['boost_python', 'boost_system'],
        extra_compile_args = ['-O0', '-g'],
    ),

    # samoa._core
    setuptools.extension.Extension(
        name = 'samoa.core._core',
        sources = enum_files('samoa/core', '.cpp'),
        include_dirs = ['.'],
        libraries = ['boost_python', 'boost_system'],
        extra_compile_args = ['-O0', '-g'],
    ),

    # samoa._server
    setuptools.extension.Extension(
        name = 'samoa.server._server',
        sources = enum_files('samoa/server', '.cpp'),
        include_dirs = ['.'],
        libraries = ['boost_python', 'boost_system'],
        extra_compile_args = ['-O0', '-g'],
    ),
]

setuptools.setup(

    name = 'Samoa',
    version = '0.1',
    packages = ['samoa'],
    tests_require = ['nose'],
    ext_modules = extensions,
)

