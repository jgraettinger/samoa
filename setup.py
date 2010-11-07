
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
        libraries = ['boost_python'],
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

