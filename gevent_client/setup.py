
import sys
import os.path
import subprocess
import setuptools

def compile_protobuf():

    base_path = os.path.dirname(sys.argv[0])
    proto_in  = os.path.join(base_path, 'samoa.proto')
    build_out = os.path.join(base_path, 'build', 'generated', 'samoa', 'core', 'protobuf')

    if not os.path.exists(build_out):
        os.makedirs(build_out)

    cmd = 'protoc --cpp_out=%s %s' % (build_out, proto_in)
    print cmd
    assert not subprocess.call(cmd, shell = True)

    cmd = 'protoc --bplbindings_out=%s %s' % (build_out, proto_in)
    print cmd
    assert not subprocess.call(cmd, shell = True)

    cpp_sources = [
        os.path.join(build_out, 'samoa.pb.cc'),
        os.path.join(build_out, '_samoa.cpp')]

    build_samoa_out = os.path.join(build_out, 'samoa')
    for source in os.listdir(build_samoa_out):
        if source.endswith('.cpp'):
            cpp_sources.append(os.path.join(build_samoa_out, source))

    include_dir = os.path.join(base_path, 'build', 'generated')
    return include_dir, cpp_sources

include_path, cpp_sources = compile_protobuf()

setuptools.setup(
    name = 'samoa-client',
    version = '0.1',
    packages = ['samoa'],
    ext_modules = [
        setuptools.Extension('samoa.protobuf._samoa', cpp_sources,
            include_dirs = [include_path],
            libraries = ['boost_python', 'protobuf'])],
    install_requires = ['gevent'],
)
