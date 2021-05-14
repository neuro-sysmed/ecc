from setuptools import setup
import json
import glob

def readme():
    with open('README.rst') as f:
        return f.read()

def get_version():
    with open('version.json') as json_file:
        data = json.load(json_file)

    if 'dev' in data and data['dev']:
        return "{}.{}.{}-dev{}".format( data['major'], data['minor'], data['patch'], data['dev'])

    if 'rc' in data and data['rc']:
        return "{}.{}.{}-rc{}".format( data['major'], data['minor'], data['patch'], data['rc'])

    return "{}.{}.{}".format( data['major'], data['minor'], data['patch'])

def get_requirements():

    file_handle = open('requirements.txt', 'r')
    data = file_handle.read()
    file_handle.close()
    return data.split("\n")


def get_scripts(directory='bin') -> list:
    files = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            if filename.endswith("~") or filename.endswith("__"):
                continue
            files.append(os.path.join(path, filename))
#    print( paths )
    return files



setup(name='ecc',
      version= get_version(),
      description='Elastic Compute Cluster',
      url='https://github.com/usegalaxy-no/ecc/',
      author='Kim Brugger',
      author_email='kim.brugger@uib.no',
      license='MIT',
      packages=['ecc'],
      classifiers=[
        'License :: MIT License',
        'Programming Language :: Python :: +3.6'
        ],
      install_requires=[ get_requirements() ],
      scripts=scripts(),
)
