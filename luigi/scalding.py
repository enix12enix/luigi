import getpass
import logging
import os
import re
import subprocess

import configuration
import hadoop
import hadoop_jar

logger = logging.getLogger('luigi-interface')

conf = configuration.get_config()

"""
Scalding support for Luigi.

Example configuration section in client.cfg:
[scalding]
# scala home directory, which should include a lib subdir with scala jars.
scala-home: /usr/share/scala

# scalding home directory, which should include a lib subdir with
# scalding-*-assembly-* jars as built from the official Twitter build script.
scalding-home: /usr/share/scalding

# provided dependencies, e.g. jars required for compiling but not executing
# scalding jobs. Currently requred jars:
# org.apache.hadoop/hadoop-core/0.20.2
# org.slf4j/slf4j-log4j12/1.6.6
# log4j/log4j/1.2.15
# commons-httpclient/commons-httpclient/3.1
# commons-cli/commons-cli/1.2
# org.apache.zookeeper/zookeeper/3.3.4
scalding-provided: /usr/share/scalding/provided

# additional jars required.
scalding-libjars: /usr/share/scalding/libjars
"""

scala_home = conf.get('scalding', 'scala-home', None)
scalding_home = conf.get('scalding', 'scalding-home', None)
scalding_provided = conf.get('scalding', 'scalding-provided', None)
scalding_libjars = conf.get('scalding', 'scalding-libjars', None)
tmp_dir = conf.get('core', 'tmp-dir', '/tmp/luigi')


class ScaldingJobRunner(hadoop.JobRunner):
    """JobRunner for `pyscald` commands. Used to run a ScaldingJobTask"""

    def __init__(self):
        pass

    def _get_jars(self, path):
        return [os.path.join(path, j) for j in os.listdir(path)
                if j.endswith('.jar')]

    def get_scala_jars(self, include_compiler=False):
        lib_dir = os.path.join(scala_home, 'lib')
        jars = [os.path.join(lib_dir, 'scala-library.jar')]

        # additional jar for scala 2.10 only
        reflect = os.path.join(lib_dir, 'scala-reflect.jar')
        if os.path.exists(reflect):
            jars.append(reflect)

        if include_compiler:
            jars.append(os.path.join(lib_dir, 'scala-compiler.jar'))

        return jars

    def get_scalding_jars(self):
        lib_dir = os.path.join(scalding_home, 'lib')
        return self._get_jars(lib_dir)

    def get_scalding_core(self):
        lib_dir = os.path.join(scalding_home, 'lib')
        for j in os.listdir(lib_dir):
            if j.startswith('scalding-core-'):
                return os.path.join(lib_dir, j)
        return None

    def get_provided_jars(self):
        return self._get_jars(scalding_provided)

    def get_libjars(self):
        return self._get_jars(scalding_libjars)

    def get_job_name(self, source):
        return 'scalding-job-{0}-{1}'.format(
            getpass.getuser(), os.path.basename(os.path.splitext(source)[0]))

    def get_job_jar(self, job):
        return os.path.join(tmp_dir, self.get_job_name(job.source()) + '.jar')

    def get_build_dir(self, source):
        build_dir = os.path.join(tmp_dir, self.get_job_name(source))
        return build_dir

    def get_job_class(self, source):
        # find name of the job class
        # usually the one that matches file name or last class that extends Job
        job_name = os.path.splitext(os.path.basename(source))[0]
        for l in open(source).readlines():
            p = re.search(r'class\s+([^\s\(]+).*extends\s+.*Job', l)
            if p:
                job_class = p.groups()[0]
                if job_class == job_name:
                    return job_class
        return job_class

    def build_job_jar(self, job):
        job_src = job.source()
        job_jar = self.get_job_jar(job)
        if os.path.exists(job_jar):
            src_mtime = os.path.getmtime(job_src)
            jar_mtime = os.path.getmtime(job_jar)
            if jar_mtime > src_mtime:
                return job_jar

        build_dir = self.get_build_dir(job_src)
        if not os.path.exists(build_dir):
            os.makedirs(build_dir)

        classpath = ':'.join(filter(None,
                                    self.get_scalding_jars() +
                                    self.get_provided_jars() +
                                    self.get_libjars() +
                                    job.extra_jars()))
        scala_cp = ':'.join(self.get_scala_jars(include_compiler=True))

        # compile scala source
        arglist = ['java', '-cp', scala_cp, 'scala.tools.nsc.Main',
                   '-classpath', classpath,
                   '-d', build_dir, job_src]
        subprocess.check_call(arglist)

        # build job jar file
        arglist = ['jar', 'cf', job_jar, '-C', build_dir, '.']
        subprocess.check_call(arglist)
        return job_jar

    def run_job(self, job):
        if not job.source() or not os.path.exists(job.source()):
            logger.error("Can't find source: {0}, full path {1}".format(
                         job.source(), os.path.abspath(job.source())))
            raise Exception("job source does not exist")

        job_jar = self.build_job_jar(job)
        jars = [job_jar] + self.get_libjars() + job.extra_jars()
        libjars = ','.join(filter(None, jars))
        arglist = ['hadoop', 'jar', self.get_scalding_core(),
                   '-libjars', libjars]
        if job.jobconfs():
            arglist += ['-D%s' % c for c in job.jobconfs()]

        job_class = job.main() or self.get_job_class(job.source())
        arglist += [job_class, '--hdfs']

        (tmp_files, job_args) = hadoop_jar.fix_paths(job)
        arglist += job_args

        logger.error("%r", arglist)
        env = os.environ.copy()
        jars.append(self.get_scalding_core())
        env['HADOOP_CLASSPATH'] = ':'.join(filter(None, jars))
        hadoop.run_and_track_hadoop_job(arglist, env=env)

        for a, b in tmp_files:
            a.move(b)


class ScaldingJobTask(hadoop.BaseHadoopJobTask):
    """A job task for Scalding that define a scala source and (optional) main
    method

    requires() should return a dictionary where the keys are Scalding argument
    names and values are lists of paths. For example:
    {'input1': ['A', 'B'], 'input2': ['C']} => --input1 A,B --input2 C
    """

    def source(self):
        """Path to the scala source for this Hadoop Job"""
        return None

    def extra_jars(self):
        """Extra jars for building and running this Hadoop Job"""
        return []

    def main(self):
        """optional main method for this Hadoop Job"""
        return None

    def job_runner(self):
        return ScaldingJobRunner()

    def atomic_output(self):
        """If True, then rewrite output arguments to be temp locations and
        atomically move them into place after the job finishes"""
        return True

    def job_args(self):
        """Extra arguments to pass to the Scalding job"""
        return []

    def args(self):
        """returns an array of args to pass to the job."""
        arglist = []
        for k, v in self.requires_hadoop().iteritems():
            arglist.extend(['--' + k, ','.join([t.output().path for t in v])])
        arglist.extend(['--output', self.output()])
        arglist.extend(self.job_args())
        return arglist
