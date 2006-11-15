#!/usr/bin/env python

"""SFTP-like Amazon S3 client.

S3 does not natively support directories; we fake a directory tree
for navigation, but no directories need to be created (hence, no 'mkdir'
command).
"""

__author__ = 'Dug Song <dugsong@monkey.org>'
__copyright__ = 'Copyright (c) 2006 Dug Song'
__license__ = 'GPL'
__url__ = 'http://monkey.org/~dugsong/s3tools/'
__version__ = '0.1'

import cStringIO, fnmatch, gzip, inspect, os, readline, shlex, sys
import xml.dom.minidom

import S3

# XXX - fill these in, or add a keys.py file or whatever
ACCESS_KEY = ''
SECRET_KEY = ''
if not SECRET_KEY:
    try:
        from keys import ACCESS_KEY, SECRET_KEY
    except ImportError:
        def _get_s3key(k):
            fds = os.popen3('security find-internet-password ' \
                            '-g -s s3.amazonaws.com -d %s' % k)
            return shlex.split(fds[2].readline())[1]
        ACCESS_KEY = _get_s3key('access')
        SECRET_KEY = _get_s3key('secret')

# XXX - TODO:
# fake {mk,rm}dir? how to represent empty dirs?
# support s3rsync's owner/perms/times metadata
# cmd_getdir

class S3ftp(object):
    def __init__(self, access_key, secret_key, bucket):
        self.conn = S3.AWSAuthConnection(ACCESS_KEY, SECRET_KEY)
        r = self.conn.list_all_my_buckets()
        if not self.ok(r):
            self.perror(r)
            sys.exit(1)
        self.bucket = bucket
        self.cwd = '/'
        if self.bucket not in [ e.name for e in r.entries ]:
            r = self.conn.create_bucket(self.bucket)
            if not self.ok(r):
                self.perror(r)
                return
        print 'Connected to S3, bucket "%s"' % self.bucket
    
    def ok(self, r):
        return r.http_response.status < 300

    def perror(self, r):
        doc = xml.dom.minidom.parseString(r.body)
        print '%s: %s' % \
              (doc.getElementsByTagName('Code')[0].childNodes[0].data,
               doc.getElementsByTagName('Message')[0].childNodes[0].data)

    def _path_to_prefix(self, path):
        path = path.lstrip('/')
        if path:
            path += '/'
        return path

    def _path_to_key(self, path):
        return path.lstrip('/')
    
    def cmd_invalid(self, *args):
        print >>sys.stderr, 'Invalid command.'

    def cmd_help(self, *args):
        """display this help text"""
        for x in dir(self):
            if x.startswith('cmd_'):
                l = [ x[4:] ]
                f = getattr(self, x)
                args, varargs, varkw, defaults = inspect.getargspec(f)
                n = len(args or []) - len(defaults or [])
                for a in args[1:n]:
                    l.append('<%s>' % a)
                for a in args[n:]:
                    if not a.startswith('_'):
                        l.append('[%s]' % a)
                if f.__doc__:
                    print '%-24s %s' % (' '.join(l), f.__doc__)

    def normpath(self, path):
        path = os.path.expanduser(path)
        if not path.startswith('/'):
            path = '%s/%s' % (self.cwd, path)
        path = os.path.normpath(path)
        if path.startswith('//'):
            path = path[1:]	# XXX - os.path.normpath bug
        return path

    def cmd_cd(self, path):
        """change remote directory"""
        path = self.normpath(path)
        prefix = self._path_to_prefix(path)
        options = { 'delimiter':'/' }
        if prefix:
            options['prefix'] = prefix
        r = self.conn.list_bucket(self.bucket, options=options)
        if not self.ok(r):
            self.perror(r)
        else:
            self.cwd = path
    
    def cmd_lcd(self, path='~'):
        """change local directory"""
        try:
            os.chdir(self.normpath(path))
        except OSError, msg:
            print 'Couldn\'t change local directory to "%s": %s' % \
                  (path, msg[1])
    
    def cmd_get(self, path, _pager=''):
        """download a file"""
        path = self._path_to_key(self.normpath(path))
        r = self.conn.get(self.bucket, path)
        if self.ok(r):
            buf = r.object.data
            if r.http_response.getheader('content-encoding', '') == 'gzip':
                buf = gzip.GzipFile(fileobj=cStringIO.StringIO(buf)).read()
            if _pager:
                f = os.popen(_pager, 'w')
            else:
                f = open(os.path.basename(path), 'wb')
            f.write(buf)
            f.close()
        else:
            self.perror(r)

    def cmd_more(self, path):
        """page through a file"""
        return self.cmd_get(path, os.environ.get('PAGER', 'more'))
    
    def cmd_lpwd(self):
        """print local working directory"""
        print 'Local working directory:', os.getcwd()

    def cmd_lls(self, *args):
        """display local directory listing"""
        os.system('ls %s' % ' '.join(args))

    def cmd_ls(self, path='', _path2=''):
        """list files / directories"""
        if path == '-l':
            prefix = self._path_to_prefix(self.normpath(_path2))
            verbose = True
        else:
            prefix = self._path_to_prefix(self.normpath(path))
            verbose = False
        options = { 'delimiter':'/' }
        if prefix:
            options['prefix'] = prefix
        r = self.conn.list_bucket(self.bucket, options=options)
        if not self.ok(r):
            self.perror(r)
            return
        if not verbose:
            if not r.entries:
                # XXX - try HEAD - path is a file?
                pass
            l = []	# XXX - '.', '..'?
            # XXX - should do HEADs on all, to get S3Object.metadata if set
            for p in r.common_prefixes:
                l.append(p.prefix[len(prefix):])
            for e in r.entries:
                l.append(os.path.basename(e.key))
            print ' '.join(l)
        else:
            for p in r.common_prefixes:
                print 'drwx------\t%s\t\t\t\t\t\t%s' % \
                      (r.name, p.prefix[len(prefix):])
            for e in r.entries:
                print '-rw-------\t%s\t%s\t%s\t%s' % \
                      (e.owner.display_name, e.size, e.last_modified,
                       e.key[len(prefix):])
    
    def cmd_lsdir(self, path=''):
        """recursively list a directory tree"""
        options = {}
        prefix = self._path_to_prefix(self.normpath(path))
        if prefix:
            options['prefix'] = prefix
        r = self.conn.list_bucket(self.bucket, options=options)
        for e in r.entries:
            print '-rw-------\t%s\t%s\t%s\t%s' % \
                  (e.owner.display_name, e.size, e.last_modified,
                   e.key[len(prefix):])
    
    def cmd_put(self, lpath, rpath=''):
        """upload file"""
        buf = open(lpath).read()
        if not rpath:
            rpath = self.normpath('%s/%s' % (
                self.cwd, os.path.basename(lpath)))

        key = self._path_to_key(self.normpath(rpath))
        print 'Uploading', lpath, 'to', key
        r = self.conn.put(self.bucket, key, buf)
        if not self.ok(r):
            self.perror(r)
    
    def cmd_pwd(self):
        """print working directory on remote machine"""
        print 'Remote working directory: %s' % self.cwd
    
    def cmd_putdir(self, lpath, rpath=''):
        """upload an entire directory"""
        lpath = os.path.normpath(lpath)
        if not rpath:
            rpath = self.normpath(
                '%s/%s' % (self.cwd, os.path.basename(lpath)))
        for subdir, dirs, files in os.walk(lpath):
            for fname in files:
                path = '%s/%s' % (subdir, fname)
                buf = open(path).read()
                if buf:
                    key = self._path_to_key(self.normpath(
                        '%s/%s' % (rpath, path[len(lpath)+1:])))
                    print 'Uploading', path, 'to', key
                    r = self.conn.put(self.bucket, key, buf)
                    if not self.ok(r):
                        self.perror(r)
                        raise RuntimeError
    
    def cmd_rm(self, path):
        """delete remote file"""
        path = self.normpath(path)
        print 'Removing', path
        r = self.conn.delete(self.bucket, self._path_to_key(path))
        if not self.ok(r):
            self.perror(r)
    
    def cmd_rmdir(self, path):
        """recursively remove a directory tree"""
        options = { 'delimiter':'/' }
        prefix = self._path_to_prefix(self.normpath(path))
        if prefix:
            options['prefix'] = prefix
        r = self.conn.list_bucket(self.bucket, options=options)
        for p in r.common_prefixes:
            self.cmd_rmdir(p.prefix)
        for e in r.entries:
            self.cmd_rm(e.key)

    def cmd_version(self):
        print 's3ftp self.__version__'
    
    def cmd_exit(self):
        sys.exit(0)
    cmd_quit = cmd_exit
    
    def main(self):
        while 1:
            try:
                l = shlex.split(raw_input('s3ftp> ')) or [ '' ]
            except EOFError:
                break
            if l[0].startswith('!'):
                os.system(l[0][1:])
            else:
                m = getattr(self, 'cmd_%s' % l[0], self.cmd_help)
                m(*l[1:])

if __name__ == '__main__':
    if len(sys.argv) == 2:
        bucket = sys.argv[1]
    else:
        bucket = os.getenv('USER')
    S3ftp(ACCESS_KEY, SECRET_KEY, bucket).main()
