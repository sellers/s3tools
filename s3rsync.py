#!/usr/bin/env python

"""rsync-like client for Amazon S3.

supports compression (content-encoded, so files are stored on S3 compressed),
maintains file permissions, ownership, timestamps, etc.
"""

__author__ = 'Dug Song <dugsong@monkey.org>'
__copyright__ = 'Copyright (c) 2006 Dug Song'
__license__ = 'GPL'
__url__ = 'http://monkey.org/~dugsong/s3tools/'
__version__ = '0.1'

import cStringIO, fnmatch, gzip, md5, optparse, os, shlex, stat, sys, time
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

# XXX - TODO
# store directories, symlinks, devices as file with add'l metadata

# XXX - zero mtime in gzip header for a stable etag
class FakeTime(object):
    def time(self):
        return 0
gzip.time = FakeTime()

class S3rsync(object):
    ignore_pats = (
        '*.o', '*.lo', '*.la', '#*#', '.*.rej', '*.rej',
        '.*~', '*~', '.#*', '.DS_Store', '*.pyc'
        )
    compressed_exts = dict.fromkeys((
        '.gz', '.bz2', '.zip', '.rar', '.Z',
        '.jar', '.tgz', '.tbz2', '.sit',
        '.jpg', '.jpeg', '.gif', 'png',
        '.mp3', '.ogg', '.mp4', '.aac', '.wma'
        '.mpeg', '.avi', '.mov', '.wmv', '.ogm',
        '.pkg', '.mpkg', '.msi', '.rpm', '.deb',
        ))
    
    def __init__(self, access_key, secret_key):
        self.conn = S3.AWSAuthConnection(access_key, secret_key)
        
    def _perror(self, r):
        doc = xml.dom.minidom.parseString(r.body)
        print 'error: %s - %s' % \
              (doc.getElementsByTagName('Code')[0].childNodes[0].data,
               doc.getElementsByTagName('Message')[0].childNodes[0].data)

    def _exclude(self, filename):
        if self.opts.exclude:
            for pat in self.ignore_pats:
                if fnmatch.fnmatchcase(filename, pat):
                    return True
        return False

    def _fix_attrs(self, filename, attrs):
        if 'mode' in attrs:
            os.chmod(filename, int(attrs['mode']))
        if 'uid' in attrs:
            try: os.lchown(filename, int(attrs['uid']), int(attrs['gid']))
            except OSError, msg: print >>sys.stderr, msg
        if 'atime' in attrs and 'mtime' in attrs:
            os.utime(filename, (long(attrs['atime']), long(attrs['mtime'])))

    def _gzip_string(self, buf):
        f = cStringIO.StringIO()
        gf = gzip.GzipFile(fileobj=f, mode='w')
        gf.write(buf)
        gf.close()
        return f.getvalue()
    
    def _get(self, bucket, src, dst, src_size=-1, src_etag=''):
        if self._exclude(src): return
        headers = {}
        try:
            size = os.path.getsize(dst)
            if self.opts.ignore: return
            buf = open(dst).read()
            etags = [ md5.md5(buf).hexdigest() ]
            if src_size != size:
                if os.path.splitext(dst)[1].lower() not in self.compressed_exts:
                    etags.append(md5.md5(self._gzip_string(buf)).hexdigest())
            if src_etag in etags:
                return
            headers['If-None-Match'] = etags
        except OSError:
            pass
        r = self.conn.get(bucket, src, headers)
        if r.http_response.status == 200:
            if self.opts.verbose:
                print dst
            buf = r.object.data
            if r.http_response.getheader('content-encoding', '') == 'gzip':
                buf = gzip.GzipFile(fileobj=cStringIO.StringIO(buf)).read()
            f = open(dst, 'wb'); f.write(buf); f.close()
            self._fix_attrs(dst, r.object.metadata)
        elif r.http_response.status != 304:
            self._perror(r)
       
    def _get_file(self, bucket, path, dst):
        fname = os.path.basename(path)
        if os.path.isdir(dst):
            dst = os.path.join(dst, fname)
        self._get(bucket, path, dst)

    def _get_dir(self, bucket, path, dst):
        options = {}
        prefix = ''
        if path:
            prefix = os.path.normpath(path) + '/'
            options['prefix'] = prefix
        r = self.conn.list_bucket(bucket, options=options)
        dstdir = os.path.join(dst, os.path.basename(prefix.rstrip('/')))
        for e in r.entries:
            skey = e.key[len(prefix):]
            dstfile = os.path.join(dstdir, skey)
            dstsubdir = os.path.dirname(dstfile)
            if not os.path.isdir(dstsubdir):
                os.makedirs(dstsubdir)
            self._get(bucket, e.key, dstfile, e.size, e.etag.strip('"'))
    
    def _put(self, bucket, filename, key, etag=''):
        headers = {}
        st = os.lstat(filename)
        if not stat.S_ISREG(st[stat.ST_MODE]):
            print >>sys.stderr, 'skipping non-file', filename
            return
        if not st[stat.ST_SIZE]:
            return
        obj = S3.S3Object(open(filename).read(), metadata={
            'mode':str(stat.S_IMODE(st[stat.ST_MODE])),
            'uid':str(st[stat.ST_UID]), 'gid':str(st[stat.ST_GID]),
            'atime':str(st[stat.ST_ATIME]), 'mtime':str(st[stat.ST_MTIME]),
            })
        etags = [ md5.md5(obj.data).hexdigest() ]
        if self.opts.compress:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in self.compressed_exts:
                obj.data = self._gzip_string(obj.data)
                headers['Content-Encoding'] = 'gzip'
                etags.append(md5.md5(obj.data).hexdigest())
        if etag not in etags:
            if self.opts.verbose:
                print key
            r = self.conn.put(bucket, key, obj, headers=headers)
            if r.http_response.status != 200:
                self._perror(r)

    def _put_file(self, bucket, path, dst):
        if self._exclude(path): return
        fname = os.path.basename(path)
        # XXX - if dst ends with '/', treat it as a directory
        if not dst or dst.endswith('/'):
            dst = dst + fname
        dst = os.path.normpath(dst).lstrip('/')
        r = self.conn.list_bucket(bucket,
                                  options={ 'prefix':dst, 'delimiter':'/' })
        if not r.entries:
            self._put(bucket, path, dst)
        elif not self.opts.ignore:
            self._put(bucket, path, dst, etag=r.entries[0].etag.strip('"'))

    def _put_dir(self, bucket, lpath, rpath):
        rdir = os.path.normpath(os.path.join(rpath,
                   os.path.basename(os.path.normpath(lpath))))
        r = self.conn.list_bucket(bucket, options={ 'prefix':rdir })
        etags = dict([(e.key, e.etag.strip('"')) for e in r.entries ])
        for subdir, dirs, files in os.walk(lpath):
            for fname in files:
                filename = os.path.join(subdir, fname)
                key = os.path.join(rdir, filename[len(lpath)+1:])
                self._put(bucket, filename, key, etags.get(key, ''))
    
    def _parse_url(self, url):
        scheme, path = url.split('//', 1)
        return path.split('/', 1)

    def main(self):
        usage = 'usage: %prog [OPTION]... SRC [SRC]... s3://BUCKET/DEST\n' \
                'usage: %prog [OPTION]... s3://BUCKET/SRC [DEST]'
        op = optparse.OptionParser(usage=usage)
        op.add_option('-r', dest='recurse', action='store_true',
                      help='recurse into directories')
        op.add_option('-a', dest='recurse', action='store_true',
                      help='archive mode, equivalent to -r '
                      '(perms, symlinks, times, devices are always preserved)')
        op.add_option('-v', dest='verbose', action='store_true',
                      help='enable verbose output')
        op.add_option('-z', dest='compress', action='store_true',
                      help='use compression (reduces storage on S3 :-)')
        op.add_option('-C', dest='exclude', action='store_true',
                      help='auto ignore files in the same way CVS does')
        op.add_option('--ignore-existing', dest='ignore',
                      action='store_true',
                      help='ignore files that already exist on receiver')
        #op.add_option('--delete', dest='delete', action='store_true',
        #              help='delete files that don\'t exist on sender')
        self.opts, args = op.parse_args(sys.argv[1:])

        if not args:
            pass
        elif args[0].startswith('s3://'):
            bucket, key = self._parse_url(args[0])
            _get = self.opts.recurse and self._get_dir or self._get_file
            if len(args) == 2:
                return _get(bucket, key, args[1])
            elif len(args) == 1:
                return _get(bucket, key, '.')
        elif args[-1].startswith('s3://'):
            bucket, key = self._parse_url(args[-1])
            _put = self.opts.recurse and self._put_dir or self._put_file
            for arg in args[:-1]:
                _put(bucket, arg, key)
            return
        op.error('invalid arguments')

if __name__ == '__main__':
    S3rsync(ACCESS_KEY, SECRET_KEY).main()
