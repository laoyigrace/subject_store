# Copyright 2010-2011 Josh Durgin
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Storage backend for RBD
   (RADOS (Reliable Autonomic Distributed Object Store) Block Device)"""
from __future__ import absolute_import
from __future__ import with_statement

import contextlib
import hashlib
import logging
import math

from oslo_config import cfg
from oslo_utils import units
import six
from six.moves import urllib

from subject_store import capabilities
from subject_store.common import utils
from subject_store import driver
from subject_store import exceptions
from subject_store.i18n import _, _LE, _LI
from subject_store import location

try:
    import rados
    import rbd
except ImportError:
    rados = None
    rbd = None

DEFAULT_POOL = 'subjects'
DEFAULT_CONFFILE = '/etc/ceph/ceph.conf'
DEFAULT_USER = None    # let librados decide based on the Ceph conf file
DEFAULT_CHUNKSIZE = 8  # in MiB
DEFAULT_SNAPNAME = 'snap'

LOG = logging.getLogger(__name__)

_RBD_OPTS = [
    cfg.IntOpt('rbd_store_chunk_size', default=DEFAULT_CHUNKSIZE,
               min=1,
               help=_("""
Size, in megabytes, to chunk RADOS subjects into.

Provide an integer value representing the size in megabytes to chunk
Glance subjects into. The default chunk size is 8 megabytes. For optimal
performance, the value should be a power of two.

When Ceph's RBD object storage system is used as the storage backend
for storing Glance subjects, the subjects are chunked into objects of the
size set using this option. These chunked objects are then stored
across the distributed block data store to use for Glance.

Possible Values:
    * Any positive integer value

Related options:
    * None

""")),
    cfg.StrOpt('rbd_store_pool', default=DEFAULT_POOL,
               help=_("""
RADOS pool in which subjects are stored.

When RBD is used as the storage backend for storing Glance subjects, the
subjects are stored by means of logical grouping of the objects (chunks
of subjects) into a ``pool``. Each pool is defined with the number of
placement groups it can contain. The default pool that is used is
'subjects'.

More information on the RBD storage backend can be found here:
http://ceph.com/planet/how-data-is-stored-in-ceph-cluster/

Possible Values:
    * A valid pool name

Related options:
    * None

""")),
    cfg.StrOpt('rbd_store_user', default=DEFAULT_USER,
               help=_("""
RADOS user to authenticate as.

This configuration option takes in the RADOS user to authenticate as.
This is only needed when RADOS authentication is enabled and is
applicable only if the user is using Cephx authentication. If the
value for this option is not set by the user or is set to None, a
default value will be chosen, which will be based on the client.
section in rbd_store_ceph_conf.

Possible Values:
    * A valid RADOS user

Related options:
    * rbd_store_ceph_conf

""")),
    cfg.StrOpt('rbd_store_ceph_conf', default=DEFAULT_CONFFILE,
               help=_("""
Ceph configuration file path.

This configuration option takes in the path to the Ceph configuration
file to be used. If the value for this option is not set by the user
or is set to None, librados will locate the default configuration file
which is located at /etc/ceph/ceph.conf. If using Cephx
authentication, this file should include a reference to the right
keyring in a client.<USER> section

Possible Values:
    * A valid path to a configuration file

Related options:
    * rbd_store_user

""")),
    cfg.IntOpt('rados_connect_timeout', default=0,
               help=_("""
Timeout value for connecting to Ceph cluster.

This configuration option takes in the timeout value in seconds used
when connecting to the Ceph cluster i.e. it sets the time to wait for
subject-api before closing the connection. This prevents subject-api
hangups during the connection to RBD. If the value for this option
is set to less than or equal to 0, no timeout is set and the default
librados value is used.

Possible Values:
    * Any integer value

Related options:
    * None

"""))
]


class StoreLocation(location.StoreLocation):
    """
    Class describing a RBD URI. This is of the form:

        rbd://subject

        or

        rbd://fsid/pool/subject/snapshot
    """

    def process_specs(self):
        # convert to ascii since librbd doesn't handle unicode
        for key, value in six.iteritems(self.specs):
            self.specs[key] = str(value)
        self.fsid = self.specs.get('fsid')
        self.pool = self.specs.get('pool')
        self.subject = self.specs.get('subject')
        self.snapshot = self.specs.get('snapshot')

    def get_uri(self):
        if self.fsid and self.pool and self.snapshot:
            # ensure nothing contains / or any other url-unsafe character
            safe_fsid = urllib.parse.quote(self.fsid, '')
            safe_pool = urllib.parse.quote(self.pool, '')
            safe_subject = urllib.parse.quote(self.subject, '')
            safe_snapshot = urllib.parse.quote(self.snapshot, '')
            return "rbd://%s/%s/%s/%s" % (safe_fsid, safe_pool,
                                          safe_subject, safe_snapshot)
        else:
            return "rbd://%s" % self.subject

    def parse_uri(self, uri):
        prefix = 'rbd://'
        if not uri.startswith(prefix):
            reason = _('URI must start with rbd://')
            msg = _LI("Invalid URI: %s") % reason

            LOG.info(msg)
            raise exceptions.BadStoreUri(message=reason)
        # convert to ascii since librbd doesn't handle unicode
        try:
            ascii_uri = str(uri)
        except UnicodeError:
            reason = _('URI contains non-ascii characters')
            msg = _LI("Invalid URI: %s") % reason

            LOG.info(msg)
            raise exceptions.BadStoreUri(message=reason)
        pieces = ascii_uri[len(prefix):].split('/')
        if len(pieces) == 1:
            self.fsid, self.pool, self.subject, self.snapshot = \
                (None, None, pieces[0], None)
        elif len(pieces) == 4:
            self.fsid, self.pool, self.subject, self.snapshot = \
                map(urllib.parse.unquote, pieces)
        else:
            reason = _('URI must have exactly 1 or 4 components')
            msg = _LI("Invalid URI: %s") % reason

            LOG.info(msg)
            raise exceptions.BadStoreUri(message=reason)
        if any(map(lambda p: p == '', pieces)):
            reason = _('URI cannot contain empty components')
            msg = _LI("Invalid URI: %s") % reason

            LOG.info(msg)
            raise exceptions.BadStoreUri(message=reason)


class SubjectIterator(object):
    """
    Reads data from an RBD subject, one chunk at a time.
    """

    def __init__(self, pool, name, snapshot, store, chunk_size=None):
        self.pool = pool or store.pool
        self.name = name
        self.snapshot = snapshot
        self.user = store.user
        self.conf_file = store.conf_file
        self.chunk_size = chunk_size or store.READ_CHUNKSIZE
        self.store = store

    def __iter__(self):
        try:
            with self.store.get_connection(conffile=self.conf_file,
                                           rados_id=self.user) as conn:
                with conn.open_ioctx(self.pool) as ioctx:
                    with rbd.Subject(ioctx, self.name,
                                   snapshot=self.snapshot) as subject:
                        size = subject.size()
                        bytes_left = size
                        while bytes_left > 0:
                            length = min(self.chunk_size, bytes_left)
                            data = subject.read(size - bytes_left, length)
                            bytes_left -= len(data)
                            yield data
                        raise StopIteration()
        except rbd.SubjectNotFound:
            raise exceptions.NotFound(
                _('RBD subject %s does not exist') % self.name)


class Store(driver.Store):
    """An implementation of the RBD backend adapter."""

    _CAPABILITIES = capabilities.BitMasks.RW_ACCESS
    OPTIONS = _RBD_OPTS

    EXAMPLE_URL = "rbd://<FSID>/<POOL>/<IMAGE>/<SNAP>"

    def get_schemes(self):
        return ('rbd',)

    @contextlib.contextmanager
    def get_connection(self, conffile, rados_id):
        client = rados.Rados(conffile=conffile, rados_id=rados_id)

        try:
            client.connect(timeout=self.connect_timeout)
        except rados.Error:
            msg = _LE("Error connecting to ceph cluster.")
            LOG.exception(msg)
            raise exceptions.BackendException()
        try:
            yield client
        finally:
            client.shutdown()

    def configure_add(self):
        """
        Configure the Store to use the stored configuration options
        Any store that needs special configuration should implement
        this method. If the store was not able to successfully configure
        itself, it should raise `exceptions.BadStoreConfiguration`
        """
        try:
            chunk = self.conf.subject_store.rbd_store_chunk_size
            self.chunk_size = chunk * units.Mi
            self.READ_CHUNKSIZE = self.chunk_size
            self.WRITE_CHUNKSIZE = self.READ_CHUNKSIZE

            # these must not be unicode since they will be passed to a
            # non-unicode-aware C library
            self.pool = str(self.conf.subject_store.rbd_store_pool)
            self.user = str(self.conf.subject_store.rbd_store_user)
            self.conf_file = str(self.conf.subject_store.rbd_store_ceph_conf)
            self.connect_timeout = self.conf.subject_store.rados_connect_timeout
        except cfg.ConfigFileValueError as e:
            reason = _("Error in store configuration: %s") % e
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name='rbd',
                                                   reason=reason)

    @capabilities.check
    def get(self, location, offset=0, chunk_size=None, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file, and returns a tuple of generator
        (for reading the subject file) and subject_size

        :param location: `subject_store.location.Location` object, supplied
                        from subject_store.location.get_location_from_uri()
        :raises: `subject_store.exceptions.NotFound` if subject does not exist
        """
        loc = location.store_location
        return (SubjectIterator(loc.pool, loc.subject, loc.snapshot, self),
                self.get_size(location))

    def get_size(self, location, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file, and returns the size

        :param location: `subject_store.location.Location` object, supplied
                        from subject_store.location.get_location_from_uri()
        :raises: `subject_store.exceptions.NotFound` if subject does not exist
        """
        loc = location.store_location
        # if there is a pool specific in the location, use it; otherwise
        # we fall back to the default pool specified in the config
        target_pool = loc.pool or self.pool
        with self.get_connection(conffile=self.conf_file,
                                 rados_id=self.user) as conn:
            with conn.open_ioctx(target_pool) as ioctx:
                try:
                    with rbd.Subject(ioctx, loc.subject,
                                   snapshot=loc.snapshot) as subject:
                        img_info = subject.stat()
                        return img_info['size']
                except rbd.SubjectNotFound:
                    msg = _('RBD subject %s does not exist') % loc.get_uri()
                    LOG.debug(msg)
                    raise exceptions.NotFound(msg)

    def _create_subject(self, fsid, conn, ioctx, subject_name,
                      size, order, context=None):
        """
        Create an rbd subject. If librbd supports it,
        make it a cloneable snapshot, so that copy-on-write
        volumes can be created from it.

        :param subject_name: Subject's name

        :retval: `subject_store.rbd.StoreLocation` object
        """
        librbd = rbd.RBD()
        features = conn.conf_get('rbd_default_features')
        if ((features is None) or (int(features) == 0)):
            features = rbd.RBD_FEATURE_LAYERING
        librbd.create(ioctx, subject_name, size, order, old_format=False,
                      features=int(features))
        return StoreLocation({
            'fsid': fsid,
            'pool': self.pool,
            'subject': subject_name,
            'snapshot': DEFAULT_SNAPNAME,
        }, self.conf)

    def _delete_subject(self, target_pool, subject_name,
                      snapshot_name=None, context=None):
        """
        Delete RBD subject and snapshot.

        :param subject_name: Subject's name
        :param snapshot_name: Subject snapshot's name

        :raises: NotFound if subject does not exist;
                InUseByStore if subject is in use or snapshot unprotect failed
        """
        with self.get_connection(conffile=self.conf_file,
                                 rados_id=self.user) as conn:
            with conn.open_ioctx(target_pool) as ioctx:
                try:
                    # First remove snapshot.
                    if snapshot_name is not None:
                        with rbd.Subject(ioctx, subject_name) as subject:
                            try:
                                subject.unprotect_snap(snapshot_name)
                                subject.remove_snap(snapshot_name)
                            except rbd.SubjectNotFound as exc:
                                msg = (_("Snap Operating Exception "
                                         "%(snap_exc)s "
                                         "Snapshot does not exist.") %
                                       {'snap_exc': exc})
                                LOG.debug(msg)
                            except rbd.SubjectBusy as exc:
                                log_msg = (_LE("Snap Operating Exception "
                                               "%(snap_exc)s "
                                               "Snapshot is in use.") %
                                           {'snap_exc': exc})
                                LOG.error(log_msg)
                                raise exceptions.InUseByStore()

                    # Then delete subject.
                    rbd.RBD().remove(ioctx, subject_name)
                except rbd.SubjectHasSnapshots:
                    log_msg = (_LE("Remove subject %(img_name)s failed. "
                                   "It has snapshot(s) left.") %
                               {'img_name': subject_name})
                    LOG.error(log_msg)
                    raise exceptions.HasSnapshot()
                except rbd.SubjectBusy:
                    log_msg = (_LE("Remove subject %(img_name)s failed. "
                                   "It is in use.") %
                               {'img_name': subject_name})
                    LOG.error(log_msg)
                    raise exceptions.InUseByStore()
                except rbd.SubjectNotFound:
                    msg = _("RBD subject %s does not exist") % subject_name
                    raise exceptions.NotFound(message=msg)

    @capabilities.check
    def add(self, subject_id, subject_file, subject_size, context=None,
            verifier=None):
        """
        Stores an subject file with supplied identifier to the backend
        storage system and returns a tuple containing information
        about the stored subject.

        :param subject_id: The opaque subject identifier
        :param subject_file: The subject data to write, as a file-like object
        :param subject_size: The size of the subject data to write, in bytes
        :param verifier: An object used to verify signatures for subjects

        :retval: tuple of URL in backing store, bytes written, checksum
                and a dictionary with storage system specific information
        :raises: `subject_store.exceptions.Duplicate` if the subject already
                existed
        """
        checksum = hashlib.md5()
        subject_name = str(subject_id)
        with self.get_connection(conffile=self.conf_file,
                                 rados_id=self.user) as conn:
            fsid = None
            if hasattr(conn, 'get_fsid'):
                fsid = conn.get_fsid()
            with conn.open_ioctx(self.pool) as ioctx:
                order = int(math.log(self.WRITE_CHUNKSIZE, 2))
                LOG.debug('creating subject %s with order %d and size %d',
                          subject_name, order, subject_size)
                if subject_size == 0:
                    LOG.warning(_("since subject size is zero we will be doing "
                                  "resize-before-write for each chunk which "
                                  "will be considerably slower than normal"))

                try:
                    loc = self._create_subject(fsid, conn, ioctx, subject_name,
                                             subject_size, order)
                except rbd.SubjectExists:
                    msg = _('RBD subject %s already exists') % subject_id
                    raise exceptions.Duplicate(message=msg)

                try:
                    with rbd.Subject(ioctx, subject_name) as subject:
                        bytes_written = 0
                        offset = 0
                        chunks = utils.chunkreadable(subject_file,
                                                     self.WRITE_CHUNKSIZE)
                        for chunk in chunks:
                            # If the subject size provided is zero we need to do
                            # a resize for the amount we are writing. This will
                            # be slower so setting a higher chunk size may
                            # speed things up a bit.
                            if subject_size == 0:
                                chunk_length = len(chunk)
                                length = offset + chunk_length
                                bytes_written += chunk_length
                                LOG.debug(_("resizing subject to %s KiB") %
                                          (length / units.Ki))
                                subject.resize(length)
                            LOG.debug(_("writing chunk at offset %s") %
                                      (offset))
                            offset += subject.write(chunk, offset)
                            checksum.update(chunk)
                            if verifier:
                                verifier.update(chunk)
                        if loc.snapshot:
                            subject.create_snap(loc.snapshot)
                            subject.protect_snap(loc.snapshot)
                except Exception as exc:
                    log_msg = (_LE("Failed to store subject %(img_name)s "
                                   "Store Exception %(store_exc)s") %
                               {'img_name': subject_name,
                                'store_exc': exc})
                    LOG.error(log_msg)

                    # Delete subject if one was created
                    try:
                        target_pool = loc.pool or self.pool
                        self._delete_subject(target_pool, loc.subject,
                                           loc.snapshot)
                    except exceptions.NotFound:
                        pass

                    raise exc

        # Make sure we send back the subject size whether provided or inferred.
        if subject_size == 0:
            subject_size = bytes_written

        return (loc.get_uri(), subject_size, checksum.hexdigest(), {})

    @capabilities.check
    def delete(self, location, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file to delete.

        :param location: `subject_store.location.Location` object, supplied
                  from subject_store.location.get_location_from_uri()

        :raises: NotFound if subject does not exist;
                InUseByStore if subject is in use or snapshot unprotect failed
        """
        loc = location.store_location
        target_pool = loc.pool or self.pool
        self._delete_subject(target_pool, loc.subject, loc.snapshot)
