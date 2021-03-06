# Copyright 2010 OpenStack Foundation
# Copyright 2014 Red Hat, Inc.
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

"""
A simple filesystem-backed store
"""

import errno
import hashlib
import logging
import os
import stat

import jsonschema
from oslo_config import cfg
from oslo_serialization import jsonutils
from oslo_utils import encodeutils
from oslo_utils import excutils
from oslo_utils import units
from six.moves import urllib

import subject_store
from subject_store import capabilities
from subject_store.common import utils
import subject_store.driver
from subject_store import exceptions
from subject_store.i18n import _, _LE, _LW
import subject_store.location


LOG = logging.getLogger(__name__)

_FILESYSTEM_CONFIGS = [
    cfg.StrOpt('filesystem_store_datadir',
               default='/var/lib/subject/subjects',
               help=_("""
Directory to which the filesystem backend store writes subjects.

Upon start up, Glance creates the directory if it doesn't already
exist and verifies write access to the user under which
``subject-api`` runs. If the write access isn't available, a
``BadStoreConfiguration`` exception is raised and the filesystem
store may not be available for adding new subjects.

NOTE: This directory is used only when filesystem store is used as a
storage backend. Either ``filesystem_store_datadir`` or
``filesystem_store_datadirs`` option must be specified in
``subject-api.conf``. If both options are specified, a
``BadStoreConfiguration`` will be raised and the filesystem store
may not be available for adding new subjects.

Possible values:
    * A valid path to a directory

Related options:
    * ``filesystem_store_datadirs``
    * ``filesystem_store_file_perm``

""")),
    cfg.MultiStrOpt('filesystem_store_datadirs',
                    help=_("""
List of directories and their priorities to which the filesystem
backend store writes subjects.

The filesystem store can be configured to store subjects in multiple
directories as opposed to using a single directory specified by the
``filesystem_store_datadir`` configuration option. When using
multiple directories, each directory can be given an optional
priority to specify the preference order in which they should
be used. Priority is an integer that is concatenated to the
directory path with a colon where a higher value indicates higher
priority. When two directories have the same priority, the directory
with most free space is used. When no priority is specified, it
defaults to zero.

More information on configuring filesystem store with multiple store
directories can be found at
http://docs.openstack.org/developer/subject/configuring.html

NOTE: This directory is used only when filesystem store is used as a
storage backend. Either ``filesystem_store_datadir`` or
``filesystem_store_datadirs`` option must be specified in
``subject-api.conf``. If both options are specified, a
``BadStoreConfiguration`` will be raised and the filesystem store
may not be available for adding new subjects.

Possible values:
    * List of strings of the following form:
        * ``<a valid directory path>:<optional integer priority>``

Related options:
    * ``filesystem_store_datadir``
    * ``filesystem_store_file_perm``

""")),
    cfg.StrOpt('filesystem_store_metadata_file',
               help=_("""
Filesystem store metadata file.

The path to a file which contains the metadata to be returned with
any location associated with the filesystem store. The file must
contain a valid JSON object. The object should contain the keys
``id`` and ``mountpoint``. The value for both keys should be a
string.

Possible values:
    * A valid path to the store metadata file

Related options:
    * None

""")),
    cfg.IntOpt('filesystem_store_file_perm',
               default=0,
               help=_("""
File access permissions for the subject files.

Set the intended file access permissions for subject data. This provides
a way to enable other services, e.g. Nova, to consume subjects directly
from the filesystem store. The users running the services that are
intended to be given access to could be made a member of the group
that owns the files created. Assigning a value less then or equal to
zero for this configuration option signifies that no changes be made
to the  default permissions. This value will be decoded as an octal
digit.

For more information, please refer the documentation at
http://docs.openstack.org/developer/subject/configuring.html

Possible values:
    * A valid file access permission
    * Zero
    * Any negative integer

Related options:
    * None

"""))]

MULTI_FILESYSTEM_METADATA_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "mountpoint": {"type": "string"}
        },
        "required": ["id", "mountpoint"],
    }
}


class StoreLocation(subject_store.location.StoreLocation):
    """Class describing a Filesystem URI."""

    def process_specs(self):
        self.scheme = self.specs.get('scheme', 'file')
        self.path = self.specs.get('path')

    def get_uri(self):
        return "file://%s" % self.path

    def parse_uri(self, uri):
        """
        Parse URLs. This method fixes an issue where credentials specified
        in the URL are interpreted differently in Python 2.6.1+ than prior
        versions of Python.
        """
        pieces = urllib.parse.urlparse(uri)
        assert pieces.scheme in ('file', 'filesystem')
        self.scheme = pieces.scheme
        path = (pieces.netloc + pieces.path).strip()
        if path == '':
            reason = _("No path specified in URI")
            LOG.info(reason)
            raise exceptions.BadStoreUri(message=reason)
        self.path = path


class ChunkedFile(object):

    """
    We send this back to the Glance API server as
    something that can iterate over a large file
    """

    def __init__(self, filepath, offset=0, chunk_size=4096,
                 partial_length=None):
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.partial_length = partial_length
        self.partial = self.partial_length is not None
        self.fp = open(self.filepath, 'rb')
        if offset:
            self.fp.seek(offset)

    def __iter__(self):
        """Return an iterator over the subject file."""
        try:
            if self.fp:
                while True:
                    if self.partial:
                        size = min(self.chunk_size, self.partial_length)
                    else:
                        size = self.chunk_size

                    chunk = self.fp.read(size)
                    if chunk:
                        yield chunk

                        if self.partial:
                            self.partial_length -= len(chunk)
                            if self.partial_length <= 0:
                                break
                    else:
                        break
        finally:
            self.close()

    def close(self):
        """Close the internal file pointer"""
        if self.fp:
            self.fp.close()
            self.fp = None


class Store(subject_store.driver.Store):

    _CAPABILITIES = (capabilities.BitMasks.READ_RANDOM |
                     capabilities.BitMasks.WRITE_ACCESS |
                     capabilities.BitMasks.DRIVER_REUSABLE)
    OPTIONS = _FILESYSTEM_CONFIGS
    READ_CHUNKSIZE = 64 * units.Ki
    WRITE_CHUNKSIZE = READ_CHUNKSIZE
    FILESYSTEM_STORE_METADATA = None

    def get_schemes(self):
        return ('file', 'filesystem')

    def _check_write_permission(self, datadir):
        """
        Checks if directory created to write subject files has
        write permission.

        :datadir is a directory path in which subject wites subject files.
        :raises: BadStoreConfiguration exception if datadir is read-only.
        """
        if not os.access(datadir, os.W_OK):
            msg = (_("Permission to write in %s denied") % datadir)
            LOG.exception(msg)
            raise exceptions.BadStoreConfiguration(
                store_name="filesystem", reason=msg)

    def _set_exec_permission(self, datadir):
        """
        Set the execution permission of owner-group and/or other-users to
        subject directory if the subject file which contained needs relevant
        access permissions.

        :datadir is a directory path in which subject writes subject files.
        """

        if self.conf.subject_store.filesystem_store_file_perm <= 0:
            return

        try:
            mode = os.stat(datadir)[stat.ST_MODE]
            perm = int(str(self.conf.subject_store.filesystem_store_file_perm),
                       8)
            if perm & stat.S_IRWXO > 0:
                if not mode & stat.S_IXOTH:
                    # chmod o+x
                    mode |= stat.S_IXOTH
                    os.chmod(datadir, mode)
            if perm & stat.S_IRWXG > 0:
                if not mode & stat.S_IXGRP:
                    # chmod g+x
                    os.chmod(datadir, mode | stat.S_IXGRP)
        except (IOError, OSError):
            LOG.warning(_LW("Unable to set execution permission of "
                            "owner-group and/or other-users to datadir: %s")
                        % datadir)

    def _create_subject_directories(self, directory_paths):
        """
        Create directories to write subject files if
        it does not exist.

        :directory_paths is a list of directories belonging to subject store.
        :raises: BadStoreConfiguration exception if creating a directory fails.
        """
        for datadir in directory_paths:
            if os.path.exists(datadir):
                self._check_write_permission(datadir)
                self._set_exec_permission(datadir)
            else:
                msg = _("Directory to write subject files does not exist "
                        "(%s). Creating.") % datadir
                LOG.info(msg)
                try:
                    os.makedirs(datadir)
                    self._check_write_permission(datadir)
                    self._set_exec_permission(datadir)
                except (IOError, OSError):
                    if os.path.exists(datadir):
                        # NOTE(markwash): If the path now exists, some other
                        # process must have beat us in the race condition.
                        # But it doesn't hurt, so we can safely ignore
                        # the error.
                        self._check_write_permission(datadir)
                        self._set_exec_permission(datadir)
                        continue
                    reason = _("Unable to create datadir: %s") % datadir
                    LOG.error(reason)
                    raise exceptions.BadStoreConfiguration(
                        store_name="filesystem", reason=reason)

    def _validate_metadata(self, metadata_file):
        """Validate metadata against json schema.

        If metadata is valid then cache metadata and use it when
        creating new subject.

        :param metadata_file: JSON metadata file path
        :raises: BadStoreConfiguration exception if metadata is not valid.
        """
        try:
            with open(metadata_file, 'rb') as fptr:
                metadata = jsonutils.load(fptr)

            if isinstance(metadata, dict):
                # If metadata is of type dictionary
                # i.e. - it contains only one mountpoint
                # then convert it to list of dictionary.
                metadata = [metadata]

            # Validate metadata against json schema
            jsonschema.validate(metadata, MULTI_FILESYSTEM_METADATA_SCHEMA)
            subject_store.check_location_metadata(metadata)
            self.FILESYSTEM_STORE_METADATA = metadata
        except (jsonschema.exceptions.ValidationError,
                exceptions.BackendException, ValueError) as vee:
            err_msg = encodeutils.exception_to_unicode(vee)
            reason = _('The JSON in the metadata file %(file)s is '
                       'not valid and it can not be used: '
                       '%(vee)s.') % dict(file=metadata_file,
                                          vee=err_msg)
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(
                store_name="filesystem", reason=reason)
        except IOError as ioe:
            err_msg = encodeutils.exception_to_unicode(ioe)
            reason = _('The path for the metadata file %(file)s could '
                       'not be accessed: '
                       '%(ioe)s.') % dict(file=metadata_file,
                                          ioe=err_msg)
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(
                store_name="filesystem", reason=reason)

    def configure_add(self):
        """
        Configure the Store to use the stored configuration options
        Any store that needs special configuration should implement
        this method. If the store was not able to successfully configure
        itself, it should raise `exceptions.BadStoreConfiguration`
        """
        if not (self.conf.subject_store.filesystem_store_datadir or
                self.conf.subject_store.filesystem_store_datadirs):
            reason = (_("Specify at least 'filesystem_store_datadir' or "
                        "'filesystem_store_datadirs' option"))
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name="filesystem",
                                                   reason=reason)

        if (self.conf.subject_store.filesystem_store_datadir and
                self.conf.subject_store.filesystem_store_datadirs):

            reason = (_("Specify either 'filesystem_store_datadir' or "
                        "'filesystem_store_datadirs' option"))
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name="filesystem",
                                                   reason=reason)

        if self.conf.subject_store.filesystem_store_file_perm > 0:
            perm = int(str(self.conf.subject_store.filesystem_store_file_perm),
                       8)
            if not perm & stat.S_IRUSR:
                reason = _LE("Specified an invalid "
                             "'filesystem_store_file_perm' option which "
                             "could make subject file to be unaccessible by "
                             "subject service.")
                LOG.error(reason)
                reason = _("Invalid 'filesystem_store_file_perm' option.")
                raise exceptions.BadStoreConfiguration(store_name="filesystem",
                                                       reason=reason)

        self.multiple_datadirs = False
        directory_paths = set()
        if self.conf.subject_store.filesystem_store_datadir:
            self.datadir = self.conf.subject_store.filesystem_store_datadir
            directory_paths.add(self.datadir)
        else:
            self.multiple_datadirs = True
            self.priority_data_map = {}
            for datadir in self.conf.subject_store.filesystem_store_datadirs:
                (datadir_path,
                 priority) = self._get_datadir_path_and_priority(datadir)
                priority_paths = self.priority_data_map.setdefault(
                    int(priority), [])
                self._check_directory_paths(datadir_path, directory_paths,
                                            priority_paths)
                directory_paths.add(datadir_path)
                priority_paths.append(datadir_path)

            self.priority_list = sorted(self.priority_data_map,
                                        reverse=True)

        self._create_subject_directories(directory_paths)

        metadata_file = self.conf.subject_store.filesystem_store_metadata_file
        if metadata_file:
            self._validate_metadata(metadata_file)

    def _check_directory_paths(self, datadir_path, directory_paths,
                               priority_paths):
        """
        Checks if directory_path is already present in directory_paths.

        :datadir_path is directory path.
        :datadir_paths is set of all directory paths.
        :raises: BadStoreConfiguration exception if same directory path is
               already present in directory_paths.
        """
        if datadir_path in directory_paths:
            msg = (_("Directory %(datadir_path)s specified "
                     "multiple times in filesystem_store_datadirs "
                     "option of filesystem configuration") %
                   {'datadir_path': datadir_path})

            # If present with different priority it's a bad configuration
            if datadir_path not in priority_paths:
                LOG.exception(msg)
                raise exceptions.BadStoreConfiguration(
                    store_name="filesystem", reason=msg)

            # Present with same prio (exact duplicate) only deserves a warning
            LOG.warning(msg)

    def _get_datadir_path_and_priority(self, datadir):
        """
        Gets directory paths and its priority from
        filesystem_store_datadirs option in subject-api.conf.

        :param datadir: is directory path with its priority.
        :returns: datadir_path as directory path
                 priority as priority associated with datadir_path
        :raises: BadStoreConfiguration exception if priority is invalid or
               empty directory path is specified.
        """
        priority = 0
        parts = [part.strip() for part in datadir.rsplit(":", 1)]
        datadir_path = parts[0]
        if len(parts) == 2 and parts[1]:
            priority = parts[1]
            if not priority.isdigit():
                msg = (_("Invalid priority value %(priority)s in "
                         "filesystem configuration") % {'priority': priority})
                LOG.exception(msg)
                raise exceptions.BadStoreConfiguration(
                    store_name="filesystem", reason=msg)

        if not datadir_path:
            msg = _("Invalid directory specified in filesystem configuration")
            LOG.exception(msg)
            raise exceptions.BadStoreConfiguration(
                store_name="filesystem", reason=msg)

        return datadir_path, priority

    @staticmethod
    def _resolve_location(location):
        filepath = location.store_location.path

        if not os.path.exists(filepath):
            raise exceptions.NotFound(subject=filepath)

        filesize = os.path.getsize(filepath)
        return filepath, filesize

    def _get_metadata(self, filepath):
        """Return metadata dictionary.

        If metadata is provided as list of dictionaries then return
        metadata as dictionary containing 'id' and 'mountpoint'.

        If there are multiple nfs directories (mountpoints) configured
        for subject, then we need to create metadata JSON file as list
        of dictionaries containing all mountpoints with unique id.
        But Nova will not be able to find in which directory (mountpoint)
        subject is present if we store list of dictionary(containing mountpoints)
        in subject subject metadata. So if there are multiple mountpoints then
        we will return dict containing exact mountpoint where subject is stored.

        If subject path does not start with any of the 'mountpoint' provided
        in metadata JSON file then error is logged and empty
        dictionary is returned.

        :param filepath: Path of subject on store
        :returns: metadata dictionary
        """
        if self.FILESYSTEM_STORE_METADATA:
            for subject_meta in self.FILESYSTEM_STORE_METADATA:
                if filepath.startswith(subject_meta['mountpoint']):
                    return subject_meta

            reason = (_LE("The subject path %(path)s does not match with "
                          "any of the mountpoint defined in "
                          "metadata: %(metadata)s. An empty dictionary "
                          "will be returned to the client.")
                      % dict(path=filepath,
                             metadata=self.FILESYSTEM_STORE_METADATA))
            LOG.error(reason)

        return {}

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
        filepath, filesize = self._resolve_location(location)
        msg = _("Found subject at %s. Returning in ChunkedFile.") % filepath
        LOG.debug(msg)
        return (ChunkedFile(filepath,
                            offset=offset,
                            chunk_size=self.READ_CHUNKSIZE,
                            partial_length=chunk_size),
                chunk_size or filesize)

    def get_size(self, location, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file and returns the subject size

        :param location: `subject_store.location.Location` object, supplied
                        from subject_store.location.get_location_from_uri()
        :raises: `subject_store.exceptions.NotFound` if subject does not exist
        :rtype int
        """
        filepath, filesize = self._resolve_location(location)
        msg = _("Found subject at %s.") % filepath
        LOG.debug(msg)
        return filesize

    @capabilities.check
    def delete(self, location, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file to delete

        :param location: `subject_store.location.Location` object, supplied
                  from subject_store.location.get_location_from_uri()

        :raises: NotFound if subject does not exist
        :raises: Forbidden if cannot delete because of permissions
        """
        loc = location.store_location
        fn = loc.path
        if os.path.exists(fn):
            try:
                LOG.debug(_("Deleting subject at %(fn)s"), {'fn': fn})
                os.unlink(fn)
            except OSError:
                raise exceptions.Forbidden(
                    message=(_("You cannot delete file %s") % fn))
        else:
            raise exceptions.NotFound(subject=fn)

    def _get_capacity_info(self, mount_point):
        """Calculates total available space for given mount point.

        :mount_point is path of subject data directory
        """

        # Calculate total available space
        stvfs_result = os.statvfs(mount_point)
        total_available_space = stvfs_result.f_bavail * stvfs_result.f_bsize
        return max(0, total_available_space)

    def _find_best_datadir(self, subject_size):
        """Finds the best datadir by priority and free space.

        Traverse directories returning the first one that has sufficient
        free space, in priority order. If two suitable directories have
        the same priority, choose the one with the most free space
        available.
        :param subject_size: size of subject being uploaded.
        :returns: best_datadir as directory path of the best priority datadir.
        :raises: exceptions.StorageFull if there is no datadir in
                self.priority_data_map that can accommodate the subject.
        """
        if not self.multiple_datadirs:
            return self.datadir

        best_datadir = None
        max_free_space = 0
        for priority in self.priority_list:
            for datadir in self.priority_data_map.get(priority):
                free_space = self._get_capacity_info(datadir)
                if free_space >= subject_size and free_space > max_free_space:
                    max_free_space = free_space
                    best_datadir = datadir

            # If datadir is found which can accommodate subject and has maximum
            # free space for the given priority then break the loop,
            # else continue to lookup further.
            if best_datadir:
                break
        else:
            msg = (_("There is no enough disk space left on the subject "
                     "storage media. requested=%s") % subject_size)
            LOG.exception(msg)
            raise exceptions.StorageFull(message=msg)

        return best_datadir

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

        :note:: By default, the backend writes the subject data to a file
              `/<DATADIR>/<ID>`, where <DATADIR> is the value of
              the filesystem_store_datadir configuration option and <ID>
              is the supplied subject ID.
        """

        datadir = self._find_best_datadir(subject_size)
        filepath = os.path.join(datadir, str(subject_id))

        if os.path.exists(filepath):
            raise exceptions.Duplicate(subject=filepath)

        checksum = hashlib.md5()
        bytes_written = 0
        try:
            with open(filepath, 'wb') as f:
                for buf in utils.chunkreadable(subject_file,
                                               self.WRITE_CHUNKSIZE):
                    bytes_written += len(buf)
                    checksum.update(buf)
                    if verifier:
                        verifier.update(buf)
                    f.write(buf)
        except IOError as e:
            if e.errno != errno.EACCES:
                self._delete_partial(filepath, subject_id)
            errors = {errno.EFBIG: exceptions.StorageFull(),
                      errno.ENOSPC: exceptions.StorageFull(),
                      errno.EACCES: exceptions.StorageWriteDenied()}
            raise errors.get(e.errno, e)
        except Exception:
            with excutils.save_and_reraise_exception():
                self._delete_partial(filepath, subject_id)

        checksum_hex = checksum.hexdigest()
        metadata = self._get_metadata(filepath)

        LOG.debug(_("Wrote %(bytes_written)d bytes to %(filepath)s with "
                    "checksum %(checksum_hex)s"),
                  {'bytes_written': bytes_written,
                   'filepath': filepath,
                   'checksum_hex': checksum_hex})

        if self.conf.subject_store.filesystem_store_file_perm > 0:
            perm = int(str(self.conf.subject_store.filesystem_store_file_perm),
                       8)
            try:
                os.chmod(filepath, perm)
            except (IOError, OSError):
                LOG.warning(_LW("Unable to set permission to subject: %s") %
                            filepath)

        return ('file://%s' % filepath, bytes_written, checksum_hex, metadata)

    @staticmethod
    def _delete_partial(filepath, iid):
        try:
            os.unlink(filepath)
        except Exception as e:
            msg = _('Unable to remove partial subject '
                    'data for subject %(iid)s: %(e)s')
            LOG.error(msg % dict(iid=iid,
                                 e=encodeutils.exception_to_unicode(e)))
