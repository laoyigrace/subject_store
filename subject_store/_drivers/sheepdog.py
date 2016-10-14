# Copyright 2013 Taobao Inc.
# Copyright (C) 2016 Nippon Telegraph and Telephone Corporation.
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

"""Storage backend for Sheepdog storage system"""

import hashlib
import logging
import six

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_utils import excutils
from oslo_utils import units

import subject_store
from subject_store import capabilities
from subject_store.common import utils
import subject_store.driver
from subject_store import exceptions
from subject_store.i18n import _
import subject_store.location


LOG = logging.getLogger(__name__)

DEFAULT_ADDR = '127.0.0.1'
DEFAULT_PORT = 7000
DEFAULT_CHUNKSIZE = 64  # in MiB

_SHEEPDOG_OPTS = [
    cfg.IntOpt('sheepdog_store_chunk_size',
               min=1,
               default=DEFAULT_CHUNKSIZE,
               help=_("""
Chunk size for subjects to be stored in Sheepdog data store.

Provide an integer value representing the size in mebibyte
(1048576 bytes) to chunk Glance subjects into. The default
chunk size is 64 mebibytes.

When using Sheepdog distributed storage system, the subjects are
chunked into objects of this size and then stored across the
distributed data store to use for Glance.

Chunk sizes, if a power of two, help avoid fragmentation and
enable improved performance.

Possible values:
    * Positive integer value representing size in mebibytes.

Related Options:
    * None

""")),
    cfg.PortOpt('sheepdog_store_port',
                default=DEFAULT_PORT,
                help=_("""
Port number on which the sheep daemon will listen.

Provide an integer value representing a valid port number on
which you want the Sheepdog daemon to listen on. The default
port is 7000.

The Sheepdog daemon, also called 'sheep', manages the storage
in the distributed cluster by writing objects across the storage
network. It identifies and acts on the messages it receives on
the port number set using ``sheepdog_store_port`` option to store
chunks of Glance subjects.

Possible values:
    * A valid port number (0 to 65535)

Related Options:
    * sheepdog_store_address

""")),
    cfg.StrOpt('sheepdog_store_address',
               default=DEFAULT_ADDR,
               help=_("""
Address to bind the Sheepdog daemon to.

Provide a string value representing the address to bind the
Sheepdog daemon to. The default address set for the 'sheep'
is 127.0.0.1.

The Sheepdog daemon, also called 'sheep', manages the storage
in the distributed cluster by writing objects across the storage
network. It identifies and acts on the messages directed to the
address set using ``sheepdog_store_address`` option to store
chunks of Glance subjects.

Possible values:
    * A valid IPv4 address
    * A valid IPv6 address
    * A valid hostname

Related Options:
    * sheepdog_store_port

"""))
]


class SheepdogSubject(object):
    """Class describing an subject stored in Sheepdog storage."""

    def __init__(self, addr, port, name, chunk_size):
        self.addr = addr
        self.port = port
        self.name = name
        self.chunk_size = chunk_size

    def _run_command(self, command, data, *params):
        cmd = ("collie vdi %(command)s -a %(addr)s -p %(port)d %(name)s "
               "%(params)s" %
               {"command": command,
                "addr": self.addr,
                "port": self.port,
                "name": self.name,
                "params": " ".join(map(str, params))})

        try:
            return processutils.execute(
                cmd, process_input=data)[0]
        except processutils.ProcessExecutionError as exc:
            LOG.error(exc)
            raise subject_store.BackendException(exc)

    def get_size(self):
        """
        Return the size of the this subject

        Sheepdog Usage: collie vdi list -r -a address -p port subject
        """
        out = self._run_command("list -r", None)
        return int(out.split(' ')[3])

    def read(self, offset, count):
        """
        Read up to 'count' bytes from this subject starting at 'offset' and
        return the data.

        Sheepdog Usage: collie vdi read -a address -p port subject offset len
        """
        return self._run_command("read", None, str(offset), str(count))

    def write(self, data, offset, count):
        """
        Write up to 'count' bytes from the data to this subject starting at
        'offset'

        Sheepdog Usage: collie vdi write -a address -p port subject offset len
        """
        self._run_command("write", data, str(offset), str(count))

    def create(self, size):
        """
        Create this subject in the Sheepdog cluster with size 'size'.

        Sheepdog Usage: collie vdi create -a address -p port subject size
        """
        if not isinstance(size, (six.integer_types, float)):
            raise exceptions.Forbidden("Size is not a number")
        self._run_command("create", None, str(size))

    def resize(self, size):
        """Resize this subject in the Sheepdog cluster with size 'size'.

        Sheepdog Usage: collie vdi create -a address -p port subject size
        """
        self._run_command("resize", None, str(size))

    def delete(self):
        """
        Delete this subject in the Sheepdog cluster

        Sheepdog Usage: collie vdi delete -a address -p port subject
        """
        self._run_command("delete", None)

    def exist(self):
        """
        Check if this subject exists in the Sheepdog cluster via 'list' command

        Sheepdog Usage: collie vdi list -r -a address -p port subject
        """
        out = self._run_command("list -r", None)
        if not out:
            return False
        else:
            return True


class StoreLocation(subject_store.location.StoreLocation):
    """
    Class describing a Sheepdog URI. This is of the form:

        sheepdog://addr:port:subject

    """

    def process_specs(self):
        self.subject = self.specs.get('subject')
        self.addr = self.specs.get('addr')
        self.port = self.specs.get('port')

    def get_uri(self):
        return "sheepdog://%(addr)s:%(port)d:%(subject)s" % {
            'addr': self.addr,
            'port': self.port,
            'subject': self.subject}

    def parse_uri(self, uri):
        valid_schema = 'sheepdog://'
        if not uri.startswith(valid_schema):
            reason = _("URI must start with '%s'") % valid_schema
            raise exceptions.BadStoreUri(message=reason)
        pieces = uri[len(valid_schema):].split(':')
        if len(pieces) == 3:
            self.subject = pieces[2]
            self.port = int(pieces[1])
            self.addr = pieces[0]
        # This is used for backwards compatibility.
        else:
            self.subject = pieces[0]
            self.port = self.conf.subject_store.sheepdog_store_port
            self.addr = self.conf.subject_store.sheepdog_store_address


class SubjectIterator(object):
    """
    Reads data from an Sheepdog subject, one chunk at a time.
    """

    def __init__(self, subject):
        self.subject = subject

    def __iter__(self):
        subject = self.subject
        total = left = subject.get_size()
        while left > 0:
            length = min(subject.chunk_size, left)
            data = subject.read(total - left, length)
            left -= len(data)
            yield data
        raise StopIteration()


class Store(subject_store.driver.Store):
    """Sheepdog backend adapter."""

    _CAPABILITIES = (capabilities.BitMasks.RW_ACCESS |
                     capabilities.BitMasks.DRIVER_REUSABLE)
    OPTIONS = _SHEEPDOG_OPTS
    EXAMPLE_URL = "sheepdog://addr:port:subject"

    def get_schemes(self):
        return ('sheepdog',)

    def configure_add(self):
        """
        Configure the Store to use the stored configuration options
        Any store that needs special configuration should implement
        this method. If the store was not able to successfully configure
        itself, it should raise `exceptions.BadStoreConfiguration`
        """

        try:
            chunk_size = self.conf.subject_store.sheepdog_store_chunk_size
            self.chunk_size = chunk_size * units.Mi
            self.READ_CHUNKSIZE = self.chunk_size
            self.WRITE_CHUNKSIZE = self.READ_CHUNKSIZE

            self.addr = self.conf.subject_store.sheepdog_store_address
            self.port = self.conf.subject_store.sheepdog_store_port
        except cfg.ConfigFileValueError as e:
            reason = _("Error in store configuration: %s") % e
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name='sheepdog',
                                                   reason=reason)

        try:
            processutils.execute("collie")
        except processutils.ProcessExecutionError as exc:
            reason = _("Error in store configuration: %s") % exc
            LOG.error(reason)
            raise exceptions.BadStoreConfiguration(store_name='sheepdog',
                                                   reason=reason)

    @capabilities.check
    def get(self, location, offset=0, chunk_size=None, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file, and returns a generator for reading
        the subject file

        :param location: `subject_store.location.Location` object, supplied
                        from subject_store.location.get_location_from_uri()
        :raises: `subject_store.exceptions.NotFound` if subject does not exist
        """

        loc = location.store_location
        subject = SheepdogSubject(loc.addr, loc.port, loc.subject,
                              self.READ_CHUNKSIZE)
        if not subject.exist():
            raise exceptions.NotFound(_("Sheepdog subject %s does not exist")
                                      % subject.name)
        return (SubjectIterator(subject), subject.get_size())

    def get_size(self, location, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file and returns the subject size

        :param location: `subject_store.location.Location` object, supplied
                        from subject_store.location.get_location_from_uri()
        :raises: `subject_store.exceptions.NotFound` if subject does not exist
        :param rtype: int
        """

        loc = location.store_location
        subject = SheepdogSubject(loc.addr, loc.port, loc.subject,
                              self.READ_CHUNKSIZE)
        if not subject.exist():
            raise exceptions.NotFound(_("Sheepdog subject %s does not exist")
                                      % subject.name)
        return subject.get_size()

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

        :retval: tuple of URL in backing store, bytes written, and checksum
        :raises: `subject_store.exceptions.Duplicate` if the subject already
                existed
        """

        subject = SheepdogSubject(self.addr, self.port, subject_id,
                              self.WRITE_CHUNKSIZE)
        if subject.exist():
            raise exceptions.Duplicate(_("Sheepdog subject %s already exists")
                                       % subject_id)

        location = StoreLocation({
            'subject': subject_id,
            'addr': self.addr,
            'port': self.port
        }, self.conf)

        subject.create(subject_size)

        try:
            offset = 0
            checksum = hashlib.md5()
            chunks = utils.chunkreadable(subject_file, self.WRITE_CHUNKSIZE)
            for chunk in chunks:
                chunk_length = len(chunk)
                # If the subject size provided is zero we need to do
                # a resize for the amount we are writing. This will
                # be slower so setting a higher chunk size may
                # speed things up a bit.
                if subject_size == 0:
                    subject.resize(offset + chunk_length)
                subject.write(chunk, offset, chunk_length)
                offset += chunk_length
                checksum.update(chunk)
                if verifier:
                    verifier.update(chunk)
        except Exception:
            # Note(zhiyan): clean up already received data when
            # error occurs such as SubjectSizeLimitExceeded exceptions.
            with excutils.save_and_reraise_exception():
                subject.delete()

        return (location.get_uri(), offset, checksum.hexdigest(), {})

    @capabilities.check
    def delete(self, location, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file to delete

        :param location: `subject_store.location.Location` object, supplied
                  from subject_store.location.get_location_from_uri()

        :raises: NotFound if subject does not exist
        """

        loc = location.store_location
        subject = SheepdogSubject(loc.addr, loc.port, loc.subject,
                              self.WRITE_CHUNKSIZE)
        if not subject.exist():
            raise exceptions.NotFound(_("Sheepdog subject %s does not exist") %
                                      loc.subject)
        subject.delete()
