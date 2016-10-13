# Copyright 2011 OpenStack Foundation
# Copyright 2012 RedHat Inc.
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

"""Base class for all storage backends"""

import logging

from oslo_config import cfg
from oslo_utils import encodeutils
from oslo_utils import importutils
from oslo_utils import units

from subject_store import capabilities
from subject_store import exceptions
from subject_store.i18n import _

LOG = logging.getLogger(__name__)


class Store(capabilities.StoreCapability):

    OPTIONS = None
    READ_CHUNKSIZE = 4 * units.Mi  # 4M
    WRITE_CHUNKSIZE = READ_CHUNKSIZE

    def __init__(self, conf):
        """
        Initialize the Store
        """

        super(Store, self).__init__()

        self.conf = conf
        self.store_location_class = None

        try:
            if self.OPTIONS is not None:
                self.conf.register_opts(self.OPTIONS, group='subject_store')
        except cfg.DuplicateOptError:
            pass

    def configure(self, re_raise_bsc=False):
        """
        Configure the store to use the stored configuration options
        and initialize capabilities based on current configuration.

        Any store that needs special configuration should implement
        this method.
        """

        try:
            self.configure_add()
        except exceptions.BadStoreConfiguration as e:
            self.unset_capabilities(capabilities.BitMasks.WRITE_ACCESS)
            msg = (_(u"Failed to configure store correctly: %s "
                     "Disabling add method.")
                   % encodeutils.exception_to_unicode(e))
            LOG.warning(msg)
            if re_raise_bsc:
                raise
        finally:
            self.update_capabilities()

    def get_schemes(self):
        """
        Returns a tuple of schemes which this store can handle.
        """
        raise NotImplementedError

    def get_store_location_class(self):
        """
        Returns the store location class that is used by this store.
        """
        if not self.store_location_class:
            class_name = "%s.StoreLocation" % (self.__module__)
            LOG.debug("Late loading location class %s", class_name)
            self.store_location_class = importutils.import_class(class_name)
        return self.store_location_class

    def configure_add(self):
        """
        This is like `configure` except that it's specifically for
        configuring the store to accept objects.

        If the store was not able to successfully configure
        itself, it should raise `exceptions.BadStoreConfiguration`.
        """
        # NOTE(flaper87): This should probably go away

    @capabilities.check
    def get(self, location, offset=0, chunk_size=None, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file, and returns a tuple of generator
        (for reading the subject file) and subject_size

        :param location: `subject_store.location.Location` object, supplied
                        from subject_store.location.get_location_from_uri()
        :raises: `subject.exceptions.NotFound` if subject does not exist
        """
        raise NotImplementedError

    def get_size(self, location, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file, and returns the size

        :param location: `subject_store.location.Location` object, supplied
                        from subject_store.location.get_location_from_uri()
        :raises: `subject_store.exceptions.NotFound` if subject does not exist
        """
        raise NotImplementedError

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

        :retval: tuple of URL in backing store, bytes written, checksum
               and a dictionary with storage system specific information
        :raises: `subject_store.exceptions.Duplicate` if the subject already
                existed
        """
        raise NotImplementedError

    @capabilities.check
    def delete(self, location, context=None):
        """
        Takes a `subject_store.location.Location` object that indicates
        where to find the subject file to delete

        :param location: `subject_store.location.Location` object, supplied
                  from subject_store.location.get_location_from_uri()
        :raises: `subject_store.exceptions.NotFound` if subject does not exist
        """
        raise NotImplementedError

    def set_acls(self, location, public=False, read_tenants=None,
                 write_tenants=None, context=None):
        """
        Sets the read and write access control list for an subject in the
        backend store.

        :param location: `subject_store.location.Location` object, supplied
                  from subject_store.location.get_location_from_uri()
        :param public: A boolean indicating whether the subject should be public.
        :param read_tenants: A list of tenant strings which should be granted
                      read access for an subject.
        :param write_tenants: A list of tenant strings which should be granted
                      write access for an subject.
        """
        raise NotImplementedError
