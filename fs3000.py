# Copyright (c) 2014 - 2015 CarryCloud Corporation.
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
Drivers for CarryCloud FS3000 array based on RESTful API.
"""

import cookielib
import json
import random
import re
import types
import urllib2

from oslo_concurrency import lockutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import timeutils
import six
import taskflow.engines
from taskflow.patterns import linear_flow
from taskflow import task
from taskflow.utils import misc

from cinder import exception
from cinder.i18n import _, _LW, _LI, _LE
from cinder import objects
from cinder.volume.configuration import Configuration
from cinder.volume.drivers.san import san
from cinder.volume import manager
from cinder.volume import utils as vol_utils
from cinder.volume import volume_types
from cinder.zonemanager import utils as zm_utils
from cinder import utils
from cinder.volume.drivers.infortrend.eonstor_ds_cli import cli_factory as cli

LOG = logging.getLogger(__name__)


CONF = cfg.CONF
VERSION = '00.00.01'

GiB = 1024 * 1024 * 1024
ENABLE_TRACE = False

loc_opts = [
    cfg.StrOpt('storage_pool_names',
               default=None,
               deprecated_name='storage_pool_name',
               help='Comma-separated list of storage pool names to be used.'),
    cfg.StrOpt('storage_protocol',
               default='iSCSI',
               help='Protocol to access the storage '
                    'allocated from this Cinder backend')]

CONF.register_opts(loc_opts)


def decorate_all_methods(method_decorator):
    """Applies decorator on the methods of a class.

    This is a class decorator, which will apply method decorator referred
    by method_decorator to all the public methods (without underscore as
    the prefix) in a class.
    """
    if not ENABLE_TRACE:
        return lambda cls: cls

    def _decorate_all_methods(cls):
        for attr_name, attr_val in cls.__dict__.items():
            if (isinstance(attr_val, types.FunctionType) and
                    not attr_name.startswith("_")):
                setattr(cls, attr_name, method_decorator(attr_val))
        return cls

    return _decorate_all_methods


def log_enter_exit(func):
    if not CONF.debug:
        return func

    def inner(self, *args, **kwargs):
        LOG.debug("Entering %(cls)s.%(method)s",
                  {'cls': self.__class__.__name__,
                   'method': func.__name__})
        start = timeutils.utcnow()
        ret = func(self, *args, **kwargs)
        end = timeutils.utcnow()
        LOG.debug("Exiting %(cls)s.%(method)s. "
                  "Spent %(duration)s sec. "
                  "Return %(return)s",
                  {'cls': self.__class__.__name__,
                   'duration': timeutils.delta_seconds(start, end),
                   'method': func.__name__,
                   'return': ret})
        return ret
    return inner


@decorate_all_methods(log_enter_exit)
class CCFS3000RESTClient(object):
    """CarryCloud FS3000 Client interface handing REST calls and responses."""

    HEADERS = {'Accept': 'application/json',
               'Content-Type': 'application/json'}
    HostTypeEnum_HostManual = 1
    HostLUNTypeEnum_LUN = 1
    HostLUNAccessEnum_NoAccess = 0
    HostLUNAccessEnum_Production = 1

    def __init__(self, host, port=443, user='', password='', debug=False):
        self.username = user
        self.password = password
        self.mgmt_url = 'https://%(host)s:%(port)s' % {'host': host,
						       'port': port}
        self.debug = debug
        self.cookie_jar = cookielib.CookieJar()
        self.cookie_handler = urllib2.HTTPCookieProcessor(self.cookie_jar)
        self.url_opener = urllib2.build_opener(self.cookie_handler)

    def _http_log_req(self, req):
        if not self.debug:
            return

        string_parts = ['curl -i']
        string_parts.append(' -X %s' % req.get_method())

        for k in req.headers:
            header = ' -H "%s: %s"' % (k, req.headers[k])
            string_parts.append(header)

        if req.data:
            string_parts.append(" -d '%s'" % (req.data))
        string_parts.append(' ' + req.get_full_url())
        LOG.debug("\nREQ: %s\n", "".join(string_parts))

    def _http_log_resp(self, resp, body, failed_req=None):
        if not self.debug and failed_req is None:
            return
        if failed_req:
            LOG.error(
                _LE('REQ: [%(method)s] %(url)s %(req_hdrs)s\n'
                    'REQ BODY: %(req_b)s\n'
                    'RESP: [%(code)s] %(resp_hdrs)s\n'
                    'RESP BODY: %(resp_b)s\n'),
                {'method': failed_req.get_method(),
                 'url': failed_req.get_full_url(),
                 'req_hdrs': failed_req.headers,
                 'req_b': failed_req.data,
                 'code': resp.getcode(),
                 'resp_hdrs': str(resp.headers).replace('\n', '\\n'),
                 'resp_b': body})
        else:
            LOG.debug(
                "RESP: [%s] %s\nRESP BODY: %s\n",
                resp.getcode(),
                str(resp.headers).replace('\n', '\\n'),
                body)

    def _http_log_err(self, err, req):
        LOG.error(
            _LE('REQ: [%(method)s] %(url)s %(req_hdrs)s\n'
                'REQ BODY: %(req_b)s\n'
                'ERROR CODE: [%(code)s] \n'
                'ERROR REASON: %(resp_e)s\n'),
            {'method': req.get_method(),
             'url': req.get_full_url(),
             'req_hdrs': req.headers,
             'req_b': req.data,
             'code': err.code,
             'resp_e': err.reason})

    def _getRelURL(self, parameter):
        strPara = ""
        for key, value in parameter.items() :
            strPara += "&%(key)s=%(value)s" % {'key' : key, 'value' : value}
        strURL = "/serviceHandler.php?" + strPara
	if self.debug:
	    LOG.debug("_getRelURL = %s" % strURL)
        return strURL

    def _request(self, rel_url, req_data=None, method=None,
                 return_rest_err=True):
        req_body = None if req_data is None else json.dumps(req_data)
        err = None
        resp_data = None
        url = self.mgmt_url + rel_url
        req = urllib2.Request(url, req_body, CCFS3000RESTClient.HEADERS)
        if method not in (None, 'GET', 'POST'):
            req.get_method = lambda: method
        self._http_log_req(req)
        try:
            resp = self.url_opener.open(req)
            resp_body = resp.read()
            resp_data = json.loads(resp_body) if resp_body else None
            self._http_log_resp(resp, resp_body)
        except urllib2.HTTPError as http_err:
            if hasattr(http_err, 'read'):
                resp_body = http_err.read()
                self._http_log_resp(http_err, resp_body, req)
                if resp_body:
                    err = json.loads(resp_body)['error']
                else:
                    err = {'errorCode': -1,
                           'httpStatusCode': http_err.code,
                           'messages': six.text_type(http_err),
                           'request': req}
            else:
                self._http_log_err(http_err, req)
                resp_data = http_err.reason
                err = {'errorCode': -1,
                       'httpStatusCode': http_err.code,
                       'messages': six.text_type(http_err),
                       'request': req}

            if not return_rest_err:
                raise exception.VolumeBackendAPIException(data=err)
        return (err, resp_data) if return_rest_err else resp_data

    def _login(self):
        url_parameter = {'service' : 'LoginService',
                         'action' : 'login',
                         'account' : self.username,
                         'password' : self.password}
        login_rel = self._getRelURL(url_parameter)
        err, resp = self._request(login_rel)

	if not err:
	    php_session_id = None
	    for cookie in self.cookie_jar:
		if cookie.name == "PHPSESSID":
		    php_session_id = cookie.value
		    break
	    cookie_content = 'PHPSESSID=%s'%php_session_id
	    self.url_opener.addheaders.append(('Cookie', cookie_content))
	return err, resp

    def _logout(self):
        url_parameter = {'service' : 'LoginService',
                         'action' : 'logout'}
	self._request(self._getRelURL(url_parameter))

    def request (self, url_para):
        err, resp = self._login()
	if not err:
	    rel_url = self._getRelURL(url_para)
	    err, resp = self._request(rel_url)
	    self._logout()
        return err, resp

    def get_pools(self, fields=None):
        url_parameter = {'service' : 'VgService',
                         'action' : 'getAllVGs'}
        err, pools = self.request(url_parameter)
	return pools if not err else None

    def get_luns(self):
        url_parameter = {'service' : 'LvService',
                         'action' : 'getAllLVs'}
        return self.request(url_parameter)

    def get_lun_by_name(self, name):
        err, luns = self.get_luns()
	if not err:
	    for lun in luns :
		if lun['Name'] == name:
		    return err, lun
	return err, None

    def get_lun_by_id(self, lun_id):
        err, luns = self.get_luns()
        if not err:
	    for lun in luns :
	        if lun['Id'] == lun_id:
		    return err, lun
	return err, None
                
    def get_snaps_by_lunid(self, lun_id):
        url_parameter = {'service' : 'LvService',
                         'action' : 'getSnapshotsByLv',
                         'lvId' : lun_id}
        return self.request(url_parameter)

    def get_snap_by_name (self, lun_id, snapname):
        err, snaps = self.get_snaps_by_lunid(lun_id)
	if not err:
	    for snap in snaps :
		if snap['Name'] == snapname:
		    return err, snap
        return err, None

    def get_system_info(self):
        url_parameter = {'service' : 'SysinfoService',
                         'action' : 'getDetail'}
        err, resp = self.request(url_parameter)
        return None if err else resp
    def get_active_fc_wwns(self, initiator):
        url_parameter = {'service' : 'FCLunMappingService',
                         'action' : 'getTargetPortNameByInitator',
                         'wwn' : initiator}
        err, resp = self.request(url_parameter)
        return resp

    def create_lun(self, pool_id, name, size, **kwargs):
        url_para = {'service' : 'LvService',
                    'action' : 'createLv',
                    'name' : name,
                    'vgId' : pool_id,
                    'sizeGB' : size}
        err, resp = self.request(url_para)
        return (err, None) if err else \
            (err, resp)

        #lun_create_url = '/api/types/storageResource/action/createLun'
        #lun_parameters = {'pool': {"id": pool_id},
        #                  'isThinEnabled': True,
        #                  'size': size}
        #if 'is_thin' in kwargs:
        #    lun_parameters['isThinEnabled'] = kwargs['is_thin']
        # More Advance Feature
        #data = {'name': name,
        #        'description': name,
        #        'lunParameters': lun_parameters}
        #err, resp = self._request(lun_create_url, data)
        #return (err, None) if err else \
        #    (err, resp['content']['storageResource'])

    def delete_lun(self, lun_id, force_snap_deletion=False):
        url_para = {'service' : 'LvService',
                    'action' : 'deleteLv',
                    'id' : lun_id}
        return self.request(url_para)

    def create_host(self, hostname):
        host_create_url = '/api/types/host/instances'
        data = {'type': CCFS3000RESTClient.HostTypeEnum_HostManual,
                'name': hostname}
        err, resp = self._request(host_create_url, data)
        return (err, None) if err else (err, resp['content'])

    def delete_host(self, host_id):
        host_delete_url = '/api/instances/host/%s' % host_id
        err, resp = self._request(host_delete_url, None, 'DELETE')
        return err, resp

    def create_initiator(self, initiator_uid, host_id):
        initiator_create_url = '/api/types/hostInitiator/instances'
        data = {'host': {'id': host_id},
                'initiatorType': 2 if initiator_uid.lower().find('iqn') == 0
                else 1,
                'initiatorWWNorIqn': initiator_uid}
        err, resp = self._request(initiator_create_url, data)
        return (err, None) if err else (err, resp['content'])

    def register_initiator(self, initiator_id, host_id):
        initiator_register_url = \
            '/api/instances/hostInitiator/%s/action/register' % initiator_id
        data = {'host': {'id': host_id}}
        err, resp = self._request(initiator_register_url, data)
        return err, resp

    def get_host_lun_by_ends(self, host_id, lun_id,
                             protocol):
        if protocol == 'FC':
            url_para = {'service' : 'FCLunMappingService',
                        'action' : 'getAllFCLunMappings'}
        elif protocol == 'iSCSI':
            url_para = {'service' : 'LunMappingService',
                        'action' : 'getAllFS3000LunMappings'}
        err, resp = self.request(url_para)
        LOG.debug("lun map %s", resp)
        if err:
            return -1
        for mapping in resp:
            if (mapping['lvId'] == lun_id):
                return mapping['lunNumber']

    def _get_avail_host_lun(self, protocol, initiator):
        if protocol == 'FC':
            url_para = {'service' : 'FCLunMappingService',
                        'action' : 'echoUsedLunsByWWN',
                        'wwn' : initiator}
        elif protocol == 'iSCSI':
            url_para = {'service' : 'LunMappingService',
                        'action' : 'echoUsedLunsByInitiator',
                        'initiator' : initiator}
        else:
            msg = _('storage_protocol %(invalid)s is not supported. '
                    ) % {'invalid': protocol}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        err, resp = self.request(url_para)
        host_lun = 0
        resp.sort()
        for used_lun in resp:
            if (host_lun == int(used_lun)):
                host_lun += 1
        
        LOG.debug("take host_lun %s, used_lun %s",host_lun, resp)
        return host_lun

    def expose_lun(self, lun_id, initiator, protocol):
        host_lun = self._get_avail_host_lun(protocol, initiator)
        LOG.debug("expose_lun lun_id %s init %s host_lun %s",
            lun_id, initiator, host_lun)
        if protocol == 'FC':
            url_para = {'service' : 'FCLunMappingService',
                        'action' : 'createFCLunMapping',
                        'lvId' : lun_id,
                        'lunNumber' : host_lun,
                        'wwn' : initiator}
        elif protocol == 'iSCSI':
            url_para = {'service' : 'LunMappingService',
                        'action' : 'createLunMappingWithChapUser',
                        'lvId' : lun_id,
                        'lunNumber' : host_lun,
                        'initiator' : initiator}
        else:
            msg = _('storage_protocol %(invalid)s is not supported. '
                    ) % {'invalid': protocol}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        err, resp = self.request(url_para)
        return err, resp

    def hide_lun(self, lun_id, host_id, protocol):
        LOG.debug("hide_lun lun_id %s, host_id %s, proto %s",
            lun_id, host_id, protocol)
        host_lun = self.get_host_lun_by_ends(host_id, lun_id, protocol)
        if protocol == 'FC':
            url_para = {'service' : 'FCLunMappingService',
                        'action' : 'deleteFCLunMapping',
                        'lunNumber' : host_lun,
                        'wwn' : host_id}
        elif protocol == 'iSCSI':
            url_para = {'service' : 'LunMappingService',
                        'action' : 'deleteLunMappingWithChapUser',
                        'lunNumber' : host_lun,
                        'initiator' : host_id}
        err, resp = self.request(url_para)
        return err,resp

    def create_snap(self, lun_id, snap_name, lun_size, snap_description=None):
        url_para = {'service' : 'LvService',
                    'action' : 'createSnapshot',
                    'lvId' : lun_id,
                    'name' : snap_name,
                    'spaceGB' : lun_size}
        return self.request(url_para)

    def delete_snap(self, snap_id):
        """Deletes the snap by the snap_id."""
        url_para = {'service' : 'LvService',
                    'action' : 'deleteSnapshot',
                    'snapshotId' : snap_id}
        return self.request(url_para)

    def extend_lun(self, lun_id, size):
        url_para = {'service' : 'LvService',
                    'action' : 'doExpandLvSize',
                    'lvId' : lun_id,
                    'expandedSizeMB' : size*1024}
        err, resp = self.request(url_para)
        return (err, None) if err else \
            (err, resp)

    def modify_lun_name(self, lun_id, new_name):
        """Modify the lun name."""
        lun_modify_url = \
            '/api/instances/storageResource/%s/action/modifyLun' % lun_id
        data = {'name': new_name}
        err, resp = self._request(lun_modify_url, data)
        if err:
            if err['errorCode'] in (0x6701020,):
                LOG.warning(_LW('Nothing to modify, the lun %(lun)s '
                                'already has the name %(name)s.'),
                            {'lun': lun_id, 'name': new_name})
            else:
                reason = (_('Manage existing lun failed. Can not '
                          'rename the lun %(lun)s to %(name)s') %
                          {'lun': lun_id, 'name': new_name})
                raise exception.VolumeBackendAPIException(
                    data=reason)


class ArrangeHostTask(task.Task):

    def __init__(self, helper, connector):
        LOG.debug('ArrangeHostTask.__init__ %s', connector)
        super(ArrangeHostTask, self).__init__(provides='host_id')
        self.helper = helper
        self.connector = connector

    def execute(self, *args, **kwargs):
        LOG.debug('ArrangeHostTask.execute %s', self.connector)
        host_id = self.helper.arrange_host(self.connector)
        return host_id


class ExposeLUNTask(task.Task):
    def __init__(self, helper, volume, lun_data):
        LOG.debug('ExposeLUNTask.__init__ %s', lun_data)
        super(ExposeLUNTask, self).__init__()
        self.helper = helper
        self.lun_data = lun_data
        self.volume = volume

    def execute(self, host_id):
        LOG.debug('ExposeLUNTask.execute %(vol)s %(host)s'
                  % {'vol': self.lun_data,
                     'host': host_id})
        self.helper.expose_lun(self.volume, self.lun_data, host_id)

    def revert(self, result, host_id, *args, **kwargs):
        LOG.warning(_LW('ExposeLUNTask.revert %(vol)s %(host)s'),
                    {'vol': self.lun_data, 'host': host_id})
        if isinstance(result, misc.Failure):
            LOG.warning(_LW('ExposeLUNTask.revert: Nothing to revert'))
            return
        else:
            LOG.warning(_LW('ExposeLUNTask.revert: hide_lun'))
            self.helper.hide_lun(self.volume, self.lun_data, host_id)


class GetConnectionInfoTask(task.Task):

    def __init__(self, helper, volume, lun_data, connector, *argv, **kwargs):
        LOG.debug('GetConnectionInfoTask.__init__ %(vol)s %(conn)s',
                  {'vol': lun_data, 'conn': connector})
        super(GetConnectionInfoTask, self).__init__(provides='connection_info')
        self.helper = helper
        self.lun_data = lun_data
        self.connector = connector
        self.volume = volume

    def execute(self, host_id):
        LOG.debug('GetConnectionInfoTask.execute %(vol)s %(conn)s %(host)s',
                  {'vol': self.lun_data, 'conn': self.connector,
                   'host': host_id})
        return self.helper.get_connection_info(self.volume,
                                               self.connector,
                                               #self.lun_data['currentNode'],
                                               self.lun_data['Id'],
                                               host_id)


@decorate_all_methods(log_enter_exit)
class CCFS3000Helper(object):

    stats = {'driver_version': VERSION,
             'storage_protocol': None,
             'free_capacity_gb': 'unknown',
             'reserved_percentage': 0,
             'total_capacity_gb': 'unknown',
             'vendor_name': 'CarryCloud',
             'volume_backend_name': None}

    def __init__(self, conf):
        self.configuration = conf
        self.configuration.append_config_values(loc_opts)
        self.configuration.append_config_values(san.san_opts)
        self.storage_protocol = conf.storage_protocol
        self.supported_storage_protocols = ('iSCSI', 'FC')
        if self.storage_protocol not in self.supported_storage_protocols:
            msg = _('storage_protocol %(invalid)s is not supported. '
                    'The valid one should be among %(valid)s.') % {
                'invalid': self.storage_protocol,
                'valid': self.supported_storage_protocols}
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        self.active_storage_ip = self.configuration.san_ip
        self.storage_username = self.configuration.san_login
        self.storage_password = self.configuration.san_password
        self.max_over_subscription_ratio = (
            self.configuration.max_over_subscription_ratio)
        self.lookup_service_instance = None
        self.lookup_service = zm_utils.create_lookup_service()
        # Here we use group config to keep same as cinder manager
        zm_conf = Configuration(manager.volume_manager_opts)
        if (zm_conf.safe_get('zoning_mode') == 'fabric' or
                self.configuration.safe_get('zoning_mode') == 'fabric'):
            from cinder.zonemanager.fc_san_lookup_service \
                import FCSanLookupService
            self.lookup_service_instance = \
                FCSanLookupService(configuration=self.configuration)
        self.client = CCFS3000RESTClient(self.active_storage_ip, 443,
                                        self.storage_username,
                                        self.storage_password,
                                        debug=CONF.debug)
        system_info = self.client.get_system_info()

        if not system_info:
            msg = _('Basic system information is unavailable.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        self.storage_serial_number = system_info['serialNumber']
        conf_pools = self.configuration.safe_get("storage_pool_names")
        # When managed_all_pools is True, the storage_pools_map will be
        # updated in update_volume_stats.
        self.is_managing_all_pools = False if conf_pools else True
        self.storage_pools_map = self._get_managed_storage_pools_map(
            conf_pools)
        #self.thin_enabled = False
        #self.storage_targets = self._get_storage_targets()

    def _get_managed_storage_pools_map(self, pools):

        managed_pools = self.client.get_pools()
        if pools:
            storage_pool_names = set([po.strip() for po in pools.split(",")])
            array_pool_names = set([po['Name'] for po in managed_pools])
            non_exist_pool_names = storage_pool_names.difference(
                array_pool_names)
            storage_pool_names.difference_update(non_exist_pool_names)
            if not storage_pool_names:
                msg = _("All the specified storage pools to be managed "
                        "do not exist. Please check your configuration. "
                        "Non-existent "
                        "pools: %s") % ",".join(non_exist_pool_names)
                raise exception.VolumeBackendAPIException(data=msg)
            if non_exist_pool_names:
                LOG.warning(_LW("The following specified storage pools "
                                "do not exist: %(unexist)s. "
                                "This host will only manage the storage "
                                "pools: %(exist)s"),
                            {'unexist': ",".join(non_exist_pool_names),
                             'exist': ",".join(storage_pool_names)})
            else:
                LOG.debug("This host will manage the storage pools: %s.",
                          ",".join(storage_pool_names))

            managed_pools = filter(lambda po: po['Name'] in storage_pool_names,
                                   managed_pools)
        else:
            LOG.debug("No storage pool is configured. This host will "
                      "manage all the pools on the FS3000 system.")

        return self._build_storage_pool_id_map(managed_pools)

    def _build_storage_pool_id_map(self, pools):
        return {po['Name']: po['Id'] for po in pools}

    def _get_iscsi_targets(self):
        res = {'a': [], 'b': []}
        node_dict = {}
        for node in self.client.get_iscsi_nodes(('id', 'name')):
            node_dict[node['id']] = node['name']
        fields = ('id', 'ipAddress', 'ethernetPort', 'iscsiNode')
        pat = re.compile(r'sp(a|b)', flags=re.IGNORECASE)
        for portal in self.client.get_iscsi_portals(fields):
            eth_id = portal['ethernetPort']['id']
            node_id = portal['iscsiNode']['id']
            m = pat.match(eth_id)
            if m:
                sp = m.group(1).lower()
                item = (node_dict[node_id], portal['ipAddress'],
                        portal['id'])
                res[sp].append(item)
            else:
                LOG.warning(_LW('SP of %s is unknown'), portal['id'])
        return res

    def _get_fc_targets(self):
        res = {'a': [], 'b': []}
        fields = ('id', 'wwn', 'storageProcessorId')
        pat = re.compile(r'sp(a|b)', flags=re.IGNORECASE)
        for port in self.client.get_fc_ports(fields):
            sp_id = port['storageProcessorId']['id']
            m = pat.match(sp_id)
            if m:
                sp = m.group(1).lower()
                wwn = port['wwn'].replace(':', '')
                node_wwn = wwn[0:16]
                port_wwn = wwn[16:32]
                item = (node_wwn, port_wwn, port['id'])
                res[sp].append(item)
            else:
                LOG.warning(_LW('SP of %s is unknown'), port['id'])
        return res

    def _get_storage_targets(self, connector):
        if self.storage_protocol == 'iSCSI':
            return self._get_iscsi_targets(connector)
        elif self.storage_protocol == 'FC':
            return self._get_fc_targets(connector)
        else:
            return {'a': [], 'b': []}

    def _get_volumetype_extraspecs(self, volume):
        specs = {}

        type_id = volume['volume_type_id']
        if type_id is not None:
            specs = volume_types.get_volume_type_extra_specs(type_id)

        return specs

    def _load_provider_location(self, provider_location):
        pl_dict = {}
        for item in provider_location.split('|'):
            k_v = item.split('^')
            if len(k_v) == 2 and k_v[0]:
                pl_dict[k_v[0]] = k_v[1]
        return pl_dict

    def _dumps_provider_location(self, pl_dict):
        return '|'.join([k + '^' + pl_dict[k] for k in pl_dict])

    def get_lun_by_id(self, lun_id):
        err, lun = self.client.get_lun_by_id(lun_id)
        if err or not lun:
            raise exception.VolumeBackendAPIException(
                "Cannot find lun with id : %s" % lun_id)
        return lun

    def _get_target_storage_pool_name(self, volume):
        return vol_utils.extract_host(volume['host'], 'pool')

    def _get_target_storage_pool_id(self, volume):
        name = self._get_target_storage_pool_name(volume)
        return self.storage_pools_map[name]

    def create_volume(self, volume):
        name = volume['display_name']+'-'+volume['name']
        size = volume['size']
        #extra_specs = self._get_volumetype_extraspecs(volume)
        #k = 'storagetype:provisioning'
        is_thin = False
        #if k in extra_specs:
        #    v = extra_specs[k].lower()
        #    if v == 'thin':
        #        is_thin = True
        #    elif v == 'thick':
        #        is_thin = False
        #    else:
        #        msg = _('Value %(v)s of %(k)s is invalid') % {'k': k, 'v': v}
        #        LOG.error(msg)
        #        raise exception.VolumeBackendAPIException(data=msg)
        err, resp = self.client.create_lun(
            self._get_target_storage_pool_id(volume), name, size,
            is_thin=is_thin)
        if err:
            raise exception.VolumeBackendAPIException(data=err['messages'])

        err, lun = self.client.get_lun_by_name(name)
        if err:
            raise exception.VolumeBackendAPIException(data=err['messages'])
	elif not lun:
	    err_msg = 'can not get created LV by name %s' % name
            raise exception.VolumeBackendAPIException(data=err_msg)

        #if volume.get('consistencygroup_id'):
        #    cg_id = (
        #        self._get_group_id_by_name(volume.get('consistencygroup_id')))
        #    err, res = self.client.update_consistencygroup(cg_id, [lun['id']])
        #    if err:
        #        raise exception.VolumeBackendAPIException(data=err['messages'])

        pl_dict = {'system': self.storage_serial_number,
                   'type': 'lun',
                   'id': lun['Id']}
        model_update = {'provider_location':
                        self._dumps_provider_location(pl_dict)}
        volume['provider_location'] = model_update['provider_location']
        return model_update

    def _extra_lun_or_snap_id(self, volume):
        if volume.get('provider_location') is None:
            return None
        pl_dict = self._load_provider_location(volume['provider_location'])
        res_type = pl_dict.get('type', None)
        if 'lun' == res_type or 'snap' == res_type:
            if pl_dict.get('id', None):
                return pl_dict['id']
        msg = _('Fail to find LUN ID of %(vol)s in from %(pl)s') % {
            'vol': volume['name'], 'pl': volume['provider_location']}
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def delete_volume(self, volume):
        lun_id = self._extra_lun_or_snap_id(volume)
        err, resp = self.client.delete_lun(lun_id)
        if err:
            if not self.client.get_lun_by_id(lun_id):
                LOG.warning(_LW("LUN %(name)s is already deleted or does not "
                                "exist. Message: %(msg)s"),
                            {'name': volume['name'], 'msg': err['messages']})
            else:
                raise exception.VolumeBackendAPIException(data=err['messages'])

    def create_snapshot(self, snapshot, name, snap_desc):
        """This function will create a snapshot of the given volume."""
        LOG.debug('Entering CCFS3000Helper.create_snapshot.')
        lun_id = self._extra_lun_or_snap_id(snapshot['volume'])
        lun_size = snapshot['volume']['size']
        if not lun_id:
            msg = _('Failed to get LUN ID for volume %s') %\
                snapshot['volume']['name']
            raise exception.VolumeBackendAPIException(data=msg)
        err, resp = self.client.create_snap(
            lun_id, name, lun_size, snap_desc)
	if err:
            raise exception.VolumeBackendAPIException(data=err['messages'])

        err, snap = self.client.get_snap_by_name(lun_id, name)
        if err:
            raise exception.VolumeBackendAPIException(data=err['messages'])
        elif not snap:
	    err_msg = 'can not get snapshot %(name)s by lun_id %(lun_id)s' % {'name': name, 'lun_id': lun_id}
            raise exception.VolumeBackendAPIException(data=err_msg)

        pl_dict = {'system': self.storage_serial_number,
                   'type': 'snap',
                   'id': snap['Id']}
        model_update = {'provider_location':
                        self._dumps_provider_location(pl_dict)}
        snapshot['provider_location'] = model_update['provider_location']
        return model_update

    def delete_snapshot(self, snapshot):
        """Gets the snap id by the snap name and delete the snapshot."""
        snap_id = self._extra_lun_or_snap_id(snapshot)
        if not snap_id:
            return
        err, resp = self.client.delete_snap(snap_id)
        if err:
            raise exception.VolumeBackendAPIException(data=err['messages'])

    def extend_volume(self, volume, new_size):
	origin_size = volume['size']
	if origin_size >= new_size:
	    raise exception.VolumeBackendAPIException("New size for extend must be greater than current size. (current: %s, extended: %s)" % (origin_size, new_size))
	extend_size = new_size - origin_size
        lun_id = self._extra_lun_or_snap_id(volume)
        err, resp = self.client.extend_lun(lun_id, extend_size)
        if err:
	    raise exception.VolumeBackendAPIException(data=err['messages'])

    def _extract_iscsi_uids(self, connector):
        if 'initiator' not in connector:
            if self.storage_protocol == 'iSCSI':
                msg = _('Host %s has no iSCSI initiator') % connector['host']
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            else:
                return ()
        return [connector['initiator']]

    def _extract_fc_uids(self, connector):
        if 'wwnns' not in connector or 'wwpns' not in connector:
            if self.storage_protocol == 'FC':
                msg = _('Host %s has no FC initiators') % connector['host']
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            else:
                return ()
        wwnns = connector['wwnns']
        wwpns = connector['wwpns']
        wwns = [(node + port).upper() for node, port in zip(wwnns, wwpns)]
        return map(lambda wwn: re.sub(r'\S\S',
                                      lambda m: m.group(0) + ':',
                                      wwn,
                                      len(wwn) / 2 - 1),
                   wwns)

    def _categorize_initiators(self, connector):
        if self.storage_protocol == 'iSCSI':
            initiator_uids = self._extract_iscsi_uids(connector)
        elif self.storage_protocol == 'FC':
            initiator_uids = self._extract_fc_uids(connector)
        else:
            initiator_uids = []
        registered_initiators = []
        orphan_initiators = []
        new_initiator_uids = []
        registered_initiators = initiator_uids
        #for initiator_uid in initiator_uids:
        #    initiator = self.client.get_initiator_by_uid(initiator_uid)
        #    if initiator:
        #        initiator = initiator[0]
        #        if 'parentHost' in initiator and initiator['parentHost']:
        #            registered_initiators.append(initiator)
        #        else:
        #            orphan_initiators.append(initiator)
        #    else:
        #        new_initiator_uids.append(initiator_uid)
        return registered_initiators, orphan_initiators, new_initiator_uids

    def _extract_host_id(self, registered_initiators, hostname=None):
        if registered_initiators:
            return registered_initiators[0]['parentHost']['id']
        if hostname:
            host = self.client.get_host_by_name(hostname, ('id',))
            if host:
                return host[0]['id']
        return None

    def _create_initiators(self, new_initiator_uids, host_id):
        for initiator_uid in new_initiator_uids:
            err, initiator = self.client.create_initiator(initiator_uid,
                                                          host_id)
            if err:
                if err['httpStatusCode'] in (409,):
                    LOG.warning(_LW('Initiator %s had been created.'),
                                initiator_uid)
                    return
                msg = _('Failed to create initiator %s') % initiator_uid
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

    def _register_initiators(self, orphan_initiators, host_id):
        for initiator in orphan_initiators:
            err, resp = self.client.register_initiator(initiator['id'],
                                                       host_id)
            if err:
                msg = _('Failed to register initiator %(initiator)s '
                        'to %(host)s') % {'initiator': initiator['id'],
                                          'host': host_id}
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

    def _build_init_targ_map(self, mapping):
        """Function to process data from lookup service."""
        #   mapping
        #   {
        #        <San name>: {
        #            'initiator_port_wwn_list':
        #            ('200000051e55a100', '200000051e55a121'..)
        #            'target_port_wwn_list':
        #            ('100000051e55a100', '100000051e55a121'..)
        #        }
        #   }
        target_wwns = []
        init_targ_map = {}

        for san_name in mapping:
            mymap = mapping[san_name]
            for target in mymap['target_port_wwn_list']:
                if target not in target_wwns:
                    target_wwns.append(target)
            for initiator in mymap['initiator_port_wwn_list']:
                init_targ_map[initiator] = mymap['target_port_wwn_list']
        LOG.debug("target_wwns: %s", target_wwns)
        LOG.debug("init_targ_map: %s", init_targ_map)
        return target_wwns, init_targ_map

    def arrange_host(self, connector):
        # TODO kevin, single WWPNS now
        if self.storage_protocol == 'FC':
            host_id = str(connector['wwpns'][0]).upper()
        elif self.storage_protocol == 'iSCSI':
            host_id = connector['initiator']
        if host_id is None:
            msg = _('initiator/wwpns is none %s.') % connector
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
            host_id = ''

        return host_id

    def expose_lun(self, volume, lun_data, host_id):
        LOG.debug('expose_lun, v %s, lun %s, hostid %s',
            volume, lun_data, host_id)
        lun_id = lun_data['Id']
        err, resp = self.client.expose_lun(lun_id, host_id, self.storage_protocol)

        if err:
            msg = _('expose_lun error %s.') % err
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _get_driver_volume_type(self):
        if self.storage_protocol == 'iSCSI':
            return 'iscsi'
        elif self.storage_protocol == 'FC':
            return 'fibre_channel'
        else:
            return 'unknown'

    def _get_fc_zone_info(self, connector, targets):
        initiator_wwns = connector['wwpns']
        target_wwns = [item[1] for item in targets]
        mapping = self.lookup_service_instance.\
            get_device_mapping_from_network(initiator_wwns,
                                            target_wwns)
        target_wwns, init_targ_map = self._build_init_targ_map(mapping)
        return {'initiator_target_map': init_targ_map,
                'target_wwn': target_wwns}

    def get_connection_info(self, volume, connector,
                            lun_id, host_id):

        data = {'target_discovered': True,
                'target_lun': 'unknown',
                'volume_id': volume['id']}

        host_lun = self.client.get_host_lun_by_ends(host_id, lun_id, 
                                                    self.storage_protocol)
        data['target_lun'] = host_lun
        if self.storage_protocol == 'iSCSI':
            err, target_iqns, target_portals = self._do_iscsi_discovery(self.active_storage_ip)
            data['target_iqn'] = target_iqns[0]
            data['target_portal'] = target_portals[0]
            # TODO kevin, for multi-connection
            #data['target_iqns'] = target_iqns
            #data['target_portals'] = target_portals
            #data['target_luns'] = [host_lun[0]['hlu']] * len(targets)
        elif self.storage_protocol == 'FC':
            zone_info = self._build_initiator_target_map(connector)
            LOG.debug("zone_info %s", zone_info)
            data.update(zone_info)

        connection_info = {
            'driver_volume_type': self._get_driver_volume_type(),
            'data': data}
        return json.dumps(connection_info)

    def _execute_command(self, cli_type, *args, **kwargs):
        command = getattr(cli, cli_type)
        cli_conf = {
            'cli_retry_time': 2,
        }
 
        return command(cli_conf).execute(*args, **kwargs)
 
    def _execute(self, cli_type, *args, **kwargs):
        LOG.debug('Executing command type: %(type)s.', {'type': cli_type})
 
        rc, out = self._execute_command(cli_type, *args, **kwargs)
 
        if rc != 0:
            raise exception.VolumeBackendAPIException(data=ec)
 
        return rc, out
 
    def _do_iscsi_discovery(self, target_ip):
        rc, targets = self._execute(
            'ExecuteCommand',
            'iscsiadm', '-m', 'discovery',
            '-t', 'sendtargets', '-p',
            target_ip,
            run_as_root=True)
 
        target_iqns = []
        target_portals = []
        if rc != 0:
            LOG.error(_LE(
                'Can not discovery in %(target_ip)s.'), {
                    'target_ip': target_ip})
            return (False, target_iqns, target_portals)
        else:
            for target in targets.splitlines():
                words = target.split(" ")
                target_iqns.append(words[1])
                target_portals.append(words[0].split(",")[0])

            LOG.debug("target_iqns %s, portals %s",
                target_iqns, target_portals)
            return (True, target_iqns, target_portals)
 
        return (False, target_iqns, target_portals)

    def _convert_wwns_fs3000_to_openstack(self, wwns):
        """Convert a list of FS3000 WWNs to OpenStack compatible WWN strings.

        Input format is '50:01:43:80:18:6b:3f:65', output format
        is '50014380186b3f65'.

        """
        output = []
        for w in wwns:
            output.append(str(''.join(w[0:].split(':'))))
        return output

    def _build_initiator_target_map(self, connector):
        """Build the target_wwns and the initiator target map."""
        target_wwns = []
        init_targ_map = {}

        initiator_wwns = connector['wwpns']
        for initiator in initiator_wwns:
	    active_wwns = self.client.get_active_fc_wwns(str(initiator).upper())
            active_wwns =(self._convert_wwns_fs3000_to_openstack(active_wwns))
            LOG.debug("active_wwns %s", active_wwns)
            target_wwns = active_wwns
            init_targ_map[str(initiator).upper()] = target_wwns

        return {'initiator_target_map': init_targ_map,
                'target_wwn': target_wwns}

    def initialize_connection(self, volume, connector):

        flow_name = 'initialize_connection'
        volume_flow = linear_flow.Flow(flow_name)
        lun_id = self._extra_lun_or_snap_id(volume)
        err, lun_data = self.client.get_lun_by_id(lun_id)
        volume_flow.add(ArrangeHostTask(self, connector),
                        ExposeLUNTask(self, volume, lun_data),
                        GetConnectionInfoTask(self, volume, lun_data,
                                              connector))

        flow_engine = taskflow.engines.load(volume_flow,
                                            store={})
        flow_engine.run()
        return json.loads(flow_engine.storage.fetch('connection_info'))

    def hide_lun(self, volume, lun_data, host_id):
        lun_id = lun_data['Id']
        err, resp = self.client.hide_lun(lun_id,
                                         host_id,
                                         self.storage_protocol)
        if err:
            if err['errorCode'] in (0x6701020,):
                LOG.warning(_LW('LUN %(lun)s backing %(vol) had been '
                                'hidden from %(host)s.'), {
                            'lun': lun_id, 'vol': lun_data['name'],
                            'host': host_id})
                return
            msg = _('Failed to hide %(vol)s from host %(host)s '
                    ': %(msg)s.') % {'vol': lun_data['name'],
                                     'host': host_id, 'msg': resp}
            raise exception.VolumeBackendAPIException(data=msg)

    def get_fc_zone_info_for_empty_host(self, connector, host_id):
        @lockutils.synchronized('emc-vnxe-host-' + host_id,
                                "emc-vnxe-host-", True)
        def _get_fc_zone_info_in_sync():
            if self.isHostContainsLUNs(host_id):
                return {}
            else:
                targets = self.storage_targets['a'] + self.storage_targets['b']
                return self._get_fc_zone_info(connector,
                                              targets)
        return {
            'driver_volume_type': self._get_driver_volume_type(),
            'data': _get_fc_zone_info_in_sync()}

    def terminate_connection(self, volume, connector, **kwargs):
        lun_id = self._extra_lun_or_snap_id(volume)
        #kevin TODO, TBD for multi-conn
        if self.storage_protocol == 'FC':
            host_id = str(connector['wwpns'][0]).upper()
        elif self.storage_protocol == 'iSCSI':
            host_id = connector['initiator']
        host_lun = self.client.get_host_lun_by_ends(
            host_id, lun_id, self.storage_protocol)
        err, lun_data = self.client.get_lun_by_id(lun_id)
        LOG.debug("lun_id %s, host_id %s, host_lun %s, lun_data %s",
            lun_id, host_id, host_lun, lun_data)
        self.hide_lun(volume, lun_data, host_id)
        if self.storage_protocol == 'FC':
            return self._build_initiator_target_map(connector)
        else:
            return

    def isHostContainsLUNs(self, host_id):
        host = self.client.get_host_by_id(host_id, ('hostLUNs',))
        if not host:
            return False
        else:
            luns = host[0]['hostLUNs']
            return True if luns else False

    def get_volume_stats(self, refresh=False):
        if refresh:
            self.update_volume_stats()
        return self.stats

    def update_volume_stats(self):
        LOG.debug("Updating volume stats")
        # Check if thin provisioning license is installed
        #licenses = self.client.get_licenses(('id', 'isValid'))
        #thin_license = filter(lambda lic: (lic['id'] == 'VNXE_PROVISION'
        #                                   and lic['isValid'] is True),
        #                      licenses)
        #if thin_license:
        #    self.thin_enabled = True
        data = {}
        backend_name = self.configuration.safe_get('volume_backend_name')
        data['volume_backend_name'] = backend_name or 'CCFS3000Driver'
        data['storage_protocol'] = self.storage_protocol
        data['driver_version'] = VERSION
        data['vendor_name'] = "CarryCloud"

        pools = self.client.get_pools()
        if not self.is_managing_all_pools:
            pools = filter(lambda a: a['Name'] in self.storage_pools_map,
                           pools)
        else:
            self.storage_pools_map = self._build_storage_pool_id_map(pools)
        data['pools'] = map(
            lambda po: self._build_pool_stats(po), pools)
        
        self.stats = data
        #self.storage_targets = self._get_storage_targets()
        LOG.debug('Volume Stats: %s', data)
        return self.stats

    def _build_pool_stats(self, pool):
        pool_stats = {
            'pool_name': pool['Name'],
            'free_capacity_gb': int(pool['Free']) / GiB,
            'total_capacity_gb': int(pool['Size']) / GiB,
            #'provisioned_capacity_gb': pool['sizeSubscribed'] / GiB,
            'provisioned_capacity_gb': 0,
            'reserved_percentage': 0,
            #'thin_provisioning_support': self.thin_enabled,
            'thin_provisioning_support': False,
            'thick_provisioning_support': True,
            'consistencygroup_support': True,
            'max_over_subscription_ratio': self.max_over_subscription_ratio
        }
        return pool_stats

    def manage_existing_get_size(self, volume, ref):
        """Return size of volume to be managed by manage_existing."""
        if 'source-id' in ref:
            lun = self.client.get_lun_by_id(ref['source-id'])
        elif 'source-name' in ref:
            lun = self.client.get_lun_by_name(ref['source-name'])
        else:
            reason = _('Reference must contain source-id or source-name key.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=ref, reason=reason)

        # Check for existence of the lun
        if len(lun) == 0:
            reason = _('Find no lun with the specified id or name.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=ref, reason=reason)

        if lun[0]['pool']['id'] != self._get_target_storage_pool_id(volume):
            reason = _('The input lun %s is not in a manageable '
                       'pool backend.') % lun[0]['id']
            raise exception.ManageExistingInvalidReference(
                existing_ref=ref, reason=reason)
        return lun[0]['sizeTotal'] / GiB

    def manage_existing(self, volume, ref):
        """Manage an existing lun in the array."""
        if 'source-id' in ref:
            lun_id = ref['source-id']
        elif 'source-name' in ref:
            lun_id = self.client.get_lun_by_name(ref['source-name'])[0]['id']
        else:
            reason = _('Reference must contain source-id or source-name key.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=ref, reason=reason)
        self.client.modify_lun_name(lun_id, volume['name'])

        pl_dict = {'system': self.storage_serial_number,
                   'type': 'lun',
                   'id': lun_id}
        model_update = {'provider_location':
                        self._dumps_provider_location(pl_dict)}
        return model_update


@decorate_all_methods(log_enter_exit)
class CCFS3000Driver(san.SanDriver):
    """CarryCloud FS3000 Driver."""

    def __init__(self, *args, **kwargs):

        super(CCFS3000Driver, self).__init__(*args, **kwargs)
        self.helper = CCFS3000Helper(self.configuration)

    def check_for_setup_error(self):
        pass

    def create_consistencygroup(self, context, group):
	pass

    def delete_consistencygroup(self, context, group):
	pass

    def update_consistencygroup(self, context, group,
                                add_volumes=None, remove_volumes=None):
	pass

    def create_cgsnapshot(self, context, cgsnapshot):
	pass

    def delete_cgsnapshot(self, context, cgsnapshot):
	pass

    def create_volume(self, volume):
        return self.helper.create_volume(volume)

    def create_volume_from_snapshot(self, volume, snapshot):
	pass

    def create_cloned_volume(self, volume, src_vref):
	pass

    def delete_volume(self, volume):
        return self.helper.delete_volume(volume)

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        LOG.debug('Entering create_snapshot.')
        snapshotname = snapshot['display_name']+'-'+snapshot['name']
        volumename = snapshot['volume_name']
        snap_desc = snapshot['display_description']
        LOG.info(_LI('Create snapshot: %(snapshot)s: volume: %(volume)s'),
                 {'snapshot': snapshotname, 'volume': volumename})

        return self.helper.create_snapshot(
            snapshot, snapshotname, snap_desc)

    def delete_snapshot(self, snapshot):
        """Delete a snapshot."""
        LOG.info(_LI('Delete snapshot: %s'), snapshot['name'])
        return self.helper.delete_snapshot(snapshot)

    def extend_volume(self, volume, new_size):
        return self.helper.extend_volume(volume, new_size)

    @zm_utils.AddFCZone
    def initialize_connection(self, volume, connector):
        return self.helper.initialize_connection(volume, connector)

    @zm_utils.RemoveFCZone
    def terminate_connection(self, volume, connector, **kwargs):
        return self.helper.terminate_connection(volume, connector)

    def get_volume_stats(self, refresh=False):
        return self.helper.get_volume_stats(refresh)

    def update_volume_stats(self):
        return self.helper.update_volume_stats()

    def manage_existing_get_size(self, volume, existing_ref):
        """Return size of volume to be managed by manage_existing."""
	pass
        #return self.helper.manage_existing_get_size(
        #    volume, existing_ref)

    def manage_existing(self, volume, existing_ref):
	pass
        #return self.helper.manage_existing(
        #    volume, existing_ref)

    def unmanage(self, volume):
        pass
