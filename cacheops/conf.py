# -*- coding: utf-8 -*-
from copy import deepcopy
import warnings
import six
import redis
from django_redis import get_redis_connection
from funcy import memoize, decorator, identity, is_tuple, merge

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


ALL_OPS = ('get', 'fetch', 'count', 'exists')


profile_defaults = {
    'ops': (),
    'local_get': False,
    'db_agnostic': True,
}
# NOTE: this is a compatibility for old style config,
# TODO: remove in cacheops 3.0
profiles = {
    'just_enable': {},
    'all': {'ops': ALL_OPS},
    'get': {'ops': ('get',)},
    'count': {'ops': ('count',)},
}
for key in profiles:
    profiles[key] = dict(profile_defaults, **profiles[key])


def get_redis_client(write = True):
    if not hasattr(settings, 'CACHEOPS_CACHE_ALIAS'):
        raise ImproperlyConfigured("You must specify CACHEOPS_CACHE_ALIAS setting to use cacheops")

    cache_alias = settings.CACHEOPS_CACHE_ALIAS
    if not isinstance(cache_alias, basestring) or len(cache_alias) == 0:
        raise ImproperlyConfigured("You must specify CACHEOPS_CACHE_ALIAS setting to use cacheops")

    return get_redis_connection(alias = cache_alias, write = write)


LRU = getattr(settings, 'CACHEOPS_LRU', False)
DEGRADE_ON_FAILURE = getattr(settings, 'CACHEOPS_DEGRADE_ON_FAILURE', False)


# Support DEGRADE_ON_FAILURE
if DEGRADE_ON_FAILURE:
    @decorator
    def handle_connection_failure(call):
        try:
            return call()
        except redis.ConnectionError as e:
            warnings.warn("The cacheops cache is unreachable! Error: %s" % e, RuntimeWarning)
        except redis.TimeoutError as e:
            warnings.warn("The cacheops cache timed out! Error: %s" % e, RuntimeWarning)
else:
    handle_connection_failure = identity


@memoize
def prepare_profiles():
    """
    Prepares a dict 'app.model' -> profile, for use in model_profile()
    """
    # NOTE: this is a compatibility for old style config,
    # TODO: remove in cacheops 3.0
    if hasattr(settings, 'CACHEOPS_PROFILES'):
        profiles.update(settings.CACHEOPS_PROFILES)

    if hasattr(settings, 'CACHEOPS_DEFAULTS'):
        profile_defaults.update(settings.CACHEOPS_DEFAULTS)

    model_profiles = {}
    ops = getattr(settings, 'CACHEOPS', {})
    for app_model, profile in ops.items():
        if profile is None:
            model_profiles[app_model] = None
            continue

        # NOTE: this is a compatibility for old style config,
        # TODO: remove in cacheops 3.0
        if is_tuple(profile):
            profile_name, timeout = profile[:2]

            try:
                model_profiles[app_model] = mp = deepcopy(profiles[profile_name])
            except KeyError:
                raise ImproperlyConfigured('Unknown cacheops profile "%s"' % profile_name)

            if len(profile) > 2:
                mp.update(profile[2])
            mp['timeout'] = timeout
            mp['ops'] = set(mp['ops'])
        else:
            model_profiles[app_model] = mp = merge(profile_defaults, profile)
            if mp['ops'] == 'all':
                mp['ops'] = ALL_OPS
            # People will do that anyway :)
            if isinstance(mp['ops'], six.string_types):
                mp['ops'] = [mp['ops']]
            mp['ops'] = set(mp['ops'])

        if 'timeout' not in mp:
            raise ImproperlyConfigured(
                'You must specify "timeout" option in "%s" CACHEOPS profile' % app_model)

    return model_profiles

@memoize
def model_profile(model):
    """
    Returns cacheops profile for a model
    """
    model_profiles = prepare_profiles()

    app = model._meta.app_label
    # module_name is fallback for Django 1.5-
    model_name = getattr(model._meta, 'model_name', None) or model._meta.module_name
    app_model = '%s.%s' % (app, model_name)
    for guess in (app_model, '%s.*' % app, '*.*'):
        if guess in model_profiles:
            return model_profiles[guess]
    else:
        return None
