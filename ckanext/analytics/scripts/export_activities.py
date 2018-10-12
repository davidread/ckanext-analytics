# encoding: utf-8

'''
Exports the activity stream as a log file, for analysis purposes
'''

from __future__ import print_function
import argparse
import json

# not importing anything from ckan until after the arg parsing, to fail on bad
# args quickly.

args = None
_context = None


def get_context():
    from ckan import model
    import ckan.logic as logic
    global _context
    if not _context:
        user = logic.get_action(u'get_site_user')(
            {u'model': model, u'ignore_auth': True}, {})
        _context = {u'model': model, u'session': model.Session,
                    u'user': user[u'name']}
    return _context


def export_all():
    from ckan import model
    query = model.Session.query(model.Activity)
    export(query)


def export_all_datasets():
    import ckan.logic as logic
    dataset_names = logic.get_action(u'package_list')(get_context(), {})
    num_datasets = len(dataset_names)
    for i, dataset_name in enumerate(dataset_names):
        print(u'{}/{} {}'.format(i + 1, num_datasets, dataset_name))
        export_dataset(dataset_name)


def export_group(group_name):
    from ckan import model

    group_id = model.Group.get(group_name).id
    activity_query = model.activity._group_activity_query(group_id)
    # not doing _filter_activity_by_user as this is for admins to analyse

    export(activity_query)


def export_dataset(dataset_name):
    # import ckan.logic as logic
    from ckan import model

    # can't use package_activity_list because it doesn't contain the detail
    # or hidden activities and has a hard limit
    # activity_dicts = logic.get_action(u'package_activity_list')(
    #     context,
    #     {u'id': dataset_name, u'limit': 0})
    package_id = model.Package.get(dataset_name).id
    activity_query = model.activity._package_multi_activity_query(package_id)
    # not doing _filter_activity_by_user as this is for admins to analyse

    export(activity_query)


def export(activity_query):
    import ckan.lib.dictization.model_dictize as model_dictize
    num_activities = activity_query.count()
    if not num_activities:
        print(u'  No activities')

    global args
    with open(args.output, 'wb') as f:
        for activity_tuple in activity_query.yield_per(25):
            activity, group_name, group_title, group_is_org, package_name, \
                package_title = activity_tuple
            # from activity_show
            activity_dict = model_dictize.activity_dictize(
                activity, get_context(), include_data=True)

            # e.g.
            # {'activity_type': u'new package',
            #  'data': {u'actor': u'boston_ckan_sandbox',
            #           u'package': {u'author': None,
            #                        u'author_email': None,
            #                        u'btype': [u'geospatial'],
            #                        u'classification': u'public',
            #                        u'contact_point': u'GIS Team',
            #                        u'contact_point_email': u'EGIS_Team@boston.gov',
            #                        u'creator_user_id': u'be45c2f4-8960-4f26-83c8-6b7ecc8364f8',
            #                        u'extras': [{u'key': u'spatial',
            #                                     u'value': u'{"type":"Polygon","coordinates": [[[-71.1789,42.2313],[-71.1789,42.3944],[-70.9951,42.3944],[-70.9951,42.2313],[-71.1789,42.2313]]]}'}],
            #                        u'groups': [],
            #                        u'id': u'82992437-d3a9-4aa2-8c56-ac6aa5b51233',
            #                        u'isopen': True,
            #                        u'license_id': u'odc-pddl',
            #                        u'license_title': u'Open Data Commons Public Domain Dedication and License (PDDL)',
            #                        u'license_url': u'http://www.opendefinition.org/licenses/odc-pddl',
            #                        u'location': [],
            #                        u'maintainer': None,
            #                        u'maintainer_email': None,
            #                        u'metadata_created': u'2018-02-09T17:28:35.461622',
            #                        u'metadata_modified': u'2018-02-09T17:28:35.461631',
            #                        u'modified': u'2017-03-02',
            #                        u'name': u'trash-collection-days',
            #                        u'notes': u'City of Boston Public Works (PWD) department trash collection days.\xa0',
            #                        u'notes_translated': {u'en': u'City of Boston Public Works (PWD) department trash collection days.\xa0'},
            #                        u'num_resources': 6,
            #                        u'num_tags': 7,
            #                        u'open': u'open',
            #                        u'organization': {u'approval_status': u'approved',
            #                                          u'created': u'2016-10-05T09:08:53.173506',
            #                                          u'description': u'One location for both spatial and non-spatial datasets in a variety of forms.\r\n',
            #                                          u'id': u'f8d82bea-34ab-4328-8adf-a5b1c331b50c',
            #                                          u'image_url': u'2016-10-05-090853.162217BostonMaps.png',
            #                                          u'is_organization': True,
            #                                          u'name': u'boston-maps',
            #                                          u'revision_id': u'aafd194b-61ee-491e-ba41-7b5d70c3bac3',
            #                                          u'state': u'active',
            #                                          u'title': u'Boston Maps',
            #                                          u'type': u'organization'},
            #                        u'owner_org': u'f8d82bea-34ab-4328-8adf-a5b1c331b50c',
            #                        u'private': False,
            #                        u'publisher': u'Department of Innovation and Technology',
            #                        u'relationships_as_object': [],
            #                        u'relationships_as_subject': [],
            #                        u'released': u'2015-10-14',
            #                        u'resources': [{u'Created': u'2018-02-09T17:28:35.465904',
            #                                        u'Language': [],
            #                                        u'Media type': None,
            #                                        u'Size': None,
            #                                        u'cache_last_updated': None,
            #                                        u'cache_url': None,
            #                                        u'data_dictionary': [],
            #                                        u'datastore_active': False,
            #                                        u'description': u'',
            #                                        u'description_translated': {},
            #                                        u'format': u'GeoJSON',
            #                                        u'hash': u'',
            #                                        u'id': u'0c22f570-e634-42ad-829c-a90d58d1610d',
            #                                        u'last_modified': None,
            #                                        u'mimetype_inner': None,
            #                                        u'name': u'GeoJSON',
            #                                        u'name_translated': {},
            #                                        u'package_id': u'82992437-d3a9-4aa2-8c56-ac6aa5b51233',
            #                                        u'position': 0,
            #                                        u'resource_type': None,
            #                                        u'revision_id': u'f7a99013-b951-48ee-b347-7ce5b489b1ef',
            #                                        u'state': u'active',
            #                                        u'tracking_summary': {u'recent': 0,
            #                                                              u'total': 0},
            #                                        u'url': u'http://bostonopendata-boston.opendata.arcgis.com/datasets/b09b9dd54c1241369080c0ee48895e85_10.geojson',
            #                                        u'url_type': None}],
            #                        u'revision_id': u'f7a99013-b951-48ee-b347-7ce5b489b1ef',
            #                        u'source': [],
            #                        u'state': u'active',
            #                        u'tags': [{u'display_name': u'boston',
            #                                   u'id': u'2fa281c5-3431-4bc0-8d47-3e64c6b480ac',
            #                                   u'name': u'boston',
            #                                   u'state': u'active',
            #                                   u'vocabulary_id': None},
            #                                  {u'display_name': u'ckan',
            #                                   u'id': u'87f84b6f-e129-4750-be0d-ba0c030b7650',
            #                                   u'name': u'ckan',
            #                                   u'state': u'active',
            #                                   u'vocabulary_id': None}],
            #                        u'temporal_notes': {},
            #                        u'theme': [],
            #                        u'title': u'Trash Collection Days',
            #                        u'title_translated': {u'en': u'Trash Collection Days'},
            #                        u'tracking_summary': {u'recent': 0, u'total': 0},
            #                        u'type': u'dataset',
            #                        u'url': u'http://bostonopendata-boston.opendata.arcgis.com/datasets/b09b9dd54c1241369080c0ee48895e85_10',
            #                        u'version': None}},
            #  'id': u'2fa19192-19af-4697-8038-8eb722c66d31',
            #  'object_id': u'82992437-d3a9-4aa2-8c56-ac6aa5b51233',
            #  'revision_id': u'f7a99013-b951-48ee-b347-7ce5b489b1ef',
            #  'timestamp': '2018-02-09T17:28:35.785088',
            #  'user_id': u'be45c2f4-8960-4f26-83c8-6b7ecc8364f8'}

            # -- logging format: json --

            log_line = dict(
                activity_type=activity_dict['activity_type'],
                timestamp=activity_dict[u'timestamp'],
                dataset_name=package_name,
                dataset_title=package_title,
                )

            if u'package' in activity_dict[u'data']:
                pkg = activity_dict[u'data'][u'package']
                log_line[u'num_resources'] = pkg[u'num_resources']
                log_line[u'resources'] = pkg[u'resources']
                log_line[u'tags'] = \
                    [tag[u'display_name'] for tag in pkg[u'tags']]

            if not group_name:
                pass
            elif group_is_org:
                log_line[u'organization_name'] = group_name
                log_line[u'organization_title'] = group_title
            else:
                log_line[u'group_name'] = group_name
                log_line[u'group_title'] = group_title
            log_line = json.dumps(log_line)

            # -- logging format: text --
            if 'package' in activity_dict['data']:
                pkg = activity_dict['data']['package']
                package_log = '{package[name]} "{package[title]}"' \
                    .format(package=pkg)
            else:
                package_log = '"" ""'

            if not group_name:
                group_type = ''
            elif group_is_org:
                group_type = 'org'
            else:
                group_type = 'group'
            group_type = ''
            log_line = '{timestamp} "{package_name}" "{package_title}" "{group_name}" "{group_title}" "{group_type}" "{package_log}"'.format(
                group_name=group_name,
                group_title=group_title,
                group_type=group_type,
                package_name=package_name,
                package_title=package_title,
                package_log=package_log,
                **activity_dict)

            # -- end of logging formats --

            print(log_line)
            f.write(log_line + '\n')

    print(u'Saved: {} ({} items)'.format(args.output, num_activities))


### this is an alternative way using activity_objects


def export___(activity_objects):
    import ckan.lib.dictization.model_dictize as model_dictize

    num_activities = len(activity_objects)
    if not num_activities:
        print(u'  No activities')

    global args
    with open(args.output, 'wb') as f:

        i = -1
        for activity_object in activity_objects:
            # e.g. activity =
            # {'activity_type': u'changed package',
            #  'id': u'62107f87-7de0-4d17-9c30-90cbffc1b296',
            #  'object_id': u'7c6314f5-c70b-4911-8519-58dc39a8e340',
            #  'revision_id': u'c3e8670a-f661-40f4-9423-b011c6a3a11d',
            #  'timestamp': '2018-04-20T16:11:45.363097',
            #  'user_id': u'724273ac-a5dc-482e-add4-adaf1871f8cb'}
            i += 1
            print(u'  activity {}/{} {}'.format(
                  i + 1, num_activities, activity_object.timestamp))

            return model_dictize.activity_detail_list_dictize(
                activity_detail_objects, context)

            import pdb; pdb.set_trace()

            log_line = "{timestamp} {package['name']}".format(**activity)
            print(log_line)
            f.write(log_line)

    print(u'Saved: {}'.format(args.output))


### this is an alternative way using activity_dicts


def export__(activity_dicts):

    num_activities = len(activity_dicts)
    if not num_activities:
        print(u'  No activities')

    global args
    with open(args.output, 'wb') as f:

        i = -1
        for activity_object in activity_dicts:
            # e.g. activity =
            # {'activity_type': u'changed package',
            #  'id': u'62107f87-7de0-4d17-9c30-90cbffc1b296',
            #  'object_id': u'7c6314f5-c70b-4911-8519-58dc39a8e340',
            #  'revision_id': u'c3e8670a-f661-40f4-9423-b011c6a3a11d',
            #  'timestamp': '2018-04-20T16:11:45.363097',
            #  'user_id': u'724273ac-a5dc-482e-add4-adaf1871f8cb'}
            i += 1
            print(u'  activity {}/{} {}'.format(
                  i + 1, num_activities, activity[u'timestamp']))

            activity = package_activity_list_dictize(
                activity_objects, package.name, package.title, context,
                include_data=data_dict['include_data'])

            actor = model.Session.query(model.User).get(activity[u'user_id'])
            actor_name = actor.name if actor else activity[u'user_id']

            # add the data to the Activity, just as we do in activity_stream_item()
            data = {
                u'package': dataset,
                u'actor': actor_name,
            }
            # there are no action functions for Activity, and anyway the ORM would
            # be faster
            activity_obj = model.Session.query(model.Activity).get(activity[u'id'])
            if u'resources' in activity_obj.data.get(u'package', {}):
                print(u'    Full dataset already recorded - no action')
            else:
                activity_obj.data = data
                # print '    {} dataset {}'.format(actor_name, repr(dataset))

    print(u'Saved: {}'.format(args.output))


def package_activity_list_dictize(activity_list, package_name, package_title,
                                  context, include_data=False):
    '''all the activities are to do with one specific package, given by
    package_name/title.
    Similar to model_dictize.activity_list_dictize, but provides the package
    name and title
    '''
    import ckan.lib.dictization.model_dictize as model_dictize
    dictized_activity_list = []
    for activity in activity_list:
        dictized_activity = \
            model_dictize.activity_dictize(activity, context, include_data)
        dictized_activity['package'] = dict(
            id=dictized_activity['object_id'],
            name=package_name, title=package_title)
        dictized_activity['object_type'] = 'package'
        dictized_activity_list.append(dictized_activity)
    return dictized_activity_list


### this is an alternative way using a query

def export_dataset_(dataset_name):
    # import ckan.logic as logic
    from ckan import model

    q = model.Session.query(model.Activity) \
        .filter_by(name=dataset_name)
    export_(q)


def export_(activity_query):
    num_activities = activity_query.count()
    if not num_activities:
        print(u'  No activities')

    global args
    with open(args.output, 'wb') as f:
        for activity in query.yield_per(100):
            # from activity_show
            activity_dict = model_dictize.activity_dictize(
                activity, context, include_data=True)


### end of alternative ways


if __name__ == u'__main__':
    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument(u'-o', u'--output',
                        default='ckan-activities-{filter}.log',
                        help=u'Log output filename')
    parser.add_argument(u'-c', u'--config', help=u'CKAN config file (.ini)')
    parser.add_argument(u'--datasets', action='store_true',
                        help=u'just export activities to do with datasets')
    parser.add_argument(u'--group',
                        help=u'just export activities the named group/org')
    parser.add_argument(u'--dataset', help=u'just export this particular '
                        u'dataset - specify its name')
    args = parser.parse_args()
    assert args.config, u'You must supply a --config'

    from ckan.lib.cli import load_config
    print(u'Loading config')
    load_config(args.config)
    if args.dataset:
        args.output = args.output.format(filter=args.dataset)
        export_dataset(args.dataset)
    elif args.group:
        args.output = args.output.format(filter=args.group)
        export_group(args.group)
    elif args.datasets:
        args.output = args.output.format(filter='datasets')
        export_all_datasets()
    else:
        args.output = args.output.format(filter='all')
        export_all()
