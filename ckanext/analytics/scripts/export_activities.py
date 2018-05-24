# encoding: utf-8

'''
Exports the activity stream as a log file, for analysis purposes
'''

from __future__ import print_function
import argparse

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


def export_dataset(dataset_name):
    import ckan.logic as logic
    from ckan import model

def export(query):
    context = get_context()
    # 'hidden' activity is that by site_user, such as harvests, which are
    # not shown in the activity stream because they can be too numerous.
    # However thes do have Activity objects, and if a hidden Activity is
    # followed be a non-hidden one and you look at the changes of that
    # non-hidden Activity, then it does a diff with the hidden one (rather than
    # the most recent non-hidden one), so it is important to store the
    # package_dict in hidden Activity objects.
    context[u'include_hidden_activity'] = True
    num_activities = query.count()
    if not num_activities:
        print(u'  No activities')

    with open(args.output) as f:

        context[u'for_view'] = False
        i = -1
        for activity in query.yield_per(100):
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

            # from activity_show
            activity_dict = model_dictize.activity_dictize(
                activity, context, include_data=True)
        

            # get the dataset as it was at this revision
            context[u'revision_id'] = activity[u'revision_id']
            # call package_show just as we do in package.py:activity_stream_item(),
            # only with a revision_id
            dataset = logic.get_action(u'package_show')(
                context,
                {u'id': activity[u'object_id'], u'include_tracking': False})
            # get rid of revision_timestamp, which wouldn't be there if saved by
            # during activity_stream_item() - something to do with not specifying
            # revision_id.
            if u'revision_timestamp' in (dataset.get(u'organization') or {}):
                del dataset[u'organization'][u'revision_timestamp']
            for res in dataset[u'resources']:
                if u'revision_timestamp' in res:
                    del res[u'revision_timestamp']

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


if __name__ == u'__main__':
    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument(u'-o', u'--output', default='ckan-activities.log',
                        help=u'Log output filename')
    parser.add_argument(u'-c', u'--config', help=u'CKAN config file (.ini)')
    parser.add_argument(u'--datasets', action='store_true',
                        help=u'just export activities to do with datasets')
    parser.add_argument(u'--dataset', help=u'just export this particular '
                        u'dataset - specify its name')
    global args
    args = parser.parse_args()
    assert args.config, u'You must supply a --config'
    from ckan.lib.cli import load_config
    print(u'Loading config')
    load_config(args.config)
    if args.dataset:
        export_dataset(args.dataset)
    elif args.datasets:
        export_all_datasets()
    else:
        export_all()
