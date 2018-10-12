=============
ckanext-analytics
=============

For exporting a CKAN's activity stream data so that it can be analysed in tools like Kibana, Sumo Logic etc

Motivation: https://github.com/ckan/ideas-and-roadmap/issues/209


------------
export_activities.py
------------
Export Activities in JSON lines format.

Usage (ensure your ckan virtualenv is activated)::

    python ../ckanext-analytics/ckanext/analytics/scripts/export_activities.py -c $CKAN_INI --dataset trash-collection-days
    python ../ckanext-analytics/ckanext/analytics/scripts/export_activities.py -c $CKAN_INI --group boston-maps
