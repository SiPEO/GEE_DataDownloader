import ee
import yaml
import time
from argparse import ArgumentParser
from utils import clipToROI, exportImageCollectionToGCS, exportImageToGCS, sentinel2CloudScore, calcCloudCoverage
from utils import GEETaskManager
import dill

from ee import image

def makeFilterList(sensor):
	filters_before = None
	filters_after = None

	def _build_filters(filter_list):
		filters = []
		for f in filter_list:
			key = f.keys()[0]
			op = f.values()[0].keys()[0]
			val = f.values()[0].values()[0]
			filters.append(getattr(ee.Filter, op)(key, val))

		return filters

	if 'filters_before' in sensor:
		filters_before = _build_filters(sensor['filters_before'])

	if 'filters_after' in sensor:
		filters_after = _build_filters(sensor['filters_after'])

	return filters_before, filters_after

def makeImageCollection(sensor, roi, start_date, end_date, modifiers=[]):
	filters_before, filters_after = makeFilterList(sensor)

	collection = ee.ImageCollection(sensor['name']) \
				.filterDate(ee.Date(start_date), ee.Date(end_date)) \
				.filterBounds(roi) \
				.map( lambda x: clipToROI(x, ee.Geometry(roi)) )

	if filters_before is not None:
		collection = collection.filter( filters_before )

	if modifiers and len(modifiers) > 0:
		for m in modifiers:
			collection = collection.map(m)

	if filters_after:
		collection = collection.filter( filters_after )

	return collection.select(sensor['bands'])

def process_datasource(task_queue, source, sensor, export_to, export_dest):
	feature_list = ee.FeatureCollection(source['features_src'])
	feature_list = feature_list.sort('system:index').toList(feature_list.size())
	n_features = feature_list.size().getInfo()

	task_list = []

	for i in range(1, n_features):
		feature_point = ee.Feature( feature_list.get(i) )

		if source['geometry'] == "point":
			feature_point = feature_point.buffer(source['size']).bounds()

		roi = feature_point.geometry()

		if isinstance(source['name'], str):
			source['name'] = [source['name']]

		if isinstance(sensor['prefix'], str):
			sensor['prefix'] = [sensor['prefix']]

		if 'prefix' in sensor:
			filename_parts = sensor['prefix'] + source['name']
		else:
			filename_parts = source['name']

		filename = "_".join(sensor['prefix']  + source['name'] + [str(i)])
		dest_path = "/".join(filename_parts)

		export_params = {
			'bucket': export_dest,
			'resolution': source['resolution'],
			'roi': roi,
			'filename': filename,
			'dest_path': dest_path
		}

		task_params = {
			'action': export_single_feature,
			'id': "_".join(filename_parts + [str(i)]), # This must be unique per task, to allow to track retries
			'kwargs': {
				'roi': roi,
				'export_params': export_params,
				'type': sensor['type'],
				'date_range': {'start_date': source['start_date'], 'end_date': source['end_date']}
			}
		}

		task_queue.add_task(task_params)

def export_single_feature(roi=None, type=None, date_range=None, export_params=None):
	modifiers = None
	if sensor['type'].lower() == "opt":
		modifiers = [sentinel2CloudScore, calcCloudCoverage]

	image_collection = makeImageCollection(sensor, roi, date_range['start_date'], date_range['end_date'], modifiers=modifiers)
	img = ee.Image(image_collection.mosaic())

	new_params = export_params.copy()
	new_params['img'] = img
	new_params['roi'] = roi

	return exportImageToGCS(**new_params)

def load_config(path):
	with open(path, 'r') as stream:
		try:
			return yaml.load(stream)
		except yaml.YAMLError as exc:
			print(exc)

if __name__ == "__main__":
	parser = ArgumentParser()
	parser.add_argument("-c", "--config", default=None, help="Config file for the download")
	args = parser.parse_args()

	assert args.config, "Please specify a config file for the download"
	config = load_config(args.config)
	print(config)

	ee.Initialize()

	task_queue = GEETaskManager(n_workers=config['max_tasks'], max_retry=config['max_retry'], wake_on_task=True, log_file=config['log_file'])

	for sensor in config['sensors']:
		for data_list in [config['data_list'][1]]:
			tasks = process_datasource(task_queue, data_list, sensor, config['export_to'], config['export_dest'])

	print("Waiting for completion...")
	task_queue.wait_till_done()
