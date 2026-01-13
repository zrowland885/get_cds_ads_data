# Modified LS2D ERA5 download code.
# Handles rejected CDS reqeuests due to queue limit and allows for chunks of multiple days (more efficient with CDS API).

import sys
import os
repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if repo_dir not in sys.path:
    sys.path.append(repo_dir)

# Python modules
import datetime
import time
import sys,os
import dill as pickle
import requests
import cdsapi

# LS2D modules
from ls2d_funcs import *

#############
# FUNCTIONS #
#############

class Tee:
    """
    Logging in file and terminal simultaneously.
    """
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)
            s.flush()

    def flush(self):
        for s in self.streams:
            s.flush()


def chunk_dates(dates, chunk_size):
    """
    Splits list of dates into lists of chunk_size.
    Prevents chunks from crossing between months (as this is not allowed by CDS API).
    """
    chunks, chunk = [], []
    for d in dates:
        if not chunk:
            chunk.append(d)
            continue

        same_month = (d.month == chunk[-1].month)
        chunk_size_reached = (len(chunk) >= chunk_size)

        if (not same_month) or chunk_size_reached:
            chunks.append(chunk)
            chunk = [d]
        else:
            chunk.append(d)

    if chunk:
        chunks.append(chunk)
    return chunks


def _download_era5_file(settings):
    """
    Download ERA5 analysis or forecasts on surface or pressure levels

    Arguments:
        settings : dictionary
            Dictionary with keys:
                chunk_dates                 : list of datetime objects with dates to download
                era5_path                   : absolute or relative path to save the NetCDF data
                case_name                   : case name used in file name of NetCDF files
                format_extension            : file extension based on format ('.nc' or '.grib')
                format                      : data format ('netcdf' or 'grib')
                write_log                   : boolean, whether to write to log files or not
                delete_expired_requests     : boolean, whether to delete pickles of expired requests
                delete_rejected_requests    : boolean, whether to delete pickles of rejected requests
                patch_netcdf                : boolean, whether to patch NetCDF files to old CDS format
                ftype                       : level/forecast/analysis switch (in: [model_an, model_fc, pressure_an, surface_an])

                lat_n, lat_s, lon_w, lon_e  : bounding box of requested area
                -OR-
                central_lat, central_lon    : requested latitude and longitude
                area_size                   : download an area of lat+/-size, lon+/-size (degrees)

    """

    # API keys
    api_key = '<INSERT YOUR CDS/ADS API KEY HERE>'
    cds_url = 'https://cds.climate.copernicus.eu/api'
    ads_url = 'https://ads.atmosphere.copernicus.eu/api'

    # Keep track of CDS downloads which are finished:
    finished = False
    state = 'new'

    # Output file name
    nc_dir, nc_file = era5_file_path(
            settings['chunk_dates'],
            settings['era5_path'], settings['case_name'], settings['ftype'], settings['format_extension'])


    # Write CDS API prints to log file (NetCDF file path/name appended with .out/.err)
    if settings['write_log']:
        # Tee allows output to go to terminal and file simultaneously

        out_file   = '{}.out'.format(nc_file[:-len(settings['format_extension'])])
        err_file   = '{}.err'.format(nc_file[:-len(settings['format_extension'])])

        stdout_file = open(out_file, "w")
        stderr_file = open(err_file, "w")

        sys.stdout = Tee(sys.__stdout__, stdout_file)
        sys.stderr = Tee(sys.__stderr__, stderr_file)


    # Bounds of domain (Yunpei mod)
    if all(k in settings for k in ('lat_n', 'lat_s', 'lon_w', 'lon_e')):
        message('Using pre-defined bounding box from settings.')
        lat_n = settings['lat_n']
        lat_s = settings['lat_s']
        lon_w = settings['lon_w']
        lon_e = settings['lon_e']
    # Fallback to original central lat/lon + size logic
    elif all(k in settings for k in ('central_lat', 'central_lon', 'area_size')):
        message('Calculating bounding box from central point and area size.')
        lat_n = settings['central_lat'] + settings['area_size']
        lat_s = settings['central_lat'] - settings['area_size']
        lon_w = settings['central_lon'] - settings['area_size']
        lon_e = settings['central_lon'] + settings['area_size']
    else:
        error('Domain boundaries are not correctly defined in settings. '
                'Please provide EITHER (lat_n, lat_s, lon_w, lon_e) '
                'OR (central_lat, central_lon, area_size).', exit=True)


    # Check if pickle with previous request is available.
    # If so, try to download NetCDF file, if not, submit new request
    pickle_file = '{}.pickle'.format(nc_file[:-len(settings['format_extension'])])

    if os.path.isfile(pickle_file):
        message('Found previous CDS/AMS request!')

        with open(pickle_file, 'rb') as f:
            cds_request = pickle.load(f)

            try:
                cds_request.update()
            except requests.exceptions.HTTPError:

                error('CDS/AMS request is no longer available online!', exit=False)
                error('To continue, delete the previous request: {}'.format(pickle_file), exit=settings['delete_expired_requests']^True)

                if settings['delete_expired_requests']:
                    os.remove(pickle_file)
                    message('Deleted expired request pickle file.')
                    state = 'deleted'

            if state != 'deleted':
                state = cds_request.reply['state']

            header('Downloading: {}-{} \t | {} \t | {}'.format(settings['chunk_dates'][0].strftime('%Y/%m/%d'), settings['chunk_dates'][-1].strftime('%d'), settings['ftype'], state))

            if state == 'completed':
                message('Request finished, downloading NetCDF/Grib file')

                cds_request.download(nc_file)
                f.close()
                os.remove(pickle_file)

                # Patch NetCDF file, to make the (+/-) identical to the old CDS
                # files, and files retrieved from MARS.
                if settings['patch_netcdf'] and (settings['format'] == 'netcdf'):
                    message('Patching NetCDF file to old CDS format') # not sure if this works with CAMS?
                    patch_netcdf(nc_file)

                finished = True

            elif state in ('queued', 'accepted', 'running'):
                message('Request not finished, current status = \"{}\"'.format(state))

            elif state == 'deleted':
                message('Deleted, skipping')

            else:
                error('Request failed, status = \"{}\"'.format(state), exit=False)
                try:
                    message('Error message = {}'.format(cds_request.reply['error'].get('message')))
                    message('Error reason = {}'.format(cds_request.reply['error'].get('reason')))
                except Exception:
                    message('Rejected for unknown reason.')
                    
                if settings['delete_rejected_requests']:
                    os.remove(pickle_file)
                    os.remove('{}.err'.format(nc_file[:-len(settings['format_extension'])]))
                    os.remove('{}.out'.format(nc_file[:-len(settings['format_extension'])]))

                    message('Deleted rejected request files.')

    else:

        header('Downloading: {}-{} \t | {} \t | {}'.format(settings['chunk_dates'][0].strftime('%Y/%m/%d'), settings['chunk_dates'][-1].strftime('%d'), settings['ftype'], state))
        message('No previous CDS/AMS request, submitting new one')

        analysis_times = ['{0:02d}:00'.format(i) for i in range(24)]
        area = [lat_n, lon_w, lat_s, lon_e]

        request = {
            'format': settings['format'],
            'time': analysis_times,
            'area': area,
            'grid': [1.0, 1.0]
        }

        if settings['ftype'] == 'pressure_an':

            # Create instance of CDS API
            server = cdsapi.Client(wait_until_complete=False, delete=False, url=cds_url, key=api_key)

            # Hardcoded pressure levels and variables
            pressure_levels = [
                # '1', '2', '3', '5', '7',
                # '10', '20', '30', '50', '70',
                # '100', '125', '150', '175',
                # '200', '225', '250',
                # '300', '350',
                # '400', '450',
                '500', '550',
                '600', '650',
                '700', '750', '775',
                '800', '825', '850', '875',
                '900', '925', '950', '975',
                '1000']

            request.update({
                'product_type': 'reanalysis',
                'pressure_level': pressure_levels,
                'year': '{0:04d}'.format(settings['chunk_dates'][0].year),
                'month': '{0:02d}'.format(settings['chunk_dates'][0].month),
                'day': [date.day for date in settings['chunk_dates']],
                'variable': [
                    'geopotential',
                    'relative_humidity',
                    'temperature'
                ]})

            cds_request = server.retrieve('reanalysis-era5-pressure-levels', request)

        elif settings['ftype'] == 'surface_an':

            # Create instance of CDS API
            server = cdsapi.Client(wait_until_complete=False, delete=False, url=cds_url, key=api_key)

            # Hardcoded variables
            request.update({
                'product_type': 'reanalysis',
                'year': '{0:04d}'.format(settings['chunk_dates'][0].year),
                'month': '{0:02d}'.format(settings['chunk_dates'][0].month),
                'day': [date.day for date in settings['chunk_dates']],
                'variable': [
                    'land_sea_mask',
                    'low_cloud_cover',
                    'toa_incident_solar_radiation'
                ]})

            cds_request = server.retrieve('reanalysis-era5-single-levels', request)

        elif settings['ftype'] == 'cams':

            # Create instance of ADS API
            server = cdsapi.Client(wait_until_complete=False, delete=False, url=ads_url, key=api_key)

            dates_str = [d.strftime("%Y-%m-%d") for d in settings['chunk_dates']]

            # Hardcoded variables
            request.update({
                'pressure_level': ['1000'],
                'date': dates_str,
                'variable': [
                    'sea_salt_aerosol_0.03-0.5um_mixing_ratio',
                    'sea_salt_aerosol_0.5-5um_mixing_ratio',
                    'sea_salt_aerosol_5-20um_mixing_ratio',
                ]})
            
            cds_request = server.retrieve('cams-global-reanalysis-eac4', request)

        # Save pickle for later processing/download
        with open(pickle_file, 'wb') as f:
            pickle.dump(cds_request, f)

    return finished


def download_era5(settings, exit_when_waiting=True):
    """
    Download all required ERA5 fields for an experiment
    between `starttime` and `endtime`

    Analysis and forecasts are downloaded as 24 hour blocks:
        Analysis: 00 UTC to (including) 23 UTC
        Forecast: 06 UTC to (including) 05 UTC next day

    Arguments:
        start : datetime object
            Start date+time of experiment
        end : datetime object
            End date+time of experiment
        lat, lon : float
            Requested center latitude and longitude
        size : float
            Download an area of lat+/-size, lon+/-size degrees
        path : string
            Directory to save files
        case : string
            Case name used in file name of NetCDF files
    """

    header('Downloading ERA5 for period: {} to {}'.format(settings['start_date'], settings['end_date']))

    if settings['format'] == 'netcdf':
        settings['format_extension'] = '.nc'
    elif settings['format'] == 'grib':
        settings['format_extension'] = '.grib'

    # Check if output directory exists, and ends with '/'
    if not os.path.isdir(settings['era5_path']):
        error('Output directory \"{}\" does not exist!'.format(settings['era5_path']))
    if settings['era5_path'][-1] != '/':
        settings['era5_path'] += '/'

    if cdsapi is None:
        error('CDS API is not installed. See: https://cds.climate.copernicus.eu/api-how-to')

    # Round date/time to full hours
    start = lower_to_hour(settings['start_date'])
    end   = lower_to_hour(settings['end_date']  )

    # Get list of required forecast and analysis times
    an_dates = get_required_analysis(start, end)
    # fc_dates = era_tools.get_required_forecast(start, end)

    # Base dictionary to pass to download function. In Python >3.3, multiprocessings Pool() can accept
    # multiple arguments. For now, keep it generic for older versions by passing all arguments inside a dict.
    download_settings = settings.copy()
    download_queue = []

    # Option to exclude download types.
    if 'blacklist_download' in settings:
        blacklist = settings['blacklist_download']
    else:
        blacklist = []

    # Loop over all required files, check if there is a local version, if not add to download queue
    def prep_dl(chunk_size, ftype):

        chunked_dates = chunk_dates(an_dates, chunk_size)

        for dates in chunked_dates:
            if ftype not in blacklist:
                era_dir, era_file = era5_file_path(
                        dates, settings['era5_path'], settings['case_name'], ftype, settings['format_extension'])
                
                if not os.path.exists(era_dir):
                    message('Creating output directory {}'.format(era_dir))
                    os.makedirs(era_dir)

                if os.path.isfile(era_file):
                    message('Found {} - {} local'.format(era_file, ftype))
                else:
                    settings_tmp = download_settings.copy()
                    settings_tmp.update({'chunk_dates': dates, 'ftype': ftype})
                    download_queue.append(settings_tmp)

    prep_dl(settings['chunk_size_sl'], 'surface_an')
    prep_dl(settings['chunk_size_pl'], 'pressure_an')
    prep_dl(settings['chunk_size_cams'], 'cams')

    finished = True
    any_dl = False
    for req in download_queue:
        if not _download_era5_file(req):
            finished = False
        else:
            any_dl = True

    if not finished:
        print(' ---------------------------------------------------------')
        print(' | One or more requests are not finished.                |')
        print(' | For CDS requests, you can monitor the progress at:    |')
        print(' | https://cds.climate.copernicus.eu/requests            |')
        print(' | For ADS, you can use:                                 |')
        print(' | https://ads.atmosphere.copernicus.eu/requests         |')
        if exit_when_waiting:
            print(' | This script will stop now, you can restart it         |')
            print(' | at any time to retry, or download the results.        |')
            print(' ---------------------------------------------------------')
            sys.exit(0)
        print(' ---------------------------------------------------------')

    return finished, any_dl

if __name__ == "__main__":

    # Chunk size is the size in days to split up the download requests.
    # Larger chunks should be more efficient, both due to the queue limit on the API and presumably the overhead of each request.
    # However, very larger chunks are deprioritised in the CDS queue, so there is probably a sweet spot.
    # Very large chunks (most likely when requesting many pressure levels) can also be rejected by the API outright due to the size limit -
    # but this limit can easily be found through trial and error by pushing the chunk size until you get an error.

    finished = False
    any_dl = True

    sleeptime = sleeptime_init = 10 # seconds
    sleeptime_increments = [10, 60, 300, 1800]
    start = time.time()
    dl_start = time.time()

    while not finished:

        header("Starting download cycle")

        settings = {
                'start_date'                : datetime.datetime(2010, 1, 1, 0),
                'end_date'                  : datetime.datetime(2011, 12, 31, 23),
                'lat_n' : 60, 'lat_s': -60,
                'lon_w': -180, 'lon_e': 180,
                'era5_path'                 : '/data',
                'case_name'                 : 'test_case',
                'write_log'                 : True,
                # 'blacklist_download'      : 'model_an', # model_an no longer in script but can be used to exclude surface_an / pressure_an / cams
                'chunk_size_sl'             : 8,
                'chunk_size_pl'             : 8,
                'chunk_size_cams'           : 8,
                'delete_expired_requests'   : True,     # this has to be done manually if False
                'delete_rejected_requests'  : True,     # this has to be done manually if False
                'format'                    : 'grib',   # netcdf/grib (grib allows for more data to be downloaded at once as the filetype doesn't have to be converted in CDS)
                'patch_netcdf'              : False,    # only applies if format is netcdf
            }

        finished, any_dl = download_era5(settings, exit_when_waiting=False)

        if not finished:

            if any_dl:
                dl_start = time.time()
                sleeptime = sleeptime_init
                i = 0
            
            else:
                dl_end = time.time()
                time_since_last_dl = dl_end - dl_start
                header(f"Time without successful DL = {time_since_last_dl:.2f} s")

                # Uncomment if you want to dynamically adjust wait times between queries (to not spam your CDS UI with loads of failed requests when downloading larger files)
                # There doesn't seem to be a reason to use this other than that - and the courtesy of not spamming the API.
                # You can play aorund with this e.g. by using a fixed increment, a multiplier or different specified wait times.
                
                # if time_since_last_dl > sleeptime + sleeptime_init:
                #     sleeptime = sleeptime_increments[i]
                #     i=i+1

            header(f"Sleeping {sleeptime:.2f} s")
            time.sleep(sleeptime)

    end = time.time()
    total_time = end - start
    header(f"Download cycle complete! Total time elapsed = {total_time:.2f} s ({(total_time/60/60):.2f} hrs)")
