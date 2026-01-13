
# Python modules
import datetime
import sys,os
import xarray as xr
import shutil


#--------------#
# LS2D Modules #
#--------------#

# from era_tools
def lower_to_hour(time):
    time_out = datetime.datetime(time.year, time.month, time.day, time.hour)
    return time_out

# from era_tools
def get_required_analysis(start, end, freq=1):

    # One day datetime offset
    one_day = datetime.timedelta(days=1)

    # Analysis start at 00 UTC, so first analysis = start day
    first_analysis = datetime.datetime(start.year, start.month, start.day)

    # If end time is after (24-freq) UTC, include next day for the analysis files.
    # `freq` is typically 1 hour for ERA5, and 3 hours for CAMS.
    hour = end.hour + end.minute / 60.
    if hour > 24 - freq:
        last_analysis = datetime.datetime(end.year, end.month, end.day) + one_day
    else:
        last_analysis = datetime.datetime(end.year, end.month, end.day)

    # Create list of datetime objects:
    dates = [first_analysis + i*one_day for i in range((last_analysis-first_analysis).days + 1)]

    return dates

# from era_tools
def get_required_forecast(start, end):

    # One day datetime offset
    one_day = datetime.timedelta(days=1)

    # Forecast runs through midnight, so last analysis = last day
    last_forecast = datetime.datetime(end.year, end.month, end.day)

    # If start time is before 06 UTC, include previous day for the forecast files
    if start.hour > 6:
        first_forecast = datetime.datetime(start.year, start.month, start.day)
    else:
        first_forecast = datetime.datetime(start.year, start.month, start.day) - one_day

    # Create list of datetime objects:
    dates = [first_forecast + i*one_day for i in range((last_forecast-first_forecast).days + 1)]

    return dates

# from era_tools
def lower_to_hour(time):
    time_out = datetime.datetime(time.year, time.month, time.day, time.hour)
    if time.minute != 0 or time.second != 0:
        warning('Changed date/time from {} to {}'.format(time, time_out))
    return time_out


# from era_tools, modified
def era5_file_path(dates, path, case, ftype, format_ext, return_dir=True):
    """
    Return saving path of files in format `path/yyyy/mm/dd/type.nc`
    """

    s_date = dates[0]
    e_date = dates[-1]

    era_dir = f"{path}/{case}/{s_date.year:04d}/{s_date.month:02d}/{s_date.day:02d}_{e_date.day:02d}"

    # era_file = "{0}/{1}.{2}".format(era_dir, ftype, format_stub)
    era_file = f"{era_dir}/{ftype}{format_ext}"

    if return_dir:
        return era_dir, era_file
    else:
        return era_file

# from patch_cds_ads
def patch_netcdf(nc_file_path):
    """
    With the introduction of the new Copernicus Data Store (CDS) in September 2024,
    the NetCDF format for ERA5 data has undergone some changes. As a result, these
    NetCDF files differ from the previous CDS format and from files currently retrieved from MARS.
    This function patches the new NetCDF files, to make them +/- identical to the old format.

    NOTE: the patched files are not 100% identical to the old format, just
    identical enough for (LS)2D to read and parse them.
    """

    # Backup old file, and remove original.
    backup_file_path = f'{nc_file_path}.unpatched'
    shutil.copyfile(nc_file_path, backup_file_path)
    os.remove(nc_file_path)

    # Edit with Xarray. Read the copied file, so that we can overwrite the original one.
    ds = xr.open_dataset(backup_file_path, decode_times=False)

    # Check if we actually have a new NetCDF file.
    if 'valid_time' not in ds.dims:
        error('Provided NetCDF is not a new (>09/2024) CDS file!')

    # Drop `expver`; we need to save this file in classic NetCDF4 format, which
    # does not support variable length strings.
    if 'expver' in ds.variables:
        ds = ds.drop('expver')

    file_name = os.path.basename(nc_file_path)

    if file_name in ['model_an.nc', 'eac4_ml.nc', 'egg4_ml.nc']:
        new_ds = ds.rename({
                'model_level': 'level',
                'valid_time': 'time'})

    elif file_name == 'pressure_an.nc':
        new_ds = ds.rename({
                'pressure_level': 'level',
                'valid_time': 'time'})

        # Yeah, somehow they thought it was a good idea to reverse the pressure levels......
        new_ds = new_ds.reindex(level=new_ds.level[::-1])

    elif file_name in ['surface_an.nc', 'surface_fc.nc', 'eac4_sfc.nc', 'egg4_sfc.nc', 'egg4_sl.nc']:
        new_ds = ds.rename({
                'valid_time': 'time'})

    else:
        error('Not a valid file type!')

    # Fix time. Old format was `hours since 1900-01-01 00:00:00.0`, new format `seconds since 1970-01-01`.
    old_ref = datetime.datetime(year=1900, month=1, day=1)
    new_ref = datetime.datetime(year=1970, month=1, day=1)

    new_ds['time'] = [(new_ref + datetime.timedelta(seconds=int(s)) - old_ref).total_seconds() / 3600. for s in new_ds.time.values]
    new_ds['time'].attrs['units'] = 'hours since 1900-01-01 00:00:00.0'

    # Remove Grib attributes.
    for v in new_ds.variables:
        da = new_ds[v]

        to_rm = []
        for attr in da.attrs:
            if 'GRIB' in attr:
                to_rm.append(attr)

        for attr in to_rm:
            del da.attrs[attr]

    # Remove dimensions of size 1.
    new_ds = new_ds.squeeze()

    # Overwrite old file.
    new_ds.to_netcdf(nc_file_path, format='NETCDF4_CLASSIC')

    return new_ds   # Just for debugging...


# from messages
_opts = {
   'blue'   : '\033[94m',
   'green'  : '\033[92m',
   'purple' : '\033[95m',
   'red'    : '\033[91m',
   'bf'     : '\033[1m',
   'ul'     : '\033[4m',
   'end'    : '\033[0m'
}

def header(message, time=True):
    """
    Format of print statements indicating new main routine
    """
    if time:
        now = datetime.datetime.now()
        print('{}{}{}{} {}[{}]{}'.format(_opts['blue'], _opts['bf'], message, _opts['end'], _opts['green'], now.strftime('%d-%m: %H:%M'), _opts['end']))
    else:
        print('{}{}{}{}'.format(_opts['blue'], _opts['bf'], message, _opts['end']))

def message(message):
    """
    Format of print statements
    """
    print(' - {}'.format(message))

def warning(message):
    """
    Format of print warnings
    """
    print('{}{}WARNING:{} {}'.format(_opts['purple'], _opts['bf'], _opts['end'], message))

def error(message, exit=True):
    """
    Format of print errors
    """
    print('{}{}ERROR:{} {}'.format(_opts['red'], _opts['bf'], _opts['end'], message))
    if exit:
        sys.exit()


#--------#
# Custom #
#--------#

def chunk_dates(dates, chunk_size):
    """ Splits list of dates into lists of chunk_size. Prevents chunks from crossing between months. """
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
