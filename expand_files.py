import xarray as xr
import cftime
import pathlib
import subprocess
import yaml
import os.path

with open('config.yaml', 'r') as fh:
  config = yaml.safe_load(fh)

def get_files(inputclass, in_dir='.', **_):
  return [fil for fil in pathlib.Path(os.path.expandvars(in_dir)).glob(f"*{inputclass}*.nc")]
  
def move_old_files(in_file, in_dir, **_):
  in_file.rename(pathlib.Path(in_dir) / f'old_{in_file.name}')

def frequency_select(in_ds, freq, offset=0, var=None, timevar='time'):
  if var is not None:
    this_ds = in_ds[var]
  else:
    this_ds = in_ds
  if 'monthly' in freq:
    if offset:
      return this_ds.isel(**{[x for x in in_ds.coords.keys() if timevar in x][0]: slice(-12*(offset+1),-12*(offset))})
    else:
      return this_ds.isel(**{[x for x in in_ds.coords.keys() if timevar in x][0]: slice(-12,None)})
  elif 'annual' in freq:
    if offset:
      return this_ds.isel(**{[x for x in in_ds.coords.keys() if timevar in x][0]: len(getattr(in_ds, timevar))-(offset+1)})
    else:
      return this_ds.isel(**{[x for x in in_ds.coords.keys() if timevar in x][0]: len(getattr(in_ds, timevar))-1})
  else:
    raise KeyError('freq must be monthly or annual')
  
def set_fill_encoding(my_ds, my_attr, timevar='time', debug=False, latvar='lat', lonvar='lon', **_):
  for var in getattr(my_ds, my_attr):
    if 'data_vars' in my_attr and len(my_ds[var].encoding['original_shape']) == 3:
      if len(my_ds[latvar]) == my_ds[var].encoding['original_shape'][1] and len(my_ds[lonvar]) == my_ds[var].encoding['original_shape'][2]:
        my_ds[var].encoding['original_shape'] = (len(my_ds[timevar]),
                my_ds[var].encoding['original_shape'][1],
                my_ds[var].encoding['original_shape'][2])
    elif 'data_vars' in my_attr and len(my_ds[var].encoding['original_shape']) == 4:
      if len(my_ds[latvar]) == my_ds[var].encoding['original_shape'][2] and len(my_ds[lonvar]) == my_ds[var].encoding['original_shape'][3]:
        my_ds[var].encoding['original_shape'] = (len(my_ds[timevar]),
                my_ds[var].encoding['original_shape'][1],
                my_ds[var].encoding['original_shape'][2],
                my_ds[var].encoding['original_shape'][3])
    if '_FillValue' not in my_ds[var].encoding.keys():
      my_ds[var].encoding['_FillValue'] = None
    if debug:
      print(my_attr, var)
      print(my_ds[var].encoding)
  return my_ds

def extend_emissions(in_ds, freq='monthly', timevar='time', yearappend=100, fill_in=False, debug=False, latvar='lat', lonvar='lon', no_decode=False, **_):
  if fill_in:
    yearlist = [x+1 for x in range(yearappend)]
  else:
    yearlist = [yearappend]
  dslist = []
  for my_yr in yearlist:
    dsnew = frequency_select(in_ds, freq, timevar=timevar)
    if 'monthly' in freq:
      dsnew[timevar] = [x.values[()].replace(year=x.values[()].year+my_yr) for x in dsnew[timevar]]
    if 'annual' in freq:
      if no_decode:
        dsnew[timevar] = dsnew[timevar].values[()] + my_yr
      else:
        dsnew[timevar] = dsnew[timevar].values[()].replace(year=dsnew[timevar].values[()].year+my_yr)
      for var in dsnew.data_vars:
        dsnew[var] = dsnew[var].expand_dims(dim=timevar)
    dslist.append(dsnew)
  dsbig = xr.concat([in_ds]+dslist, dim=timevar)
  if debug:
    print(dsbig[timevar].encoding)
  dsbig[timevar].encoding = in_ds[timevar].encoding
  dsbig[timevar].encoding['original_shape'] = (len(dsbig[timevar]),)
  if debug:
    print(dsbig[timevar].encoding)
  set_fill_encoding(dsbig, 'data_vars', latvar=latvar, lonvar=lonvar, timevar=timevar)
  set_fill_encoding(dsbig, 'coords', latvar=latvar, lonvar=lonvar, timevar=timevar)
  return dsbig

def confirm_extension(in_ds, freq='monthly', moredebug=False, timevar='time', **_):
  for var in in_ds.data_vars:
    if moredebug:
      print(var, frequency_select(in_ds, freq, offset=1, timevar=timevar))
    assert (frequency_select(in_ds, freq, var=var, timevar=timevar).values - frequency_select(in_ds, freq, offset=1, var=var, timevar=timevar)).max() == 0
    assert (frequency_select(in_ds, freq, var=var, timevar=timevar).values - frequency_select(in_ds, freq, offset=1, var=var, timevar=timevar)).min() == 0

def check_time_units_in_file(in_file, timevar='time', debug=False, **_):
  ps = subprocess.run(['ncdump', '-h', in_file], check=True, capture_output=True)
  tus = subprocess.run(['grep', f'{timevar}:units'], input=ps.stdout, capture_output=True)
  in_quotes = subprocess.run(['awk', '-F"', '{ print $2 }'], input=tus.stdout, capture_output=True)
  t_units = in_quotes.stdout.decode('utf-8').strip()
  if '00:00:00' not in t_units:
    if '0:0:0' in t_units:
      if debug:
        print(f'adjusting h:m:s in {timevar}:units to 00:00:00')
      ncatted = subprocess.run(['ncatted', '-O', '-a', f'units,{timevar},o,c, {t_units.replace("0:0:0","00:00:00")}', in_file], check=True, capture_output=True)
    else:
      if debug:
        print(f'appending 00:00:00 to {timevar}:units')
      ncatted = subprocess.run(['ncatted', '-O', '-a', f'units,{timevar},a,c, 00:00:00', in_file], check=True, capture_output=True)

def main(oldfile, my_kw):
  if 'no_decode' in my_kw.keys():
    if my_kw['no_decode']:
      old_ds = xr.open_dataset(oldfile, decode_times=False)
    else:
      old_ds = xr.open_dataset(oldfile)
  else:
    old_ds = xr.open_dataset(oldfile)
  ds = extend_emissions(old_ds, **my_kw)
  if my_kw['debug']:
    confirm_extension(ds, **my_kw)
  try:
    move_old_files(oldfile, **my_kw)
  except:
    print('renaming failed, not writing new file')
    return
  ds.to_netcdf(oldfile)
  check_time_units_in_file(oldfile, **my_kw)

if __name__=='__main__':
  for inputclass, kw in config.items():
    if 'w_data' in inputclass:
      print(inputclass)
      for my_file in get_files(inputclass, **kw):
        main(my_file, kw)
