# Main script calling to
import os
import time

# Store relative path to this script
script_dir = os.path.dirname(os.path.realpath(__file__))

# Create and store output directory
if not os.path.isdir('./output'):
    os.mkdir(os.path.join(script_dir, 'output'))
output_dir = os.path.join(script_dir, 'output')

datafrom = 'local' #'local' / 'remote' %source of data files

if (datafrom =='local'):
  input_dir = os.path.join(script_dir, 'input')
  print(input_dir)

#filespathlocal = '/Users/rpr061/Downloads/'
filespathremote = 'https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0237935/' #only applicable to GLODAP for now
socatdoi = '10.25921/yg69-jd96' # The SOCAT collection DOI info is not in ERDDAP (or can't find it)
subset = True

# Subset arguments
minlat = -45
maxlat = 55
minlon = -100
maxlon = 100
mindate = '1986-06-28T00:00:00Z'
maxdate = '1989-06-28T00:00:00Z'

datasources = ['SOCAT', 'GLODAP']
vardict = {'id':'ID', 'doi':'Source_DOI','datevec':'DATEVECTOR','unixd':'UNIXDATE',
          'lat':'LATITUDE','lon':'LONGITUDE','dep':'DEPTH',
          'temp':'TEMPERATURE','sal':'SALINITY','salf':'SALINITY_FLAG',
          'dic' : 'DIC', 'dicf':'DIC_FLAG', 'dicc':'DIC_CALCULATION',
          'alk': 'ALKALINITY', 'alkf':'ALKALINTY_FLAG', 'alkc':'ALKALINITY_CALCULATION',
          'ph': 'pH_TS','phf': 'pH_FLAG','phc': 'pH_CALCULATION',
          'fco2w':'FCO2_W','fco2wf':'FCO2W_FLAG','fco2wc': 'FCO2_CALCULATION','fco2wac':'ACCURACY_FCO2',}

starttime0 = time.time()
# Find column header line number
for ds in datasources:

    if ('SOCAT' in ds):
        source = 'SOCATv2021'
        exec(open("importSOCAT.py").read())
    elif ('GLODAP' in ds):
        source = 'GLODAPv2.2021'
        exec(open("importGLODAP.py").read())

    if 'df' not in globals():
        df = printdf
    else:
        df = df.append(printdf)

# Change format to integer for flags
allflags=[vardict['salf']]
df[vardict['salf']] = df[vardict['salf']].astype('UInt8')
df[vardict['dicf']] = df[vardict['dicf']].astype('UInt8')
df[vardict['alkf']] = df[vardict['alkf']].astype('UInt8')
df[vardict['phf']] = df[vardict['phf']].astype('UInt8')
df[vardict['fco2wf']] = df[vardict['fco2wf']].astype('UInt8')
df[vardict['fco2wc']] = df[vardict['fco2wc']].astype('UInt8')

# Keep only wanted columns
outputvars = [
     vardict['id'], vardict['doi'], 'SOURCE',
     vardict['datevec'], vardict['unixd'], vardict['lat'], vardict['lon'],
     vardict['dep'], vardict['temp'], vardict['sal'], vardict['salf'],
     vardict['fco2w'], vardict['fco2wf'], vardict['fco2wc'], vardict['fco2wac'],
     vardict['dic'], vardict['dicf'], vardict['dicc'],
     vardict['alk'], vardict['alkf'], vardict['alkc'],
     vardict['ph'], vardict['phf'], vardict['phc']]
commonvars = [i for i in outputvars if i in df.columns]
df = df[commonvars].copy()

# Export column name (PO9? CF?)
# exportvardict ={'datevec':'DATEVECTOR','unixd':'UNIXDATE','id':'ID','doi':'Source_DOI',
#                 'lat':'LATITUDE','lon':'LONGITUDE','dep':'DEPTH',
#                 'temp':'TEMPERATURE','sal':'SALINITY','salf':'SALINITY_FLAG',
#                 'fco2w':'FCO2_W','fco2wf':'FCO2W_FLAG','fco2wac':'ACCURACY_FCO2'}

# Save to csv file
df.to_csv(path_or_buf = os.path.join(output_dir, 'surfco2merged_remote.csv'), sep = ',', na_rep = '', header = True, index = False, index_label = None)

# Save to NetCDF per expocode. And one big csv
# exportNCperID.py

# Check how long the
howlongitrun = time.time()-starttime0
print('it took '+ str(howlongitrun/60) + ' minutes total')