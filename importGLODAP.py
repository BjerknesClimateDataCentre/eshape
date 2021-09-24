import pandas as pd
import time
import numpy as np
import PyCO2SYS as pyco2

# Read the file
headerlines = 0
separator = ','
ddtype = None
sourcefile = source + '_Merged_Master_File.csv'
doifile = 'Dataset_DOIs.txt'
expocodefile = 'EXPOCODES.txt'

# Read the files (master file, dois, expocodes)
if datafrom == 'local':
    filespath = filespathlocal
elif datafrom == 'remote':
    filespath = filespathremote
else:
    print('Missing valid file path')

# print(sourcefile + " file has " + str(headerlines) + " header lines")
start_time = time.time()
tempdf = pd.read_csv(filespath + sourcefile, sep=separator, skiprows=headerlines, dtype=ddtype, na_values=-9999,
                     error_bad_lines=False)
dois = pd.read_csv(filespath + doifile, sep='\t', header=None, names=['G2cruise', 'DOI'], dtype=ddtype,
                   error_bad_lines=False, encoding='utf_16_le')
expocodes = pd.read_csv(filespath + expocodefile, sep='\t', header=None, names=['G2cruise', 'EXPOCODE'], dtype=ddtype,
                        error_bad_lines=False)
print("--- %s seconds ---" % (time.time() - start_time))
print(" data frame has " + str(len(tempdf)) + " lines")

# Rename G2fco2 to G2fco2_20_0 (in GLODAP, it's given at 20 dg, 0dbar).
tempdf.rename(columns={'G2fco2': 'G2fco2_20_0'}, inplace=True)

# Subset for surface and reset indices
# Upper 10 m, with fCO2 measurement (measured and calculated
surfilt = (tempdf["G2depth"] <= 10.) & (~pd.isna(tempdf["G2fco2_20_0"])) & (
        (tempdf["G2fco2f"] == 2) | (tempdf["G2fco2f"] == 0))  # Flag 2 for Good, MEASURED. Flag 0 is for calculated
dfsurf = tempdf[surfilt]
dfsurf.reset_index(drop=True, inplace=True)

# Filter for only the uppermost measurement at each unique cast (if, e.g. samples at 2 and 10 m)
dfsurf['UNICAST'] = dfsurf.set_index(['G2cruise', 'G2station', 'G2cast']).index.factorize()[0] + 1
surfacemostind = []
for x in np.unique(dfsurf['UNICAST']):
    surfacemostind.append(dfsurf['G2depth'].iloc[np.where(dfsurf['UNICAST'] == x)].idxmin())
dfsurfgood = dfsurf.iloc[surfacemostind].copy()

# Change to UNIX date format and create Python datevector (to work internally)
# Some hours and minutes are NA: change to 0.0
dfsurfgood['G2hour'].iloc[np.where(dfsurfgood['G2hour'].isna())] = 0.0
dfsurfgood['G2minute'].iloc[np.where(dfsurfgood['G2minute'].isna())] = 0.0
tempdtframe = pd.DataFrame(
    {'year': dfsurfgood['G2year'], 'month': dfsurfgood['G2month'], 'day': dfsurfgood['G2day'],
     'hour': dfsurfgood['G2hour'], 'minute': dfsurfgood['G2minute']})
dfsurfgood['DATEVECTOR1'] = pd.to_datetime(tempdtframe, utc=True)
dfsurfgood[vardict['unixd']] = dfsurfgood['DATEVECTOR1'].astype('int64') // 10 ** 9
dfsurfgood[vardict['datevec']] = dfsurfgood['DATEVECTOR1'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Subset location and time
morefilt= (dfsurfgood['G2latitude']>=minlat) & (dfsurfgood['G2latitude']<=maxlat) & (
        dfsurfgood['G2longitude']>=minlon) & (dfsurfgood['G2longitude']<=maxlon) & (
        dfsurfgood['DATEVECTOR1']>=pd.to_datetime(mindate)) & (
        dfsurfgood['DATEVECTOR1']<=pd.to_datetime(maxdate))

dfsurfgood=dfsurfgood[morefilt]
dfsurf.reset_index(drop=True,inplace=True)

# Assign Expocodes and DOIs
# Transform expocode dataframes into dictionaries (easier to lookup)
expocodesdict = expocodes.set_index('G2cruise')['EXPOCODE'].to_dict()
doisdict = dois.set_index('G2cruise')['DOI'].to_dict()

for cruises in dfsurfgood['G2cruise'].unique():
    dfsurfgood.loc[dfsurfgood.G2cruise == cruises, 'EXPOCODE'] = expocodesdict[cruises]
    dfsurfgood.loc[dfsurfgood.G2cruise == cruises, 'DOI'] = doisdict[cruises].rsplit('.org/', 1)[1]

### CARBON STUFF
### Calculate fCO2 in situ (in GLODAP it's at 20 C, 0dbar)
# Define input and output conditions
kwargs = dict(
    par1_type=1,  # The first parameter supplied is of type "1", which means "alkalinity"
    par1=dfsurfgood['G2talk'],  # value of the first parameter
    par2_type=5,  # The second parameter supplied is of type "5", which means "fCO2"
    par2=dfsurfgood['G2fco2_20_0'],  # value of the second parameter
    salinity=dfsurfgood['G2salinity'],  # Salinity of the sample
    temperature=20,  # Temperature at input conditions
    temperature_out=dfsurfgood['G2temperature'],  # Temperature at output conditions
    pressure=0,  # Pressure    at input conditions
    pressure_out=dfsurfgood['G2pressure'],  # Pressure    at output conditions
    total_silicate=dfsurfgood['G2silicate'],  # Concentration of silicate  in the sample (in umol/kg)
    total_phosphate=dfsurfgood['G2phosphate'],  # Concentration of phosphate in the sample (in umol/kg)
    opt_pH_scale=1,  # pH scale at which the input pH is reported ("1" means "Total Scale")
    opt_k_carbonic=10,  # Choice of H2CO3 and HCO3- dissociation constants K1 and K2 ("10" means "Lueker 2000")
    opt_k_bisulfate=1,  # Choice of HSO4- dissociation constant KSO4 ("1" means "Dickson")
    opt_total_borate=1,  # Choice of boron:sal ("1" means "Uppstrom")
)
start_time = time.time()
print('CO2SYS Conditions have been defined!')
results = pyco2.sys(**kwargs)
print("--- %s seconds ---" % (time.time() - start_time))
dfsurfgood['G2fco2'] = results['fCO2_out']

# Assign calculation method (based on what data is available) 0: measured, 1:f(Alk,DIC), 2ALK,pH, 3 DIC ph
dfsurfgood[vardict['fco2wc']]=9 # Data not available
#dfsurfgood[vardict['dicc']]=0
#dfsurfgood[vardict['alkc']]=0
#dfsurfgood[vardict['phc']]=0

calcindex = dfsurfgood.index[dfsurfgood[vardict['fco2wc']]==0] #original QC =0 means calculated
for ind in calcindex:
    if (dfsurfgood['G2fco2f'][ind]==2):
        dfsurfgood[vardict['fco2wc']][ind] = 0
    elif (dfsurfgood['G2tco2f'][ind]==1) & (dfsurfgood['G2talkf'][ind]==1): # Check if it should be 2 instead!
        dfsurfgood[vardict['fco2wc']][ind] = 1
    elif (dfsurfgood['G2phtsinsitutpf'][ind] == 1) & (dfsurfgood['G2talkf'][ind] == 1):
        dfsurfgood[vardict['fco2wc']][ind] = 2
    elif (dfsurfgood['G2tco2f'][ind] == 1) & (dfsurfgood['G2phtsinsitutpf'][ind] == 1):
        dfsurfgood[vardict['fco2wc']][ind] = 3


# Rename columns
dfsurfgood.rename(
    columns={'EXPOCODE': vardict['id'], 'DOI': vardict['doi'],
             'G2latitude': vardict['lat'], 'G2longitude': vardict['lon'], 'G2depth': vardict['dep'],
             'G2temperature': vardict['temp'], 'G2salinity': vardict['sal'], 'G2salinityf': vardict['salf'],
             'G2tco2': vardict['dic'], 'G2tco2qc': vardict['dicf'],
             'G2talk': vardict['alk'], 'G2talkqc': vardict['alkf'],
             'G2phtsinsitutp': vardict['ph'], 'G2phtsqc': vardict['phf'],
             'G2fco2': vardict['fco2w'], 'G2fco2f': vardict['fco2wf']},
    inplace=True)

print(tempdf.shape, dfsurf.shape, dfsurfgood.shape)

# Add source (SOCAT, GLODAP, ARGO, etc...)
dfsurfgood['SOURCE'] = source

# Rename and reset indices
printdf = dfsurfgood
printdf.reset_index(drop=True, inplace=True)

print('GLODAP frame size is ')
print(printdf.shape)