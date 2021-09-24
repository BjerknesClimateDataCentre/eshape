import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time

filespath = '/Users/rpr061/Downloads/'
filesnames = ['SOCATv2020.tsv', 'SOCATv2020_FlagE.tsv']
colheadertextshort = 'Expocode\tversion\tSource_DOI\tQC_Flag'
metacolheadertextshort = 'Expocode\tversion\tDataset'

# Find column header line number
for file in filesnames:
    print(file)

    f = open(filespath + file)
    line = ''
    headerlines = -1

    if 'FlagE' not in file:
        metaline = ''
        metaheaderlines = -1
        while metacolheadertextshort not in metaline:
            metaline = f.readline()
            metaheaderlines = metaheaderlines + 1
        print(metaheaderlines)

        endmetaheaderlines = metaheaderlines
        while '\t' in metaline:
            metaline = f.readline()
            endmetaheaderlines = endmetaheaderlines + 1

        headerlines = headerlines + endmetaheaderlines

    # IF it started reading the metaheader, it will pick up where it left
    while colheadertextshort not in line:
        line = f.readline()
        headerlines = headerlines + 1

    f.close()

    print(file + " file has " + str(headerlines) + " header lines")
    start_time = time.time()
    tempdf = pd.read_csv(filespath + file, sep='\t', skiprows=headerlines,
                         dtype={0: str, 2: str})  # add type str to columns 0 and 2
    print("--- %s seconds ---" % (time.time() - start_time))

    print(file + " data frame has " + str(len(tempdf)) + " lines")

    if "FlagE" in file:
        print('FlagE')
        tempdf['Cruise_flag'] = 'E'
        # Pointless to loop when all has the same flag
    else:
        # Read cruise flag from metadata lines in SOCAT synthesis A-D
        tempdf['Cruise_flag'] = 'X'
        metainfoAD = pd.read_csv(filespath + file, sep='\t', skiprows=metaheaderlines,
                                 nrows=endmetaheaderlines - metaheaderlines)

        counter = 0
        allexpoflags = tempdf[['Expocode', 'Cruise_flag']]
        print(allexpoflags.shape)
        start_time = time.time()

        for expocode in metainfoAD['Expocode']:
            cruiseflag = metainfoAD['QC Flag'].values[metainfoAD['Expocode'] == expocode]
            # allexpoflags['Cruise_flag'].values[allexpoflags['Expocode'] == expocode] = cruiseflag #this line is very slow
            tempdf['Cruise_flag'].values[allexpoflags['Expocode'] == expocode] = cruiseflag  # this line is very slow
            # tempdf.loc[tempdf['Cruise_flag'].values[tempdf['Expocode'] == expocode],'Cruise_flag'] = cruiseflag # This gives a KeyError
            counter = counter + 1
            print(counter)
        print("--- %s seconds for vec assigning ---" % (time.time() - start_time))

    if 'df' not in globals():
        df = tempdf
    else:
        df = df.append(tempdf)
