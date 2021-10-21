Project eShape
==========================================================================

### Task Description ###
For the E-shape project we will create a data product where all types of co2
data from many sources are merged together (e.g. SOCAT, GLODAP, ARgo floats,
EMODnetOcean). The aim is also to calculate missing paramters when others in the
co2 system are available, and add uncertainty. Unceratinties will grow when
multiple uncertain parameters are used in a calculation.

The output will be one csv file with all data (or one netcdf per expocode). When
this is created, we will put it into ERRDAP.

### About the Scripts ###
There are currently 3 pythons cripts. One main, which decides which data from
which product to merge, and two scripts which gets all these data from SOCAT
and GLODAP. We focus first on the surface data (top 10 meter or first depth).
The package CO2SYs is used to calculate carbon system parameters from other
parameters. We wish to add columns about method (measured or calculated (which
calculation)).