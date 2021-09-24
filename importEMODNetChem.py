
dataf = pd.read_csv('emodnetworkcopy3.txt',sep='\t', skiprows=96)

dataf2=dataf.iloc[:,[58,61,62,63,64,65,66,91,92,93,94,95,96,107]].copy()
dataf2.to_csv('emodnet_med_carbdata.csv',index=False)

dataf3=dataf.iloc[:,list(range(59))].copy()
dataf3.to_csv('emodnet_med_metadata.csv',index=False)
