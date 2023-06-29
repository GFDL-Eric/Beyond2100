ncrcat --rec_apn -d time,-1 landuse.nc landuse.nc
ncap2 -O -s 'time(-1)=613' landuse.nc landuse.nc
