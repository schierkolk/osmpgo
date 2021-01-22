# OSMPGO
Installation
CD into the deliver folder
	There should be a environmental.yml and the osmpgo folder
Create conda environment
	conda env create
	conda activate osmpgo
Install OSMPGO python package
	pip install . -v
	Be sure to include the .
	Successfully installed osmpgo-0.1
	*Osmconvert is automatically compiled during the install on Linux systems
	By using the -v you should see:
	    running install
        Linux
        cc osmpgo/osmconvert/osmconvert.c -lz -O3 -o /home/vagrant/miniconda3/envs/osmpgo/bin/osmconvert
Test Install
    type in terminal
    osmconvert
    osmpgo


Example Usage
    Extract
        osmpgo extract andorra-latest.osm.pbf andorra-extract_lc_shp.osm.xml -c andorra_hole.shp
        osmpgo extract andorra-latest.osm.pbf andorra-extract_lc_b.osm.xml -b 1.4275,42.4705,1.7201,42.6325
        osmpgo extract andorra-latest.osm.pbf andorra-extract_lc_gd.osm.xml -c andorra.gdb -l andorra_hole

