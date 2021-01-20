import os
import sys
import click
from osmpgo.extract_osmxml import write_poly, write_osm
from osmpgo.export_osmxml import ReadOSM, ProcessOSM
from osmpgo.util import combine_gpkg
import time


@click.group()
def cli():
    pass


# noinspection SpellCheckingInspection
@cli.command('export', short_help='Export OSM.XML to gpkg')
@click.argument('inputs', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=True))
@click.argument('prefix', type=str)
@click.option('-t', '--theme', type=str, help='Individual themes in a comma separated list.')
@click.option('-f', '--feature', type=str, help='Feature type point,line,polygon')
@click.option('-w', '--workers', type=int, default=3, show_default=True, help='Number of workers')
@click.option('-m', '--mem_factor', type=int, default=4, show_default=True, help='memory factor for node filesize')
def export(inputs, output, prefix, theme, feature, workers, mem_factor):
    # noinspection SpellCheckingInspection
    """

        INPUTS is the name of the OSM.XML file

        OUTPUT is the name of the output folder

        PREFIX is the name added to the front of the export

        Theme options include:

        aerialway,aeroway,amenity,barrier,boundary,building,craft,emergency,geological,
        highway,historic,landuse,leisure,man_made,military,natural,office
        ,place,power,public_transport,railway,route,shop,sport,tourism ,waterway

        Example:

        osmgo export andorra-latest.osm.xml output andorra -w 8 -m 16

        osmgo export andorra-latest.osm.xml  output andorra -t highway -f line

        osmgo export andorra-latest.osm.xml  output andorra -t highway,building -f point,line
        """
    begin_time = time.time()
    print(f'Input XML: {inputs}')
    print(f'Output folder: {output}')
    print(f'Output prefix: {prefix}')

    print(f'Workers: {workers}')

    _themes = ['aerialway', 'aeroway', 'amenity', 'boundary', 'building', 'craft', 'emergency', 'geological',
               'highway', 'historic', 'landuse', 'leisure', 'natural', 'office', 'place', 'power', 'public_transport',
               'railway', 'route', 'shop', 'tourism', 'waterway']

    if theme is None:
        themes = _themes
    else:
        themes = []
        for each in theme.split(','):
            each = each.strip()
            if each in _themes:
                themes.append(each)
            else:
                print(f'Theme {each} is misspelled or missing')
                exit()
    print('Processing the following themes {}'.format(','.join(themes)))

    _features = ['point', 'line', 'polygon']

    if feature is None:
        features = _features
    else:
        features = []
        for each in feature.split(','):
            each = each.strip()
            if each in _features:
                features.append(each)
            else:
                print(f'Feature {each} is misspelled or missing')
                exit()
    print('Processing the following features {}'.format(','.join(features)))

    rosm = ReadOSM(inputs, themes, features, mem_factor)
    rosm.readxml()

    posm = ProcessOSM(themes, features, workers, rosm.tempf, output, prefix, rosm.block_count)
    posm.process()

    print(f'Finished exporting after {round(time.time() - begin_time, 0)} seconds.')


'''
OSMCONVERT is installed in the python env bin folder on Linux systems
Environment variable can be set or passed in as an argument as well
Windows example
set OSMCONVERT=C:/OSM/source/OSMtoGDB/Install/osmconvert.exe
'''


@cli.command('extract', short_help='Extract OSM file to OSM.XML based on shapefile')
@click.argument('inputs', type=click.Path(exists=True))
@click.argument('output', type=click.Path())
@click.option('-c', '--clip_data', type=click.Path(exists=True), help='Path to clip *.shp')
@click.option('-b', '--bbox', type=str, help='minx,miny,maxx,maxy in decimal degrees')
@click.option('-l', '--layer', type=str, help='layer name used in gdb')
@click.option('--osmconvert', envvar='OSMCONVERT', help='Path to osmconvert file')
def extract(inputs, output, osmconvert, bbox, clip_data, layer):
    """
    Extract OSM file to OSM.XML

    Example:

    osmpgo extract andorra-latest.osm.pbf andorra-extract_lc_shp.osm.xml -c andorra_hole.shp

    osmpgo extract andorra-latest.osm.pbf andorra-extract_lc_b.osm.xml -b 1.4275,42.4705,1.7201,42.6325

    osmpgo extract andorra-latest.osm.pbf andorra-extract_lc_gd.osm.xml -c andorra.gdb -l andorra_hole
    """
    begin_time = time.time()
    if os.path.exists(os.path.join(sys.prefix, 'bin/osmconvert')):
        osmconvert = os.path.join(sys.prefix, 'bin/osmconvert')
    elif os.path.exists(osmconvert):
        osmconvert = osmconvert
    else:
        print('Unable to find osmconvert program in {} or {}'.format(os.path.join(sys.prefix, 'bin/osmconvert'),
                                                                     osmconvert))
        exit()

    print(f'Path to osmconvert: {osmconvert}')
    if bbox is None:
        box = None
    else:
        box = []
        for each in bbox.split(','):
            each = each.strip()
            try:
                box.append(float(each))
            except ValueError:
                print(f'{each} not a integer or float value')
                exit()
        if box[0] >= box[2] or box[1] >= box[3]:
            print('Coordinates out of sequence')
            exit()
        print(f'Bounding box {box}')
    if clip_data is not None and bbox is not None:
        print('Clip data and BBOX selected')
        exit()

    if clip_data is not None:
        ext = ['.shp', '.gdb']
        if os.path.splitext(clip_data)[-1] not in ext:
            print('Clip data not a .shp or .gdb')
            exit()

        if os.path.splitext(clip_data)[-1] == '.gdb' and layer is None:
            print('GDB missing layer flag')
            exit()

    if clip_data is not None:
        if os.path.splitext(clip_data)[-1] == '.shp':
            poly = write_poly(clip_data, output)
            write_osm(inputs, output, osmconvert, poly=poly)
        else:
            poly = write_poly(clip_data, output, layer=layer)
            write_osm(inputs, output, osmconvert, poly=poly)
        print(poly)
    elif box is not None:
        print('bbox')
        write_osm(inputs, output, osmconvert, bbox=box)
    else:
        write_osm(inputs, output, osmconvert)

    # Tried several variations of subprocess to free up file but none of them seemed to work.
    # try:
    #     # If file exists, delete it
    #     if os.path.isfile(poly):
    #         os.remove(poly)
    #     else:  ## Show an error ##
    #         print("Error: %s file not found" % poly)
    # except OSError as e:
    #     print(e)
    #     print(f'Please delete {poly}')
    print(f'Finished extracting after {round(time.time() - begin_time, 0)} seconds.')


@cli.command('combine', short_help='Combine gpkg')
@click.argument('inputs', type=click.Path(exists=True))
@click.argument('output', type=click.Path())
@click.argument('prefix', type=str)
def combine(inputs, output, prefix):
    """
        INPUTS folder of GPKG

        OUTPUT GPKG

        PREFIX of the GPKG in INPUTS folder
    """
    begin_time = time.time()
    if os.path.exists(output):
        print(f'{output} Geopackage already exists, please delete before continuing')
        exit()

    combine_gpkg(inputs, output, prefix)
    print(f'Finished combining after {round(time.time() - begin_time, 0)} seconds.')
