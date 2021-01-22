import os
import geopandas as gpd
import subprocess
from tempfile import mkstemp
from typing import List, Dict, Union, Optional, Any


def write_osm(inputs: str, output: str, osmconvert: str, poly: str = None, bbox: list = None) -> None:
    """
    Used OSMCONVERT to write OSM.XML file for use by the export package
    Args:
        inputs: OSM File
        output: Output location for OSM.XML
        osmconvert: Path to osmconvert executable
        poly: file path to poly file
        bbox: list of coordinates

    Returns:
        The return is None
    """

    try:
        # cmd = None
        if poly is not None:
            cmd = '{}  {} -B={} -o={} -t={}/osm_temp'.format(osmconvert, inputs, poly, output, os.path.dirname(output))
            # subprocess.run(cmd, shell=True)
            subprocess.check_call(cmd, stderr=subprocess.STDOUT, shell=True)
            # proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            # out, err = proc.communicate()
            # proc.wait()
            # print(f'{proc.returncode} {out} {err}')

        elif bbox is not None:
            cmd = '{} {} -b={},{},{},{} -o={} -t={}/osm_temp'.format(osmconvert, inputs, bbox[0], bbox[1], bbox[2],
                                                                     bbox[3], output, os.path.dirname(output))
            # subprocess.run(cmd, shell=True)
            # result = subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True,  shell=True)
            # print(result)
            subprocess.check_call(cmd, stderr=subprocess.STDOUT, shell=True)
        else:
            cmd = '{}  {} -o={} -t={}/osm_temp'.format(osmconvert, inputs, output, os.path.dirname(output))
            # subprocess.run(cmd, shell=True)
            # result = subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True,  shell=True)
            # print(result)
            subprocess.check_call(cmd, stderr=subprocess.STDOUT, shell=True)

    except subprocess.CalledProcessError as ex:  # error code <> 0
        print("--------error------")
        print(ex.cmd)
        print(ex.returncode)
        print(ex.output)  # contains stdout and stderr together


def write_poly(clip_data: str, output: str, layer: str = None) -> str:
    """
        Read shapefile/shape and write *.poly file for use with osmconvert

    Args:
        clip_data: path to geometry file (shape/fgdb)
        output: output file path
        layer: layer name if FGDB

    Returns:
        The return is a string to the path of the .poly flie

    """
    if os.path.splitext(clip_data)[-1] == '.shp':
        print('Processing shapefile')
        wb_poly = gpd.read_file(clip_data)
        attr = os.path.basename(clip_data).split('.')[0]
    else:
        print('Processing FileGDB')
        wb_poly = gpd.read_file(clip_data, driver="FileGDB", layer=layer)
        attr = layer

    # poly = os.path.join(os.path.dirname(output), f'{attr}.poly')
    poly = mkstemp(prefix=f'{attr}_clip_', suffix='.poly', dir=os.path.dirname(output))[1]

    with open(poly, 'w') as fp:
        fp.write(attr + "\n")
        total_ring_count = 0
        for _, f in wb_poly.iterrows():
            geom = f.geometry
            coordinates = extract_coords(geom)
            for coordinate in coordinates:

                fp.write('{0}\n'.format(total_ring_count))
                for coords in coordinate[0]['exterior_coords']:
                    x = '{:.7E}'.format(coords[0])
                    y = '{:.7E}'.format(coords[1])
                    fp.write('\t{0}\t{1}\n'.format(x, y))
                fp.write("END\n")
                total_ring_count += 1
                for rings in coordinate[0]['interior_coords']:
                    fp.write('!{0}\n'.format(total_ring_count))
                    for coords in rings:
                        x = '{:.7E}'.format(coords[0])
                        y = '{:.7E}'.format(coords[1])
                        fp.write('\t{0}\t{1}\n'.format(x, y))
                    total_ring_count += 1
                    fp.write("END\n")

        fp.write("END\n")
    return poly


def extract_coords(geom) -> list:
    """
    Extract Polygon/MultiPolygon coordinates
    Args:
        geom: shapely geometry object

    Returns:
        The return is a list of extracted coordinates

    """
    coords = []
    if geom.type == 'Polygon':
        coords.append(extract_poly_coords(geom))
    elif geom.type == 'MultiPolygon':
        coords.extend(extract_multi_poly_coords(geom))
    else:
        raise ValueError('Unhandled geometry type: ' + repr(geom.type))
    return coords


def extract_poly_coords(geom) -> List[Dict[str, Union[Optional[list], Any]]]:
    """
    Extract Polygon Coords
    Args:
        geom: shapely polygon object

    Returns:
        The return is a dictionary of internal and external coordinates

    """
    exterior_coords = None
    interior_coords = None
    if geom.type == 'Polygon':
        exterior_coords = geom.exterior.coords[:]
        interior_coords = []
        for interior in geom.interiors:
            interior_coords.append(interior.coords[:])

    return [{'exterior_coords': exterior_coords,
             'interior_coords': interior_coords}]


# noinspection SpellCheckingInspection
def extract_multi_poly_coords(geom) -> list:
    """
    Extract MultiPolygon Coords
    Args:
        geom: shapely polygon object

    Returns:
        The return is a dictionary of internal and external coordinates

    """
    coords = []
    if geom.type == 'MultiPolygon':
        for part in geom:
            coords.append(extract_poly_coords(part))  # Recursive call
    return coords
