import glob
import geopandas as gpd
import fiona

# :TODO Test Code with prefix
import os


def combine_gpkg(inputs: str, outputs: str, prefix: str) -> None:
    """
    Combines separate geopackages
    Args:
        inputs: path to geopackages
        outputs: output geopackage
        prefix: prefix to input geopackage

    Returns:

    """
    all_gpkg = glob.glob(os.path.join(inputs, f'{prefix}*gpkg'))

    target_gpkg = []
    if len(all_gpkg) > 0:
        for each in all_gpkg:
            if prefix in each:
                target_gpkg.append(each)
        for each in target_gpkg:
            for layername in fiona.listlayers(each):
                print(f'Processing {layername}')
                gdf = gpd.read_file(each, layer=layername)
                gdf.to_file(outputs, layer=layername, driver="GPKG")

    else:
        print('No input files found')

    # combine_gpkg('/vagrant/output','/vagrant/test.gpkg','andorra_e_l-ns3')


def timer(start, end):
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    if int(hours) > 0:
        return '{:0>2} Hour {:0>2} Minutes {:05.2f} Seconds '.format(int(hours), int(minutes), seconds)
    elif int(minutes) > 0:
        return '{:0>2} Minutes {:05.2f} Seconds'.format(int(minutes), seconds)
    else:
        return '{:05.2f} Seconds '.format(seconds)
