import glob
import geopandas as gpd
import fiona

# :TODO Test Code with prefix
import os


def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def file_size(file_path):
    """
    this function will return the file size
    """
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)


def combine_gpkg(inputs: str, outputs: str, prefix: str) -> None:

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
