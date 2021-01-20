from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
import os
import time
from shutil import rmtree
import tempfile
import sys
import pickle
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString


mem_factor = 4


def get_element_name(line):
    """
    Gets the value of the named attribute from the string
    Gets the XML element name from the string passed in.  End of element tag is /element
    """
    s = line.find('<')
    e = line.find(' ', s)
    el = line[s + 1:e]
    if el[0:1] == '/':
        el = el[0:len(el) - 1]
    return el


def get_attribute_value(name, line):
    """
    Gets the value of the named attribute from the string

    """
    sa = line.find(' ' + name + '="') + len(name) + 3
    ea = line.find('"', sa)
    attr = line[sa:ea]
    return attr


def get_node_details(line):
    """
    Extract node id, lon, and lat from a line of xml text
    :param line: str
    :return: str, str, str
    """
    nid = get_attribute_value('id', line)
    nx = get_attribute_value('lon', line)
    ny = get_attribute_value('lat', line)
    return nid, nx, ny


def return_id(line):
    """
    Get the id attribute from a line of xml text used for ways and its segments; id is only attribute needed

    :param line: str
    :return: str
    """

    return get_attribute_value('id', line)


def get_tag_details(line):
    """
    Get key and value from tag
    :param line: str
    :return: str, str
    """

    k = get_attribute_value('k', line)[:29]
    v = get_attribute_value('v', line)[:254]

    return k, v


def read_themes(themes):
    """
    Uses CSV file and theme parameter to create custom dictionary base on users input
    :param themes: list
    :return: dict
    """

    std_flds_all = {}
    config_file = os.path.join(os.path.dirname(__file__), 'categories_and_fields.csv')
    with open(config_file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            fields = []
            if len(row) > 0:
                # categories.append(row[0])
                for fld in row:
                    if len(fld) > 0:
                        if fld == 'from':  # 'from' and 'to' fields need to have an underscore for some reason
                            fld = 'from_'
                        if fld == 'to':
                            fld = 'to_'
                        fields.append(fld)
                std_flds_all[row[0]] = fields

    # Trim std_flds to only themes passed in by user
    std_flds = {}
    for key in themes:
        if key in std_flds_all:
            std_flds[key] = std_flds_all[key]

    return std_flds


def read_osm(osm, themes, features):
    """
    Reads OSM XML line by line and build nodes, points and ways pickle files by theme to enalble
    parallel processing in follow-on step
    :param osm:
    :param themes:
    :param features:
    :return:
    """
    pointb = False
    lineb = False
    polygonb = False
    if 'point' in features:
        pointb = True

    if 'line' in features:
        lineb = True

    if 'polygon' in features:
        polygonb = True

    tempf = tempfile.mkdtemp(dir=os.path.dirname(__file__))

    std_flds = read_themes(themes)
    # print(std_flds['building'])
    categories = list(std_flds.keys())
    print(f'Processing: {",".join(categories)}')

    # FYI: Structure of node, way, and tag objects
    # node_details = ('ID', 'x', 'y')
    # way = ('id', 'nodes', 'values', 'category')
    # tag_details = ('key', 'value')

    # Create initial temp files to keep track of nodes and ways
    block_count = 1
    # node_file = bz2.BZ2File(join(tempf, 'nodeblock_{0}.dat'.format(str(block_count))), 'w')
    # unbuilt_ways = bz2.BZ2File(join(tempf, 'unbuilt_ways.dat'), 'w')  # stores way ids and ids of component nodes

    open_files = {}
    for key in std_flds.keys():
        if lineb or polygonb:
            open_files[f'{key}_way'] = open(os.path.join(tempf, f'{key}_way.pkl'), 'wb')
        if pointb:
            open_files[f'{key}_point'] = open(os.path.join(tempf, f'{key}_point.pkl'), 'wb')

    node_file = open(os.path.join(tempf, f'nodeblock_{block_count}.pkl'), 'wb')

    has_valid_tags = False  # Will be set to true when first valid tag is found

    # Create basic objects to keep track of features
    node_count = 0
    way_count = 0
    point_feature_count = 0
    line_count = 0
    type_code = -1  # -1 is not yet set, 1 is a node, 2 is a way
    feature_tags = []
    block_size = mem_factor * 1000000  # Size of each temp file for storing nodes

    xml_file = open(osm, 'rb')
    line_count = 0
    for xml_line in xml_file:
        # print(xml_line)
        try:
            # Source should be in utf-8, but encoding causes problems sometimes
            # u_line = unicode(xml_line, 'utf-8', 'replace')
            u_line = xml_line.decode('utf-8')
            element_name = get_element_name(u_line)
            # print(element_name)
            line_count += 1
            # print(element_name)
        except Exception as e:
            print(f'\tError reading line in file: {xml_line}')
            continue

        if element_name == 'node':

            try:
                type_code = -1  # Still -1 until we know node is valid
                feature_tags = []
                node_details = get_node_details(u_line)
                has_valid_tags = False

                # Make sure node coordinates are valid geographically
                if -180 <= float(node_details[1]) <= 180 and -90 <= float(node_details[2]) <= 90:
                    type_code = 1

                    # Start a new node block if size limit reached
                    if node_count > block_count * block_size:
                        node_file.close()
                        block_count += 1
                        # node_file = bz2.BZ2File(join(tempf, 'nodeblock_{0}.dat'.format(block_count)), 'w')
                        node_file = open(os.path.join(tempf, f'nodeblock_{block_count}.pkl'), 'wb')
                    info = '{0}:{1}:{2}'.format(str(node_details[0]), str(node_details[1]), str(node_details[2]))
                    # (info)
                    # node_file.write(info)
                    pickle.dump(info, node_file)

                    node_count += 1
                    if node_count > 0 and node_count % 100000 == 0:
                        print('\tCounting nodes: {0:,}'.format(node_count))

            except Exception as e:
                print(e)
                print('\tError reading node!')
                continue

        elif element_name == 'way':
            type_code = 2
            has_valid_tags = False

            if way_count > 0 and way_count % 10000 == 0:
                print('\tCounting ways: {0:,}'.format(way_count))

            way = (return_id(u_line), '')
            # way_text_string = '{0}#'.format(str(way[0]))
            # way_text_string = ''
            way_ref_list = []
            feature_tags = []

        # nd element will only be found inside a way, save it to its way string
        elif element_name == 'nd':
            # way_text_string += \
            #    str(get_attribute_value('ref', u_line)) + ':'
            way_ref_list.append(get_attribute_value('ref', u_line))
        # tag elements can be found inside nodes or ways
        elif element_name == 'tag':

            # Get name and value of the tag
            tag_details = get_tag_details(u_line)

            # If tag is not blank, add it to feature tags list
            if tag_details[1] != '':

                # 'from' and 'to' tags need an underscore for some reason
                if tag_details[0] == 'from':
                    tag_details = ('from_', tag_details[1])
                if tag_details[0] == 'to':
                    tag_details = ('to_', tag_details[1])

                feature_tags.append((tag_details[0].replace(':', '_'), tag_details[1].replace(',', ' ')))
                has_valid_tags = True

        # At a /node element (i.e. a node with tags), create a point and insert it into the points feature class
        elif '/node' in element_name and has_valid_tags and type_code == 1:

            if pointb:
                # Node details were saved when opening <node> element was read

                try:
                    #                 # Loop through the node's tags
                    for tag_kv in feature_tags:
                        # values = [node_id, node_point]
                        node_cursor_key = tag_kv[0]

                        # If tag matches a feature class, find that cursor
                        if node_cursor_key in categories:
                            values = {}
                            values['node_id'] = str(node_details[0])
                            values['geometry'] = Point(float(node_details[1]), float(node_details[2]))
                            # node_cat = node_cursor_key
                            node_fieldnames = std_flds[node_cursor_key]
                            # Loop through tags again, inserting into field values
                            for the_tag in feature_tags:
                                the_key = the_tag[0]
                                if the_key in node_fieldnames:
                                    value = str(the_tag[1])
                                    values[the_key] = value

                            pickle.dump(values, open_files[f'{node_cursor_key}_point'])
                            point_feature_count += 1
                except Exception as e:
                    print('\tError processing node with ID: {0}'.format(node_id))

            has_valid_tags = False  # Reset valid tags flag

        # No way will be only one line in the XML, we will have read through its component <nd ref> and <tag> elements
        elif '/way' in element_name and has_valid_tags:

            # Done with way, now let's load its attributes (shape comes later)
            # Need to go back and come up with a better place to put this
            if len(feature_tags) > 0 and (lineb or polygonb):
                way_id = str(way[0])  # From first line of way XML
                try:
                    # Loop through the way's tags
                    for tag_kv in feature_tags:
                        key = tag_kv[0]

                        # If tag matches a feature class, we will use this way
                        if key in categories:
                            values = {}
                            values['attrib'] = {}
                            way_cat = key
                            way_fieldnames = std_flds[way_cat]
                            # Loop through tags again, inserting into field values
                            for the_tag in feature_tags:
                                the_key = the_tag[0]
                                if the_key in way_fieldnames:
                                    value = str(the_tag[1])
                                    values['attrib'][the_key] = value

                            values['ref'] = way_ref_list  # Used as an index to align points in the correct sequence
                            values['ref_remaing'] = way_ref_list  # Used to keep track of nodes in geometry creation
                            values['way_cat'] = way_cat
                            values['way_id'] = way_id
                            values['coords'] = {}  # Place Holder for Ref Coords

                            # Dump way values to pickle theme
                            pickle.dump(values, open_files[f'{key}_way'])
                            way_count += 1

                except Exception as e:
                    print(e)
                    print('\tError reading way with id: {0}'.format(way_id))

            has_valid_tags = False  # Reset valid tags flag

    # Close xml_file if necessary
    if str(type(xml_file)) == "<type 'file'>":
        xml_file.close()

    print(f'\tCount: {0:,} nodes, {way_count:,} ways'.format(node_count, way_count))
    print(f'\tPoint features produced: {point_feature_count:,}')

# Close files that were written to
    node_file.close()
    # unbuilt_ways.close()

    for key in open_files:
        open_files[key].close()

    return tempf, block_count


def loadall(filename):
    with open(filename, "rb") as f:
        while True:
            try:
                yield pickle.load(f)
            except EOFError:
                break


def determine_force_way_to_line(cat, atts):
    """
    Part of the legacy code
    """

    if cat in ['barrier', 'boundary', 'highway', 'public_transport', 'railway', 'route']:
        force_to_line = True
        try:
            for att in atts:
                if att == 'area' and atts[att] == 'yes':
                    force_to_line = False
        except ValueError:
            force_to_line = True
    else:
        force_to_line = False
    return force_to_line


def process_ways(tempf, theme, features, output, block_count):
    """
    Each way is either a line or a polygon and writes out the appropraite geometry to a dictionary that is converted
    into a geopandas dataframe before being exported to a geopackage.

    The code as it stands does not account for relation that would create multipart polygons and holes
     in existing polygons
    :param tempf:
    :param theme:
    :param features:
    :param output:
    :param block_count:
    :return:
    """
    begin_time = time.time()
    print(f'Processing Ways for {theme}')

    lineb = False
    polygonb = False
    if 'line' in features:
        lineb = True

    if 'polygon' in features:
        polygonb = True

    # Grab attributes for theme for data schema
    std_flds = read_themes([theme])
    if lineb:
        line_flds = {'way_id': [], 'geometry': []}
        for item in std_flds[theme]:
            line_flds[item] = []
    if polygonb:
        poly_flds = {'way_id': [], 'geometry': []}
        for item in std_flds[theme]:
            poly_flds[item] = []

    completed_lines_count = 0
    completed_polygons_count = 0
    completed_ways_count = 0

    """
    Loop through each node block, loading each into memory in turn
    Each way theme has a single pickle file while the nodes have multiple files
    With each iteration of a node file a way is either created when it has all it nodes
    or updated with coordinate information from the nodes it could find and resaved into 
    a temp file that is saved over the theme way file at the end block loop
    """
    for block_num in range(1, block_count + 1):
        nodes = {}
        print(f'\tLoading block: {block_num} of {block_count} for {theme} theme')
        node_file = os.path.join(tempf, f'nodeblock_{block_num}.pkl')
        nodes_file_list = list(loadall(node_file))
        # Add nodes from block to a dictionary
        try:
            for node_string in nodes_file_list:
                node_list = node_string.split(':')
                nodes[node_list[0]] = (node_list[1], node_list[2])
        except Exception as e:
            print(e)
            print(f'\t\tError loading block: {block_num} of {block_count}')
            continue  # Should still get some useful features if we continue

        # Get table of unbuilt ways, create new table to be populated with still unbuilt ways
        try:
            # Load pickle theme element
            pkl_ways = os.path.join(tempf, f'{theme}_way.pkl')
            unbuilt_ways = list(loadall(pkl_ways))
            still_unbuilt_ways = open(os.path.join(tempf, f'still_unbuilt_ways{theme}.pkl'), 'wb')

        except Exception as e:
            print('\t\tError saving unbuilt ways table!')
            continue  # Should still get some useful features if we continue

        # print(len(unbuilt_ways))
        for way in unbuilt_ways:

            if completed_ways_count > 0 and completed_ways_count % 10000 == 0:
                print('\t\tBuilt ways: {0:,}'.format(completed_ways_count))

            way_nodes_id_list = way['ref_remaing']  # Starts off as an exact copy of ref key
            ref_remaing = [] #key that blank are appended to this list
            way_nodes_list = []
            is_way_complete = True  # Can become false if nodes are not in current block

            # Loop through each node id in the way, attempting to find its coordinates in node block
            for way_node_id in way_nodes_id_list:
                if way_node_id in nodes:
                    node_coords = nodes[way_node_id]  # Look up node coordinates in nodes dict
                    way['coords'][way_node_id] = node_coords

                # If not found, flag to place in still unbuilt ways table
                # No way is left behind unless the there is no representive node in any o fthe files
                else:
                    ref_remaing.append(way_node_id)
                    is_way_complete = False

            if is_way_complete:
                way_shape = []
                for nd in way['ref']:
                    way_shape.append((float(way['coords'][nd][0]), float(way['coords'][nd][1])))

                # There are ways in the OSM file that are missing corresponding nodes.
                # There are also some ways with partial nodes but the nodes seem to still be in order
                if len(way_shape) <= 1:
                    continue

                # Get first and last nodes
                start_point = way_shape[0]
                end_point = way_shape[-1]

                # If closed way, examine attributes to determine whether to force the way to be a line
                if start_point[0] == end_point[0] and start_point[1] == end_point[1]:
                    force_way_to_line = determine_force_way_to_line(theme, way['attrib'])

                # Process Lines
                if lineb and (not (
                        start_point[0] == end_point[0] and start_point[1] == end_point[1]) or force_way_to_line):
                    line_flds['way_id'].append(way['way_id'])
                    line = [(shape[0], shape[1]) for shape in way_shape]
                    linestring = LineString(line)
                    line_flds['geometry'].append(linestring)

                    for key in line_flds:
                        if key in way['attrib']:
                            line_flds[key].append(way['attrib'][key])
                        elif key != 'way_id' and key != 'geometry':
                            line_flds[key].append('')
                    completed_lines_count += 1

                # Find polygons...need at least three points
                elif polygonb and (start_point[0] == end_point[0] and start_point[1] == end_point[1] and len(
                        way_shape) > 3):
                    poly_flds['way_id'].append(way['way_id'])
                    polygon = []
                    for shape in way_shape:
                        polygon.append((shape[0], shape[1]))
                    polygon = Polygon(polygon)
                    poly_flds['geometry'].append(polygon)

                    for key in poly_flds:
                        if key in way['attrib']:
                            poly_flds[key].append(way['attrib'][key])
                        elif key != 'way_id' and key != 'geometry':
                            poly_flds[key].append('')
                    completed_polygons_count  += 1

            else:
                # Save incomplete way info
                way['ref_remaing'] = ref_remaing
                pickle.dump(way, still_unbuilt_ways)
        try:
            nodes.clear()
            still_unbuilt_ways.close()
            os.remove(os.path.join(tempf, f'{theme}_way.pkl'))
            os.rename(os.path.join(tempf, f'still_unbuilt_ways{theme}.pkl'), os.path.join(tempf, f'{theme}_way.pkl'))
        except Exception as e:
            print(e)
            print('\tError cleaning up block number: {0}'.format(block_num))

    # for key in line_flds:
    #     print(f"{key},{len(line_flds[key])}")
    # for key in poly_flds:
    #     print(f"{key},{len(poly_flds[key])}")

    output_gpkg = os.path.join(output, f'{theme}.gpkg')

    if polygonb:
        if len(poly_flds['way_id']) > 0:
            poly_gdf = gpd.GeoDataFrame(poly_flds, geometry='geometry')
            poly_gdf.set_crs(epsg=4326, inplace=True)
            poly_gdf.to_file(output_gpkg, layer=f'{theme}_polygon', driver="GPKG")

    if lineb:
        if len(line_flds['way_id']) > 0:
            line_gdf = gpd.GeoDataFrame(line_flds, geometry='geometry')
            line_gdf.set_crs(epsg=4326, inplace=True)
            line_gdf.to_file(output_gpkg, layer=f'{theme}_line', driver="GPKG")
        else:
            print(f'Line Theme {theme} is empty')

    text = f'Line and Polgon Theme {theme} completed after {round(time.time() - begin_time, 0)} ' \
           f'seconds with {completed_lines_count} lines and {completed_polygons_count} polygons.'
    return text


def process_nodes(tempf: str, theme: str, output: str):
    """
    Process Point Themes in a GeoPackage
    :param tempf:
    :param theme:
    :param output:
    :return:
    """
    begin_time = time.time()
    print(f'Processing Nodes for {theme}')
    count = 0
    # Load pickle theme element
    pkl_points = os.path.join(tempf, f'{theme}_point.pkl')

    # Build Data Structure
    flds = {}
    std_flds = read_themes([theme])

    # Add key for addional information
    flds['node_id'] = []
    flds['geometry'] = []

    for item in std_flds[theme]:
        flds[item] = []

    # Load data from pickle file
    for node in list(loadall(pkl_points)):
        # print(node)
        for tag in flds:
            if tag in node:
                flds[tag].append(node[tag])
            else:
                flds[tag].append('')

        count += 1
    if len(flds['geometry']) > 0:
        output_gpkg = os.path.join(output, f'{theme}.gpkg')
        point_gdf = gpd.GeoDataFrame(flds, geometry='geometry')
        point_gdf.set_crs(epsg=4326, inplace=True)
        point_gdf.to_file(output_gpkg, layer=f'{theme}_point', driver="GPKG")
    else:
        print(f'Point Theme {theme} is empty')

    text = f'Point Theme {theme} completed after {round(time.time() - begin_time, 0)} seconds with {count} points.'
    return text


if __name__ == '__main__':
    begin_time = time.time()
    # osm = 'C:/OSM/andorra-latest_clipped_to_poly.osm'
    osm = '/vagrant/andorra-latest_clip_-180.0_180.0_-90.0_90.0.osm'
    osm = '/vagrant/serbia-latest_clip_-180.0_180.0_-90.0_90.0.osm'
    # osm = 'C:/OSM/germany-latest_clip_-180.0_180.0_-90.0_90.0.osm'
    themes = ['aerialway', 'aeroway', 'amenity', 'barrier', 'boundary', 'building', 'craft', 'emergency', 'geological',
              'highway', 'historic', 'landuse', 'leisure', 'man_made', 'military', 'natural', 'office', 'place',
              'power',
              'public_transport', 'railway', 'route', 'shop', 'sport', 'tourism', 'waterway']

    # themes = ['aerialway', 'aeroway', 'amenity', 'barrier', 'building', 'highway']
    #themes = ['emergency', 'geological','highway', 'historic', 'landuse']
    features = ['point', 'line', 'polygon']
    # features = ['line']
    output = '/vagrant/output'
    workers = 3

    #Create Pickle Files
    tempf, block_count = read_osm(osm, themes, features)
    #print(tempf, block_count)
    #tempf = 'tmp5ar4stsn'
    #block_count = 4
    if 'point' in features:

        futures = []
        with ProcessPoolExecutor(max_workers=3) as executor:
            for theme in themes:
                futures.append(executor.submit(process_nodes, tempf, theme, output))
            for x in as_completed(futures):
                print(x.result())


    if 'line' in features or 'polygon' in features:
        futures = []
        with ProcessPoolExecutor(max_workers=3) as executor:
            for theme in themes:
                futures.append(executor.submit(process_ways, tempf, theme, features, output, block_count))
            for x in as_completed(futures):
                print(x.result())

    print(f'Done all themes after { round(time.time() - begin_time, 0)} seconds.')

    # Remove temp folder
    if os.path.exists(tempf):
        rmtree(tempf)


