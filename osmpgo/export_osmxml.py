from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
import csv
import os
import pkg_resources
import time
from shutil import rmtree
import tempfile
# import sys
import pickle
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString
from typing import Iterable, Any


def read_themes(themes: list) -> dict:
    """
        Uses CSV file and theme parameter to create custom dictionary base on users input

    Args:
        themes: list of key OSM themes

    Returns:
        The return value is dictionary that represents the data/field structure of the them
    """

    std_flds_all = {}
    # config_file = os.path.join(os.path.dirname(__file__), 'categories_and_fields.csv')
    config_file = pkg_resources.resource_filename("osmpgo", 'data/categories_and_fields.csv')
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


class ReadOSM:
    """
        Processing Class
    """

    def __init__(self, inputs: str, themes: list, features: list, mem_factor: int):
        self.inputs = inputs
        self.themes = themes
        self.features = features
        self.pointb = False
        self.lineb = False
        self.polygonb = False
        self.tempf = None
        self.std_flds = None
        self.categories = None
        self.mem_factor = mem_factor
        self.block_count = 0

        if 'point' in features:
            self.pointb = True

        if 'line' in features:
            self.lineb = True

        if 'polygon' in features:
            self.polygonb = True

        self.tempf = tempfile.mkdtemp(dir=os.getcwd())

        self.std_flds = read_themes(themes)
        self.categories = list(self.std_flds.keys())
        # print(f'Processing: {",".join(self.categories)}')

    @staticmethod
    def get_element_name(line: str) -> str:
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

    @staticmethod
    def get_attribute_value(name: str, line: str) -> str:
        """
        Gets the value of the named attribute from the string
        Args:
            name: Name of Tag
            line: XML Line

        Returns:
            The return value is a string representing the attribute
        """
        sa = line.find(' ' + name + '="') + len(name) + 3
        ea = line.find('"', sa)
        attr = line[sa:ea]
        return attr

    def get_node_details(self, line: str) -> tuple:
        """

        Args:
            line: XML Line

        Returns:
            The return value is a tuple of the ID, Longitude and Lattitude extracted from the XML line.
        """
        nid = str(self.get_attribute_value('id', line))
        nx = float(self.get_attribute_value('lon', line))
        ny = float(self.get_attribute_value('lat', line))

        return nid, nx, ny

    def return_id(self, line: str) -> str:
        """
         Get the id attribute from a line of xml text used for ways and its segments; id is only attribute needed

        Args:
            line: XML Line

        Returns:
            The return value is a string that represent the ID

        """

        return self.get_attribute_value('id', line)

    def get_tag_details(self, line: str) -> tuple:
        """
        Get key and value from tag
        Args:
            line: XML Line

        Returns:
            The return value is a tuple of the Key and Value extracted from the XML line.
        """

        k = self.get_attribute_value('k', line)[:29]
        v = self.get_attribute_value('v', line)[:254]

        return k, v

    def readxml(self):
        """
        Reads, interprets XML file then write objects to pickle file.
        Returns:
            None
        """

        # Create initial temp files to keep track of nodes and ways
        self.block_count = 1

        open_files = {}
        for key in self.std_flds:
            if self.lineb or self.polygonb:
                open_files[f'{key}_way'] = open(os.path.join(self.tempf, f'{key}_way.pkl'), 'wb')
            if self.pointb:
                open_files[f'{key}_point'] = open(os.path.join(self.tempf, f'{key}_point.pkl'), 'wb')

        node_file = open(os.path.join(self.tempf, f'nodeblock_{self.block_count}.pkl'), 'wb')

        has_valid_tags = False  # Will be set to true when first valid tag is found

        # Create basic objects to keep track of features
        node_count = 0
        way_count = 0
        point_feature_count = 0
        # line_count = 0
        type_code = -1  # -1 is not yet set, 1 is a node, 2 is a way
        feature_tags = []
        block_size = self.mem_factor * 1000000  # Size of each temp file for storing nodes

        xml_file = open(self.inputs, 'rb')
        line_count = 0
        for xml_line in xml_file:
            # print(xml_line)
            try:
                # Source should be in utf-8, but encoding causes problems sometimes
                # u_line = unicode(xml_line, 'utf-8', 'replace')
                u_line = xml_line.decode('utf-8')
                element_name = self.get_element_name(u_line)
                # print(element_name)
                line_count += 1
                # print(element_name)
            except Exception as e:
                print(f'\tError reading line in file: {xml_line}')
                print(e)
                continue

            if element_name == 'node':

                try:
                    type_code = -1  # Still -1 until we know node is valid
                    feature_tags = []
                    node_details = self.get_node_details(u_line)
                    has_valid_tags = False

                    # Make sure node coordinates are valid geographically
                    if -180 <= node_details[1] <= 180 and -90 <= node_details[2] <= 90:
                        type_code = 1
                        # Start a new node block if size limit reached
                        if node_count > self.block_count * block_size:
                            node_file.close()
                            self.block_count += 1
                            node_file = open(os.path.join(self.tempf, f'nodeblock_{self.block_count}.pkl'), 'wb')
                        # info = f'{node_details[0]}:{node_details[1]}:{node_details[2]}'
                        # pickle.dump(info, node_file)
                        pickle.dump([node_details[0], node_details[1], node_details[2]], node_file)

                        node_count += 1
                        if node_count > 0 and node_count % 1000000 == 0:
                            print(f'\tCounting nodes: {node_count:,}')

                except Exception as e:
                    print(e)
                    print('\tError reading node!')
                    continue

            elif element_name == 'way':
                type_code = 2
                has_valid_tags = False

                if way_count > 0 and way_count % 100000 == 0:
                    print(f'\tCounting ways: {way_count:,}')

                way = (self.return_id(u_line), '')
                way_ref_list = []
                feature_tags = []

            # nd element will only be found inside a way, save it to its way string
            elif element_name == 'nd':
                way_ref_list.append(self.get_attribute_value('ref', u_line))
            # tag elements can be found inside nodes or ways
            elif element_name == 'tag':

                # Get name and value of the tag
                tag_details = self.get_tag_details(u_line)

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

                if self.pointb:
                    # Node details were saved when opening <node> element was read

                    try:
                        #                 # Loop through the node's tags
                        for tag_kv in feature_tags:
                            node_cursor_key = tag_kv[0]
                            # If tag matches a feature class, find that cursor
                            if node_cursor_key in self.categories:
                                values = {'node_id': node_details[0],
                                          'geometry': Point(node_details[1], node_details[2])}
                                # node_cat = node_cursor_key
                                node_fieldnames = self.std_flds[node_cursor_key]
                                # Loop through tags again, inserting into field values
                                for the_tag in feature_tags:
                                    the_key = the_tag[0]
                                    if the_key in node_fieldnames:
                                        value = str(the_tag[1])
                                        values[the_key] = value

                                pickle.dump(values, open_files[f'{node_cursor_key}_point'])
                                point_feature_count += 1
                    except Exception as e:
                        print(f'\tError processing node with ID: {node_details[0]}')
                        print(e)

                has_valid_tags = False  # Reset valid tags flag

            # No way will be only one line in the XML, we will have read through its
            # component <nd ref> and <tag> elements
            elif '/way' in element_name and has_valid_tags:

                # Done with way, now let's load its attributes (shape comes later)
                # Need to go back and come up with a better place to put this
                if len(feature_tags) > 0 and (self.lineb or self.polygonb):
                    way_id = str(way[0])  # From first line of way XML
                    try:
                        # Loop through the way's tags
                        for tag_kv in feature_tags:
                            key = tag_kv[0]
                            # If tag matches a feature class, we will use this way
                            if key in self.categories:
                                values = {'attrib': {}}
                                # way_cat = key
                                way_fieldnames = self.std_flds[key]
                                # Loop through tags again, inserting into field values
                                for the_tag in feature_tags:
                                    the_key = the_tag[0]
                                    if the_key in way_fieldnames:
                                        value = str(the_tag[1])
                                        values['attrib'][the_key] = value

                                values['ref'] = way_ref_list  # Used as an index to align points in the correct sequence
                                values['ref_remaing'] = way_ref_list  # Used to keep track of nodes in geometry creation
                                values['way_cat'] = key
                                values['way_id'] = way_id
                                values['coords'] = {}  # Place Holder for Ref Coords

                                # Dump way values to pickle theme
                                pickle.dump(values, open_files[f'{key}_way'])
                                way_count += 1

                    except Exception as e:
                        print(e)
                        print(f'\tError reading way with id: {way_id}')

                has_valid_tags = False  # Reset valid tags flag

        # Close xml_file if necessary
        if str(type(xml_file)) == "<type 'file'>":
            xml_file.close()

        print(f'\tCount: {node_count:,} nodes, {way_count:,} ways')
        print(f'\tPoint features produced: {point_feature_count:,}')

        # Close files that were written to
        node_file.close()

        for key in open_files:
            open_files[key].close()


class ProcessOSM:
    """
    Processing Class that readout the output fro the ReadOSM process
    """

    def __init__(self, themes: list, features: list, workers: int,
                 tempf: str, output: str, prefix: str, block_count: int):
        self.themes = themes
        self.features = features
        self.tempf = tempf
        self.block_count = block_count
        self.workers = workers
        self.output = output
        self.prefix = prefix

        self.pointb = False
        self.lineb = False
        self.polygonb = False
        self.std_flds = None
        self.categories = None

        if 'point' in features:
            self.pointb = True

        if 'line' in features:
            self.lineb = True

        if 'polygon' in features:
            self.polygonb = True

        self.std_flds = read_themes(themes)
        self.categories = list(self.std_flds)

    def process(self) -> None:
        """
        Mutliprocessing loop for point and line/polygons. Cleans up tmp directory at the end

        """
        try:
            if self.pointb:

                futures = []
                with ProcessPoolExecutor(max_workers=self.workers) as executor:
                    for theme in self.themes:
                        futures.append(executor.submit(self.process_nodes, theme))
                    for x in as_completed(futures):
                        print(x.result())

            if self.lineb or self.polygonb:

                futures = []
                with ProcessPoolExecutor(max_workers=self.workers) as executor:
                    for theme in self.themes:
                        futures.append(executor.submit(self.process_ways, theme))
                    for x in as_completed(futures):
                        print(x.result())

        except BrokenProcessPool as e:
            print(e)
            print('This was more than likely a memory issue. Try running with fewer or even 1 '
                  'work to troubleshoot problem')

        if os.path.exists(self.tempf):
            rmtree(self.tempf)

    def process_nodes(self, theme: str) -> str:
        """
        Process Point Themes in a GeoPackage
        Args:
            theme: Key theme from OSM

        Returns:
            The return value. String that describes completion

        """
        begin_time = time.time()
        print(f'Processing Nodes for {theme}')
        count = 0

        # Build Data Structure

        std_flds = read_themes([theme])
        flds = {'node_id': [], 'geometry': []}
        flds.update({item: [] for item in std_flds[theme]})

        # Load pickle theme element
        pkl_points = os.path.join(self.tempf, f'{theme}_point.pkl')
        # Load data from pickle file
        for node in list(self.loadall(pkl_points)):
            # print(node)
            for tag in flds:
                if tag in node:
                    flds[tag].append(node[tag])
                else:
                    flds[tag].append('')

            count += 1
        if len(flds['geometry']) > 0:
            output_gpkg = os.path.join(self.output, f'{self.prefix}_{theme}.gpkg')
            point_gdf = gpd.GeoDataFrame(flds, geometry='geometry')
            point_gdf.set_crs(epsg=4326, inplace=True)
            point_gdf.to_file(output_gpkg, layer=f'{theme}_point', driver="GPKG")
        else:
            print(f'Point Theme {theme} is empty')

        text = f'Point Theme {theme} completed after {round(time.time() - begin_time, 0)} seconds with {count} points.'
        return text

    @staticmethod
    def loadall(filename: str) -> Iterable[Any]:
        """
        Sequential Lazy Unpickler
        Args:
            filename: Filename of pickle file

        Returns:
            The return value. Unpickled OBject

        """
        with open(filename, "rb") as f:
            while True:
                try:
                    yield pickle.load(f)
                except EOFError:
                    break

    def process_ways(self, theme: str) -> str:
        """
        Each way is either a line or a polygon and writes out the appropraite geometry to a dictionary that is converted
        into a geopandas dataframe before being exported to a geopackage.

        The code as it stands does not account for relation that would create multipart polygons and holes
         in existing polygons
        Args:
            theme: Key theme from OSM

        Returns:
            The return value. String that describes completion

        """
        begin_time = time.time()
        print(f'Processing Ways for {theme}')

        # Grab attributes for theme for data schema
        std_flds = read_themes([theme])

        if self.lineb:
            line_flds = {'way_id': [], 'geometry': []}
            line_flds.update({item: [] for item in std_flds[theme]})
            # for item in std_flds[theme]:
            #    line_flds[item] = []
        if self.polygonb:
            poly_flds = {'way_id': [], 'geometry': []}
            poly_flds.update({item: [] for item in std_flds[theme]})
            # for item in std_flds[theme]:
            #     poly_flds[item] = []

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
        try:
            for block_num in range(1, self.block_count + 1):
                # print('theme')
                nodes = {}
                print(f'\tLoading block: {block_num} of {self.block_count} for {theme} theme')
                node_file = os.path.join(self.tempf, f'nodeblock_{block_num}.pkl')
                nodes_file_list = list(self.loadall(node_file))
                # Add nodes from block to a dictionary
                try:
                    for node_string in nodes_file_list:
                        # node_list = node_string.split(':')
                        # nodes[node_list[0]] = (node_list[1], node_list[2])
                        nodes[node_string[0]] = (node_string[1], node_string[2])
                except Exception as e:
                    print(e)
                    print(f'\t\tError loading block: {block_num} of {self.block_count}')
                    continue  # Should still get some useful features if we continue

                # Get table of unbuilt ways, create new table to be populated with still unbuilt ways
                try:
                    # Load pickle theme element
                    pkl_ways = os.path.join(self.tempf, f'{theme}_way.pkl')
                    # unbuilt_ways = list(self.loadall(pkl_ways))
                    # Less memory to lazy load the data
                    unbuilt_ways = self.loadall(pkl_ways)
                    still_unbuilt_ways = open(os.path.join(self.tempf, f'still_unbuilt_ways{theme}.pkl'), 'wb')

                except Exception as e:
                    print(e)
                    print('\t\tError saving unbuilt ways table!')
                    continue  # Should still get some useful features if we continue

                # print(len(unbuilt_ways))
                for way in unbuilt_ways:

                    if completed_ways_count > 0 and completed_ways_count % 10000 == 0:
                        print(f'\t\tBuilt ways: {completed_ways_count:,}')

                    way_nodes_id_list = way['ref_remaing']  # Starts off as an exact copy of ref key
                    ref_remaing = []  # key that blank are appended to this list
                    # way_nodes_list = []
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
                            force_way_to_line = self.determine_force_way_to_line(theme, way['attrib'])

                        # Process Lines
                        if self.lineb and (not (
                                start_point[0] == end_point[0] and start_point[1] == end_point[1]) or
                                           force_way_to_line):
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
                        elif self.polygonb and (start_point[0] == end_point[0] and start_point[1] == end_point[1] and
                                                len(way_shape) > 3):
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
                            completed_polygons_count += 1

                    else:
                        # Save incomplete way info
                        way['ref_remaing'] = ref_remaing
                        pickle.dump(way, still_unbuilt_ways)
                try:
                    nodes.clear()
                    still_unbuilt_ways.close()
                    os.remove(os.path.join(self.tempf, f'{theme}_way.pkl'))
                    os.rename(os.path.join(self.tempf, f'still_unbuilt_ways{theme}.pkl'),
                              os.path.join(self.tempf, f'{theme}_way.pkl'))
                except Exception as e:
                    print(e)
                    print(f'\tError cleaning up block number: {block_num}')

            # for key in line_flds:
            #     print(f"{key},{len(line_flds[key])}")
            # for key in poly_flds:
            #     print(f"{key},{len(poly_flds[key])}")

            print(f'Creating Geopacakge for {theme}')
            output_gpkg = os.path.join(self.output, f'{self.prefix}_{theme}.gpkg')

            if self.polygonb:
                if len(poly_flds['way_id']) > 0:
                    poly_gdf = gpd.GeoDataFrame(poly_flds, geometry='geometry')
                    poly_gdf.set_crs(epsg=4326, inplace=True)
                    poly_gdf.to_file(output_gpkg, layer=f'{theme}_polygon', driver="GPKG")

            if self.lineb:
                if len(line_flds['way_id']) > 0:
                    line_gdf = gpd.GeoDataFrame(line_flds, geometry='geometry')
                    line_gdf.set_crs(epsg=4326, inplace=True)
                    line_gdf.to_file(output_gpkg, layer=f'{theme}_line', driver="GPKG")
                else:
                    print(f'Line Theme {theme} is empty')

            text = f'Line and Polygon Theme {theme} completed after {round(time.time() - begin_time, 0)} ' \
                   f'seconds with {completed_lines_count} lines and {completed_polygons_count} polygons.'
        except Exception as e:
            print(e)

        return text

    @staticmethod
    def determine_force_way_to_line(cat: str, atts: dict) -> bool:
        """
        Part of the legacy code
        Args:
            cat: Key OSM theme
            atts: Fields for specific theme.

        Returns:
            The return value. True for success, False otherwise.
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
