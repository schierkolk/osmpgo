import timeit
from osmpgo.export_osmxml import ReadOSM, ProcessOSM


def test():
    inputs = '../andorra-latest_clip_-180.0_180.0_-90.0_90.0.osm'
    output = 'output'
    prefix = 'andorra'
    features = ['point', 'line', 'polygon']
    mem_factor = 4
    themes = ['aerialway', 'aeroway', 'amenity', 'boundary', 'building', 'craft', 'emergency', 'geological',
              'highway', 'historic', 'landuse', 'leisure', 'natural', 'office', 'place', 'power', 'public_transport',
              'railway', 'route', 'shop', 'tourism', 'waterway']
    workers = 3
    rosm = ReadOSM(inputs, themes, features, mem_factor)
    rosm.readxml()

    posm = ProcessOSM(themes, features, workers, rosm.tempf, output, prefix, rosm.block_count)
    posm.process()


if __name__ == '__main__':
    import timeit
    print(timeit.timeit("test()", setup="from __main__ import test", number=20))
