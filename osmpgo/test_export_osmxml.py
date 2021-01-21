import pickle
from osmpgo.export_osmxml import ProcessOSM, ReadOSM
import pytest


@pytest.fixture
def create_processosm():
    return ProcessOSM(['test'], ['point'], 1, 'test', 'test', 'test', 4)


@pytest.fixture
def create_readosm():
    return ReadOSM('test.xml', ['test'], ['point'], 1)


def test_loadall(tmpdir, create_processosm):
    file = tmpdir.mkdir("pickle").join("pickle.pkl")
    node_file = open(file, 'wb')
    info = 'vampire'
    pickle.dump(info, node_file)
    info = 'wraith'
    pickle.dump(info, node_file)
    node_file.close()
    assert len(list(create_processosm.loadall(file))) == 2


def test_get_element_name(create_readosm):
    node = '<node id="625025" lat="42.5142133" lon="1.5527243"/>'
    assert create_readosm.get_element_name(node) == 'node'


def test_get_node_details(create_readosm):
    node = '<node id="625025" lat="42.5142133" lon="1.5527243"/>'
    assert create_readosm.get_node_details(node) == ('625025', 1.5527243, 42.5142133)


def test_get_attribute_value(create_readosm):
    node = '<node id="625025" lat="42.5142133" lon="1.5527243"/>'
    assert create_readosm.get_attribute_value('id', node) == '625025'


def test_return_id(create_readosm):
    node = '<node id="625025" lat="42.5142133" lon="1.5527243"/>'
    assert create_readosm.return_id(node) == '625025'


def test_get_tag_details(create_readosm):
    tag = '<tag k="highway" v="crossing"/>'
    assert create_readosm.get_tag_details(tag) == ('highway', 'crossing')

