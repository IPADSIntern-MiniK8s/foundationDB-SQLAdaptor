import sys
sys.path.append("..")

from repository import HeaderRepo
from repository import GeoRepo


class ClientService(object):
    def __init__(self):
        self.header_repo = HeaderRepo()
        self.geo_repo = GeoRepo()