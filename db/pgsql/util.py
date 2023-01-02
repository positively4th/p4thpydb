from ..util import Util as Util0

class Util(Util0):

    pNamePrefix = ':'

    def __init__(self):
        super().__init__('%(', ')s', '"', '%s', Util.pNamePrefix)


