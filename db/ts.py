import jsonpickle

class Ts:

    def transformerFactory(transformMap, inverse=False):

        def _(row):
            if not transformMap:
                return row
            try:
                return {
                    key: transformMap[key](val, inverse) if key in transformMap else val for key, val in row.items()
                }
            except Exception as e:
                print(transformMap.keys())
                print(rows.keys())
                raise e

        return _

    def str(val, inverse=False):
        return None if val is None else str(val)

    def emptyNullStr(val, inverse=False):
        return '' if val is None else str(val)

    def int(val, inverse=False):
        return int(val)

    def float(val, inverse=False):
        if inverse:
            return float('NaN') if val is None else val
        return float(val)

    def boolAsInt(val, inverse=False):
        return bool(val) if inverse else int(val)

    def json(val, inverse=False):
        return jsonpickle.loads(val) if inverse else jsonpickle.dumps(val)

    def dateAsStr(val, inverse=False):
        if inverse:
            return datetime.strptime(val, '%Y-%m-%d')

        #print('val', val)
        return val if isinstance(val, str) else val.strftime('%Y-%m-%d')

    def nullableDateAsStr(val, inverse=False):
        if inverse:
            if val != '':
                return datetime.strptime(val, '%Y-%m-%d')
            return None

        #print('val', val)
        if val == '' or val is None:
            return ''
        return val if isinstance(val, str) else val.strftime('%Y-%m-%d')

    def dateTimeAsStr(val, inverse=False):
        if inverse:
            return datetime.fromisoformat(val)

        return val if isinstance(val, str) else val.isoformat()

    def nullableDateTimeAsStr(val, inverse=False):
        if inverse:
            if val != '':
                return datetime.fromisoformat()
            return None

        if val == '' or val is None:
            return ''
        return val if isinstance(val, str) else val.isoformat()

    def categoryAsStr(cats=[]):

        def _(val, inverse=False):
            if inverse:
                return val
            assert(val in cats)
            return val

        return _

    def npArrayAsJSON(val, inverse=False):
        if inverse:
            res = jsonpickle.decode(val)
            return res
        #res = val.tolist()
        #res = np.asarray(val)
        res = jsonpickle.encode(val)
        return res