from datetime import datetime
import jsonpickle
import dill


class Ts:

    class RowTransformer(dict):
        def __init__(self, *args, rowTransform=lambda row, *args, **kwargs: row, **kwargs):
            super().__init__(*args, **kwargs)

            self._rowTransform = rowTransform

        def __call__(self, row, inverse, *args, **kwargs):
            try:
                res = {
                    key: self[key](val, inverse) if key in self else val for key, val in row.items()
                }
            except Exception as e:
                print(f'tranformer keys: {self.keys()}')
                print(f'row keys:        {row.keys()}')
                raise e

            if callable(self.rowTransform):
                res = self.rowTransform(res, inverse, *args, **kwargs)

            return res

        @property
        def rowTransform(self):
            return self._rowTransform

        @rowTransform.setter
        def rowTransform(self, f):
            self._rowTransform = f

        @rowTransform.deleter
        def rowTransform(self):
            self._rowTransform = None

        def chainRowTransform(self, rowTransform):

            rowTransform0 = self.rowTransform

            def chainedRowTransform(row, inverse, *args, **kwargs):
                res = rowTransform0(row, inverse, *args, **kwargs)
                res = rowTransform(res, inverse, *args, **kwargs)
                return res

            self.rowTransform = chainedRowTransform

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
                print(row.keys())
                raise e

        return _

    def listTCreator(T):

        def helper(val: list, inverse=False):

            if inverse:
                return [T(item, inverse) for item in jsonpickle.loads(val)]

            return jsonpickle.dumps([T(item) for item in val])

        return helper

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
        if inverse:
            return jsonpickle.loads(val)

        return jsonpickle.dumps(val)

    def nullableJSON(val, inverse=False):
        if inverse:
            return val if val is None else jsonpickle.loads(val)

        return jsonpickle.dumps(val)

    def dateAsStr(val, inverse=False):
        if inverse:
            return datetime.strptime(val, '%Y-%m-%d')

        # print('val', val)
        return val if isinstance(val, str) else val.strftime('%Y-%m-%d')

    def floatAsStr(val, inverse=False):
        if inverse:
            return float(val)

        # print('val', val)
        return str(val)

    def nullableDateAsStr(val, inverse=False):
        if inverse:
            if val != '':
                return datetime.strptime(val, '%Y-%m-%d')
            return None

        # print('val', val)
        if val == '' or val is None:
            return ''
        return val if isinstance(val, str) else val.strftime('%Y-%m-%d')

    def dateTimeAsStr(val, inverse=False):
        if inverse:
            return datetime.fromisoformat(val)

        dt = datetime.fromisoformat(val) if isinstance(val, str) else val
        return dt.isoformat()

    def nullableDateTimeAsStr(val, inverse=False):
        if inverse:
            if val != '':
                return datetime.fromisoformat(val)
            return None

        if val == '' or val is None:
            return ''
        return val if isinstance(val, str) else val.isoformat()

    @staticmethod
    def categoryAsStr(cats=[]):

        def _(val, inverse=False):
            if inverse:
                return val
            assert val in cats, 'Category {} not in {}.'.format(val, cats)
            return val

        return _

    def npArrayAsJSON(val, inverse=False):
        if inverse:
            res = jsonpickle.decode(val)
            return res
        # res = val.tolist()
        # res = np.asarray(val)
        res = jsonpickle.encode(val)
        return res

    # def Class(val, inverse=False):
    #    if inverse:
    #        res = dill.load(val)
    #        return res
    #    res = dill.dumps(val)
    #    return res

    def nullable(T: callable, isNullChecker: lambda val: val is None) -> callable:

        def nullable(val, inverse=False):

            if isNullChecker(val):
                return None

            return T(val, inverse=inverse)

        nullable.__name__ = f'nullable_{T.__name__}'
        return nullable

    @staticmethod
    def Classes(val, inverse=False):
        if inverse:
            res = dill.loads(val)
            return res
        res = dill.dumps(val)
        return res
