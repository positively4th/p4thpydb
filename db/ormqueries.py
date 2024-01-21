import json
import ramda as R
import itertools
import inspect
from typeguard import check_type
from typeguard import typechecked
from typing import Union

from contrib.p4thpymisc.src.misc import items
from .ts import Ts


class ColumnSpecModel:

    def __init__(self, columSpec):
        self.columnSpec = columSpec

    def transform(self, val, inverse=False):
        return val if 'transform' not in self.columnSpec else (self.columnSpec['transform'](val, inverse))


class TableSpecModel:

    def __init__(self, tableSpec):
        self.tableSpec = tableSpec

    def allColumns(self, sort=True, quote=True):
        return [column for column, spec in items(self.tableSpec['columnSpecs'], sort=sort)]

    def Ts(self, inverse=False):
        return {
            name: spec['transform'] for name, spec in items(self.tableSpec['columnSpecs'], sort=True) if 'transform' in spec
        }


class ORMQueries:

    defBatchSize = 200

    __DEBUG__ = False

    class EmptyMarker():
        pass

    emptyMarker = EmptyMarker()

    def prepareReturning(self, tableSpec, returning, asString: bool = False):
        _returning = [] if returning is None else returning
        _returning = [expr.strip() for expr in _returning.split(
            ',')] if isinstance(_returning, str) else _returning
        _returning = tableSpec['columns'].keys() \
            if _returning == '*' else _returning
        _returning = set(_returning)

        if asString:
            if len(_returning) < 1:
                _returning = ''
            else:
                _returning = ' RETURNING ' + \
                    ' , '.join(self.util.quote(_returning))

        return _returning

    @classmethod
    def keysFromRow(cls, keyNames: list | tuple, row: dict | list | tuple) -> dict:
        return R.pick(keyNames)(
            row if isinstance(row, dict) else row[0]
        )

    @classmethod
    def valuesFromRow(cls, valueNames: list | tuple, row: dict | list | tuple) -> dict:
        return R.pick(valueNames)(
            row if isinstance(row, dict) else row[1]
        )

    @classmethod
    def peek(cls, it, steps=1):

        if steps < 1:
            return [], it

        val = next(it, cls.emptyMarker)
        if val is cls.emptyMarker:
            return [], it

        _it = itertools.chain([val], it)

        if steps < 2:
            return [val], _it

        res, _it = cls.peek(_it, steps-1)

        return [val] + res, _it

    def __init__(self, util, pipe):
        self.pipe = pipe
        self.util = util

    def _ensureTable(self, tableSpec):

        res = not self.tableExists(tableSpec)
        qArgs = []
        if res:
            qArgs += self._createTable(tableSpec)
        return (res, qArgs)

    def _createTable(self, tableSpec):
        allColumns = [
            '{} {}'.format(self.util.quote(name), spec['definition']) for name, spec in tableSpec['columnSpecs'].items()
        ]
        primaryKeys = [
            '{}'.format(self.util.quote(key)) for key in tableSpec['primaryKeys']
        ]

        q = 'CREATE TABLE {} ({}, PRIMARY KEY ({}))'.format(self.util.quote(tableSpec['name']),
                                                            ', '.join(
                                                                allColumns),
                                                            ', '.join(
                                                                primaryKeys)
                                                            )
        return [((q, {}), {})]

    def _createViews(self, tableSpec):
        views = tableSpec['views'] if 'views' in tableSpec else {}
        qArgs = []
        for viewName, viewSpec in views.items():
            qArgs += self._createView(viewName, viewSpec)
        return qArgs

    def _createView(self, viewName, viewSpec):
        q0 = 'DROP VIEW IF EXISTS {}'.format(self.util.quote(viewName))
        q1 = 'CREATE VIEW {} AS {}'.format(
            self.util.quote(viewName), viewSpec['query'])
        return [
            ((q0, {}), {}),
            ((q1, {}), {})
        ]

    def _dropTable(self, tableSpec):
        q = 'DROP TABLE IF  EXISTS {}'.format(
            self.util.quote(tableSpec['name']))
        return [((q, {}), {})]

    def _dropViews(self, tableSpec):
        viewSpecs = tableSpec['views'] if 'views' in tableSpec else []
        qArgs = []
        for viewName, viewSpec in viewSpecs.items():
            qArgs += self._dropView(viewName)
        return qArgs

    def _dropView(self, viewName):
        q = 'DROP VIEW IF  EXISTS {}'.format(self.util.quote(viewName))
        return [((q, {}), {})]

    def applyRowTransform(self, tableSpec, rows, inverse):
        return [
            tableSpec['rowTransform'](r, inverse) for r in rows
        ] if 'rowTransform' in tableSpec \
            else rows

    def _insert(self, tableSpec, groupedRows: tuple | list, batchSize, returning=None):

        _rows = self.applyRowTransform(tableSpec, groupedRows, False)
        model = TableSpecModel(tableSpec)
        TMap = model.Ts()
        columnModelMap = {
            name: ColumnSpecModel(spec) for name, spec in items(tableSpec['columnSpecs'], sort=True)
        }

        def insertHelper(rows):

            columns = [col for col in allColumns if col in rows[0]]
            p = {}
            placeholders = [
                [
                    self.util.p(p, columnModelMap[c].transform(r[c]))
                    for c in columns
                ]
                for r in rows
            ]
            placeholders = R.pipe(
                R.map(lambda group: '(' + ', '.join(group) + ')'),
                R.join(', '),
            )(placeholders)

            q = 'INSERT INTO {} ({}) VALUES {}'.format(self.util.quote(
                tableSpec['name']), ', '.join(self.util.quote(columns)), placeholders)
            qpT = (q, p, TMap)
            qpT = self._returning(tableSpec, qpT, returning)
            return qpT, {}

        if len(groupedRows) < 1:
            return []

        allColumns = model.allColumns()

        rowGroups = R.group_by(
            lambda r: json.dumps(sorted(r.keys()))
        )(_rows)

        res = []
        for _, groupedRows in rowGroups.items():
            rowCtr = 0
            while rowCtr < len(groupedRows):
                res.append(insertHelper(groupedRows[rowCtr:rowCtr+batchSize]))
                rowCtr += batchSize

        return res

    def _returning(self, tableSpec, qpTMap, returning=None):
        _returning = self.prepareReturning(tableSpec, returning, asString=True)
        q, p, TMap = self.util.qpTSplit(qpTMap)
        check_type(TMap, Union[dict, None])
        T = Ts.RowTransformer(TMap)
        if 'rowTransform' in tableSpec:
            T.rowTransform = tableSpec['rowTransform']
        q = '{} {}'.format(q, _returning)
        return (q, p, T)

    @classmethod
    def isSingleRowResult(cls, rows):
        if not inspect.isgenerator(rows):
            return rows is not None and len(rows) == 1, rows

        nextRows, _rows = cls.peek(rows, 2)

        return len(nextRows) == 1, _rows

    def ensureSingleRows(self, updRows):

        res = [None] * len(updRows)
        for i, updRow in enumerate(updRows):
            if updRow is not None:
                isSingleRow, rows = self.isSingleRowResult(updRow)
                res[i] = rows[0] if isSingleRow else None
        return res

    def _update(self, tableSpec, rows, debug=False, returning=None):

        _rows = self.applyRowTransform(tableSpec, rows, False)
        qArgs = []
        tsModel = TableSpecModel(tableSpec)
        TMap = tsModel.Ts()

        valueColumns = set(tableSpec['columnSpecs'].keys()).difference(
            set(tableSpec['primaryKeys']))
        if len(valueColumns) < 1 and returning is None:
            return
        valSpecs = {
            col: ColumnSpecModel(tableSpec['columnSpecs'][col]) for i, col in items(valueColumns, sort=True)
        }
        keySpecs = {
            col: ColumnSpecModel(tableSpec['columnSpecs'][col]) for i, col in items(tableSpec['primaryKeys'], sort=True)
        }
        valSpecs = {**keySpecs, **valSpecs}

        for i, row in enumerate(_rows):
            valRow = self.valuesFromRow(valSpecs.keys(), row)
            p = {}
            valAssigns = [
                '{} = {}'.format(self.util.quote(col), self.util.p(
                    p, spec.transform(valRow[col], inverse=False)))
                for col, spec in items(valSpecs, sort=True)
                if col in valRow
            ]

            keyRow = self.keysFromRow(valSpecs.keys(), row)
            keyWheres = [
                '{} = {}'.format(self.util.quote(col), self.util.p(
                    p, spec.transform(keyRow[col], inverse=False)))
                for col, spec in items(keySpecs, sort=True)
                if col in keyRow
            ]

            if len(valAssigns) > 0:
                q = 'UPDATE {} SET {} WHERE {}'.format(self.util.quote(tableSpec['name']),
                                                       ','.join(valAssigns),
                                                       ' AND '.join(keyWheres)
                                                       )
                qpT = self._returning(tableSpec, (q, p, TMap), returning)
            else:
                q = 'SELECT {} FROM {} WHERE {}'.format(','.join(self.util.quote(returning)),
                                                        self.util.quote(
                                                            tableSpec['name']),
                                                        ' AND '.join(keyWheres)
                                                        )
                T = Ts.RowTransformer(TMap)
                if 'rowTransform' in tableSpec:
                    T.rowTransform = tableSpec['rowTransform']
                qpT = (q, p, T)
            qArgs.append((qpT, {}))
        return qArgs

    def _upsertInsert(self, tableSpec, rows, res, batchSize, returning=None):
        ups = []
        ins = []
        for i, rowGen in enumerate(res):
            insRow = rows[i]
            if rowGen is not None:
                genRows, rowGen = self.peek(rowGen, 1)
                if len(genRows) == 1:
                    ups.append(rowGen)
                    insRow = None
            if insRow is not None:
                ins.append(insRow)

        # return ups, self._insert(tableSpec, ins, returning=tableSpec['primaryKeys'], batchSize=batchSize)
        return ups, self._insert(tableSpec, ins, returning=returning, batchSize=batchSize)

    @typechecked
    def _upsertUpdate(self, tableSpec, rows, returning: set = None):

        res = self._update(
            tableSpec, rows, returning=set(tableSpec['primaryKeys']).union(returning))

        return res, R.difference(tableSpec['primaryKeys'], returning)

    def join(self, qpT, relatedSpec):

        q, p, T = self.util.qpTSplit(qpT)

        # print(relatedSpec)
        onMap = [
            '_r.{} = _l.{}'.format(key, fKey) for fKey, key in relatedSpec['foreignKeyMap'].items()
        ]
        aliases = [
            '{} {}'.format(expr, self.util.quote(alias)) for alias, expr in relatedSpec['columns'].items()
        ]
        q = 'SELECT _l.*, {} FROM ({}) _l'.format(', '.join(aliases), q)
        q = '{} INNER JOIN ({}) _r ON {}'.format(
            q, relatedSpec['select'], ' AND '.join(onMap))
        return q, p, T

    def _deleteSelectQuery(self, tableSpec, keyMaps):

        p = {}
        keyMapsT = [
            {
                col: ColumnSpecModel(
                    tableSpec['columnSpecs'][col]).transform(val)
                for col, val in items(keyMap)
            } for keyMap in keyMaps
        ]

        table = self.util.quote(tableSpec['name'])
        keys = [self.util.quote(key) for key in tableSpec['primaryKeys']]

        qs = [
            self.pipe.equals(('SELECT {} FROM {}'.format(
                ','.join(keys), table), []), map=keyMap)
            for keyMap in keyMapsT
        ]

        p = {}
        q = 'SELECT * FROM {}'.format(table)
        q, p, T = self.pipe.any((q, p, None), pipes=[
            [self.pipe.equals, {'map': keyMapT, }]
            for keyMapT in keyMapsT
        ])

        return [((q, p, T), {})]

    # Todo: Write more efficient method in db specific orm classes.
    def _deleteDeleteQuery(self, tableSpec, q, p):

        tss = TableSpecModel(tableSpec)
        table = self.util.quote(tableSpec['name'])
        keys = [self.util.quote(key) for key in tableSpec['primaryKeys']]

        existAlias = '_eq'
        q = 'SELECT {} FROM ({}) AS {}'.format(', '.join(keys), q, existAlias)
        where = ' AND '.join([
                '{table}.{key} = {alias}.{key}'.format(table=table, alias=existAlias, key=key) for key in keys
        ])

        q = 'DELETE FROM {} WHERE EXISTS ({} WHERE {})'.format(table, q, where)
        T = Ts.RowTransformer(tss.Ts())
        if 'rowTransform' in tableSpec:
            T._rowTransform = tableSpec['rowTransform']

        return [((q, p, T), {})]

    def select(self, tableSpec):
        tss = TableSpecModel(tableSpec)
        allColumns = ', '.join(self.util.quote(tss.allColumns()))
        q = 'SELECT {} FROM {}'.format(
            allColumns, self.util.quote(tableSpec['name']))
        p = []
        T = Ts.RowTransformer(tss.Ts())
        if 'rowTransform' in tableSpec:
            T._rowTransform = tableSpec['rowTransform']
        return q, p, T

    def view(self, tableSpec, view):
        viewSpec = tableSpec['views'][view]

        tss = TableSpecModel(tableSpec)
        csModels = {
            name: ColumnSpecModel(spec) for name, spec in viewSpec['columnSpecs'].items()
        }
        allColumns = ', '.join(self.util.quote(tss.allColumns()))

        q = 'SELECT {} FROM {}'.format(allColumns, self.util.quote(view))
        p = []
        T = Ts.RowTransformer({name: cs['transform'] for name, cs in items(
            viewSpec['columnSpecs'], sort=True)})
        return (q, p, T)

    @staticmethod
    def allColumns(tableSpec):
        return [
            '"{}"'.format(columnSpec[column]) for column, spec in items(tableSpec, sort=True)
        ]

    @property
    def ph(self):
        return self.util.placeholder
