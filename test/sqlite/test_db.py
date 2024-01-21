import unittest
from collections import OrderedDict

from db.sqlite.db import DB
from db.sqlite.util import Util
from db.ts import Ts
from db.sqlite.orm import ORM
from db.sqlite.pipes import Pipes


class TestDBSQLite(unittest.TestCase):

    def test_legacy(self):

        # Util

        util = Util()
        q = '''
        SELECT :p1, p2
        FROM a as _t
        WHRER :_p3 > :5p
        '''
        p = {
            'p1': 1,
            'p2': 2,
            '_p3': 3,
            'p4': 4,
            '5p': 5,
        }
        pStripped = util.pStrip(q, p)
        # print(pStripped)
        assert {
            'p1': 1,
            '_p3': 3,
            '5p': 5
        } == pStripped

        # DB
        # dbFile = os.path.join(tempfile.gettempdir(), str(uuid4()))
        dbFile = ':memory:'
        tmpSchemaFile = ':memory:'
        # tmpSchemaFile = '/tmp/testattached {}.db'.format(str(uuid4))
        db = DB(dbFile, attaches={
            't mp': tmpSchemaFile,
        })
        util = Util()
        pipes = Pipes()
        orm = ORM(db)
        db.query(
            'CREATE TABLE strvec3 (id TEXT, a REAL, b REAL, c REAL, PRIMARY KEY (id))')
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)")

        db.query('CREATE TABLE {} (id TEXT, a REAL, b REAL, c REAL, PRIMARY KEY (id))'.format(
            util.quote('t mp._tstrvec3')), debug=False)
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('111', 10, 10, 10)".format(
            util.quote('t mp._tstrvec3')))
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('123', 10, 20, 30)".format(
            util.quote('t mp._tstrvec3')))
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('222', 20, 20, 20)".format(
            util.quote('t mp._tstrvec3')))
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('321', 30, 20, 10)".format(
            util.quote('t mp._tstrvec3')))
        db.query("INSERT INTO {} (id, a, b, c) VALUES ('333', 30, 30, 30)".format(
            util.quote('t mp._tstrvec3')))

        # attached
        row = db.query(
            "SELECT * FROM {} WHERE id = '321'".format(util.quote('t mp._tstrvec3')), fetchAll=True)
        assert len(row) == 1
        row = row[0]
        assert {
            'id': '321',
            'a': 30,
            'b': 20,
            'c': 10,
        } == row

        # quote
        col = 'alfa'
        assert util.quote(col) == '`alfa`'
        assert util.quote(col, False) == 'alfa'

        cols = ['alfa', 'beta']
        # print('cols', util.quote(cols))
        assert util.quote(cols) == ['`alfa`', '`beta`']
        assert util.quote(cols, False) == ['alfa', 'beta']

        # ORM

        # createTable, dropTable, insert, update
        tableSpec = {
            'name': "apifootballteam_v0",
            'columnSpecs': {
                'team_id': {
                    'definition': "TEXT NOT NULL",
                    'transform': Ts.str,
                },
                'name': {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                },
                'country': {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                },
                'verified': {
                    'definition': "INT NOT NULL DEFAULT 0",
                    'transform': Ts.boolAsInt,
                },
            },
            'primaryKeys': ["team_id"],
            'views': OrderedDict({}),
        }
        tableSpec['views']['prefixed'] = {
            'query': "SELECT {team_id}, '_' || {name} {name}, {country}, {verified} FROM {table}".format(
                team_id=util.quote('team_id'),
                name=util.quote('name'),
                country=util.quote('country'),
                verified=util.quote('verified'),
                table=util.quote(tableSpec['name']),
            ),
            'columnSpecs': {
                'team_id': {
                    'definition': "TEXT NOT NULL",
                    'transform': Ts.str,
                },
                'name': {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    # prefixed by _ twice!
                    'transform': lambda n, *_: '_' + Ts.str(n, True),
                },
                'country': {
                    'definition': "TEXT NOT NULL DEFAULT ''",
                    'transform': Ts.str,
                },
                'verified': {
                    'definition': "INT NOT NULL DEFAULT 0",
                    'transform': Ts.boolAsInt,
                },
            },
        }

        # print(tableSpec['views'])
        # DB.__DEBUG__ = True
        orm.createTable(tableSpec)
        # print(tableSpec['columnSpecs'])
        assert True == orm.tableExists(tableSpec)
        assert False == orm.ensureTable(tableSpec)
        orm.dropTable(tableSpec)
        assert True == orm.ensureTable(tableSpec)
        assert True == orm.tableExists(tableSpec)
        assert False == orm.ensureTable(tableSpec)
        assert True == orm.tableExists(tableSpec)
        orm.insert(tableSpec, [
            {'team_id': 't1', 'name': 'n1', 'country': '1', 'verified': True, },
            {'team_id': 't2', 'name': 'n2', 'country': '2', 'verified': 0, }
        ], debug=False)
        # assert 1 == 0
        rows = db.query(orm.select(tableSpec), fetchAll=True)
        rows.sort(key=lambda row: row['team_id'])
        assert 'n1' == rows[0]['name']
        assert True == rows[0]['verified']
        assert 'n2' == rows[1]['name']
        assert False == rows[1]['verified']

        rows = db.query(orm.view(tableSpec, view='prefixed'), fetchAll=True)
        rows.sort(key=lambda row: row['team_id'])
        assert '__n1' == rows[0]['name']
        assert True == rows[0]['verified']
        assert '__n2' == rows[1]['name']
        assert False == rows[1]['verified']

        orm.update(tableSpec, [
            {'team_id': 't1', 'verified': False, },
            {'team_id': 't2', 'verified': 1, }
        ])
        rows = db.query(orm.select(tableSpec), fetchAll=True)
        rows.sort(key=lambda row: row['team_id'])
        assert 'n1' == rows[0]['name']
        assert False == rows[0]['verified']
        assert 'n2' == rows[1]['name']
        assert True == rows[1]['verified']

        orm.dropTable(tableSpec)

        # upsert Todo: Test with composite keys!
        orm.createTable(tableSpec)
        assert True == orm.tableExists(tableSpec)

        orm.upsert(tableSpec, [
            {'team_id': 't2', 'name': 'n2', 'country': '2', 'verified': 0, 'index': 0}
        ])
        rows = db.query(orm.select(tableSpec), fetchAll=True)
        rows.sort(key=lambda row: row['team_id'])
        assert len(rows) == 1
        assert 'n2' == rows[0]['name']
        assert False == rows[0]['verified']

        orm.upsert(tableSpec, [
            {'team_id': 't1', 'name': 'nn1',
                'country': '1', 'verified': 0, 'index': 10},
            {'team_id': 't2', 'name': 'n2', 'country': '2',
                'verified': True, 'index': 20}
        ], fetchAll=True)
        rows = db.query(orm.select(tableSpec), fetchAll=True)
        rows.sort(key=lambda row: row['team_id'])
        assert len(rows) == 2
        assert 't1' == rows[0]['team_id']
        assert 'nn1' == rows[0]['name']
        assert False == rows[0]['verified']
        assert 't2' == rows[1]['team_id']
        assert 'n2' == rows[1]['name']
        assert True == rows[1]['verified']

        orm.dropTable(tableSpec)

        # select
        # dbFile = os.path.join(tempfile.gettempdir(), str(uuid4()))
        dbFile = ':memory:'
        db = DB(dbFile)
        db.query(
            'CREATE TABLE strvec3 (id TEXT, a REAL, b REAL, c REAL, PRIMARY KEY (id))')
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)")
        tableSpec = {
            'name': "strvec3",
            'columnSpecs': {
                'id': {'definition': "TEXT NOT NULL", 'transform': Ts.str, },
                'a': {'definition': "REAL"},
                'b': {'definition': "REAL", 'transform': Ts.str, },
                'c': {'definition': "REAL", 'transform': Ts.str, },
            },
            'primaryKeys': ["id"]
        }

        assert True == orm.tableExists(tableSpec)
        assert False == orm.ensureTable(tableSpec)

        qpT = orm.select(tableSpec)
        qpT = pipes.order(qpT, ['id'])
        rows = db.query(qpT, debug=False, fetchAll=True)
        # for row in rows:
        #    print(row)
        assert rows[0]['id'] == '111'
        assert rows[0]['a'] == 1
        assert rows[2]['id'] == '222'
        assert rows[2]['a'] == 2
        assert rows[3]['id'] == '321'
        assert rows[3]['a'] == 3

        # Pipes
        # equals
        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.equals((q, p), map=[
            {'a': 1, 'b': 1, 'c': 1},
            {'a': 3, 'b': 2, 'c': 1},
        ])
        rows = db.query((q, p, T), fetchAll=True)
        assert len(rows) == 2

        # map, pipes
        q = 'SELECT * FROM strvec3'
        p = []
        qpT = pipes.concat((q, p), pipes=[
            [pipes.equals, {'map': {'a': 1, 'b': 1, 'c': 1}}]
        ])
        rows = db.query((qpT), fetchAll=True)
        assert len(rows) == 1
        assert rows[0]['id'] == '111'

        # pipesLike, pipesPipes
        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.concat((q, p), pipes=[
            [pipes.like, {'expr': 'id', 'pattern': '%2%'}],
            [pipes.order, {'exprs': ['id'], 'orders': ['DESC']}],
        ])
        rows = db.query((q, p, T), fetchAll=True)
        assert len(rows) == 3
        assert rows[2]['id'] == '123'
        assert rows[1]['id'] == '222'
        assert rows[0]['id'] == '321'

        # pipesValues, order, limit
        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.concat((q, p), pipes=[
            [pipes.member, {'expr': 'a', 'values': ['2', '3']}],
            [pipes.order, {'exprs': ['a', 'b', 'c'],
                           'orders': ['DESC', 'DESC', 'DESC']}],
            [pipes.limit, {'limit': 10}],
        ])
        rows = db.query((q, p, T), fetchAll=True)
        assert len(rows) == 3
        assert rows[0]['id'] == '333'
        assert rows[1]['id'] == '321'
        assert rows[2]['id'] == '222'

        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.concat((q, p), [
            [pipes.member, {'expr': 'a', 'values': ['2', '3']}],
            [pipes.order, {'exprs': ['a', 'b', 'c'],
                           'orders': ['DESC', 'DESC', 'DESC']}],
            [pipes.limit, {'limit': 2, 'offset': 1}]
        ])
        # [id, a, b, c] VALUES ('222', 2, 2, 2), ('321', 3, 2, 1), ('333', 3, 3, 3)

        # print(rows)
        rows = db.query((q, p, T), fetchAll=True)
        assert len(rows) == 2
        assert rows[0]['id'] == '321'
        assert rows[1]['id'] == '222'

        # pipesOr
        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.concat((q, p, T), pipes=[
            [
                pipes.any, {
                    'pipes': [
                        [pipes.equals, {'map': {'id': '222'}}],
                        [pipes.equals, {'map': {'id': '333'}}],
                    ]
                }
            ],
            [pipes.order, {'exprs': ['id'], 'orders': ['DESC']}],
        ])
        rows = db.query((q, p, T), fetchAll=True)
        assert len(rows) == 2
        assert rows[0]['id'] == '333'
        assert rows[1]['id'] == '222'

        # pipesAnd
        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.concat((q, p), pipes=[
            [
                pipes.all, {
                    'pipes': [
                        [pipes.equals, {'map': {'a': '1'}}],
                        [pipes.equals, {'map': {'c': '1'}}],
                    ]
                }
            ],
            [pipes.order, {'exprs': ['id'], 'orders': ['DESC']}],
        ])
        rows = db.query((q, p, T), fetchAll=True)
        assert len(rows) == 1
        assert rows[0]['id'] == '111'

        # pipes as list, string and callable
        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.concat((q, p, None), [
            [pipes.member((q, p, T), expr='b', values=['2'])],
            # [pipes.member, {'expr': 'b', 'values':['2']}],
            ['order', {'exprs': ['a', 'b', 'c'],
                       'orders': ['DESC', 'DESC', 'DESC']}],
            [pipes.limit, {'limit': 2, 'offset': 2}]
        ])
        rows = db.query((q, p, T), fetchAll=True)
        assert len(rows) == 1
        assert rows[0]['id'] == '123'

        # aggregate
        db = DB(dbFile)
        db.query(
            'CREATE TABLE strvec3 (id TEXT, a REAL, b REAL, c REAL, PRIMARY KEY (id))')
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('113', 1, 1, 3)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)")
        db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)")
        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.order(
            pipes.aggregate((q, p, T), {
                'sum_a': 'sum(a)',
            }, ['id'], quote=True
            ), ['id']
        )
        rows = db.query((q, p, T), fetchAll=True)
        # print(rows)
        assert len(rows) == 6
        assert rows[0]['id'] == '111'
        assert rows[0]['sum_a'] == 1
        assert rows[5]['id'] == '333'
        assert rows[5]['sum_a'] == 3

        # DB.__DEBUG__ = True
        q = 'SELECT * FROM strvec3'
        p = []
        q, p, T = pipes.order(
            pipes.aggregate((q, p, T), {
                'sum_a': 'sum(a)',
                'sum_c': 'sum(c)',
            }, {
                'subid': "substr(id, 1 ,2)",
                'a': 'a',
            }, quote=True
            ), ['subid']
        )
        rows = db.query((q, p, T), fetchAll=True)
        # print(rows)
        assert len(rows) == 5
        assert rows[0]['subid'] == '11'
        assert rows[0]['a'] == 1
        assert rows[0]['sum_a'] == 2
        assert rows[0]['sum_c'] == 4
        assert rows[4]['subid'] == '33'
        assert rows[4]['a'] == 3
        assert rows[4]['sum_a'] == 3
        assert rows[4]['sum_c'] == 3

        # print('dbFile={}'.format(dbFile))

    def test_schema_queries(self):
        dbFile = ':memory:'
        uFile = ':memory:'
        bFile = ':memory:'
        cFile = ':memory:'
        # tmpSchemaFile = '/tmp/testattached {}.db'.format(str(uuid4))
        db = DB(dbFile, attaches={
            'u': uFile,
            'b': bFile,
            'p': cFile,
        }, extensions=[
            'contrib/sqlite3-pcre/pcre'
        ])

        db.query(
            'CREATE TABLE "u"."user" (id SERIAL PRIMARY KEY, name TEXT, age INTEGER)')
        db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author1\', 31)')
        db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author2\', 32)')
        db.query('INSERT INTO "u"."user" (name, age) VALUES (\'author3\', 33)')
        db.query('CREATE INDEX "u"."name" on "user" (name)')
        db.query('CREATE INDEX "u"."age" on "user" (age)')

        db.query(
            'CREATE TABLE "b".book (id SERIAL PRIMARY KEY, title TEXT, author TEXT)')
        db.query(
            'INSERT INTO "b".book (title, author) VALUES (\'book1\', \'author1\')')
        db.query(
            'INSERT INTO "b".book (title, author) VALUES (\'book2\', \'author2\')')
        db.query(
            'INSERT INTO "b".book (title, author) VALUES (\'book3\', \'author3\')')
        db.query(
            'CREATE TABLE "p"."player" (id SERIAL PRIMARY KEY, name TEXT, position TEXT)')
        db.query(
            'INSERT INTO "p".player (name, position) VALUES (\'player1\', \'lw\')')

        # schema
        rows = db.querySchemas(fetchAll=True)
        self.assertEqual(4, len(rows))
        self.assertIn({'schema': 'u'}, rows)
        self.assertIn({'schema': 'b'}, rows)
        self.assertIn({'schema': 'p'}, rows)
        self.assertIn({'schema': 'main'}, rows)

        # table
        rows = db.queryTables(pathRE='p[.]player|u[.]user', fetchAll=True)
        self.assertEqual(2, len(rows))
        self.assertIn({'schema': 'u', 'table': 'user', 'path': 'u.user'}, rows)
        self.assertIn({'schema': 'p', 'table': 'player',
                      'path': 'p.player'}, rows)

        # column
        rows = db.queryColumns(tableRE='b[.]user', fetchAll=True)
        self.assertEqual(len(rows), 0)

        rows = db.queryColumns(pathRE='u[.]user', fetchAll=True)
        self.assertEqual(len(rows), 3)

        rows = db.queryColumns(pathRE='^u[.].*$|^b[.].*$', fetchAll=True)
        self.assertEqual(len(rows), 6)
        assert {'path': 'b.book.author', 'schema': 'b', 'table': 'book', 'column': 'author',
                'primary_key': False} in rows
        assert {'path': 'b.book.id', 'schema': 'b', 'table': 'book', 'column': 'id',
                'primary_key': True} in rows
        assert {'path': 'b.book.title', 'schema': 'b', 'table': 'book', 'column': 'title',
                'primary_key': False} in rows
        assert {'path': 'u.user.age', 'schema': 'u', 'table': 'user', 'column': 'age',
                'primary_key': False} in rows
        assert {'path': 'u.user.id', 'schema': 'u', 'table': 'user', 'column': 'id',
                'primary_key': True} in rows
        assert {'path': 'u.user.name', 'schema': 'u', 'table': 'user', 'column': 'name',
                'primary_key': False} in rows

        # index
        rows = db.queryIndexes(fetchAll=True)
        self.assertEqual(len(rows), 3 + 2)

        rows = db.queryIndexes(
            pathRE='^u[.]user[.]sqlite_autoindex_user_1$', fetchAll=True)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            {**rows[0], **{'path': 'u.user.sqlite_autoindex_user_1', 'schema': 'u', 'table': 'user',
                           'index': 'sqlite_autoindex_user_1', 'primary_key': True}
             }, rows[0])

        rows = db.queryIndexes(pathRE='^u[.]user[.]age$', fetchAll=True)
        self.assertEqual(len(rows), 1)
        self.assertEqual({**rows[0], **{'path': 'u.user.age', 'schema': 'u', 'table': 'user',
                                        'index': 'age', 'primary_key': False}
                          }, rows[0])

        rows = db.queryIndexes(
            schemaRE='^u$', tableRE='^user$', indexRE='^name', fetchAll=True)
        self.assertEqual(len(rows), 1)
        self.assertEqual({**rows[0], **{'path': 'u.user.name', 'schema': 'u', 'table': 'user',
                                        'index': 'name', 'primary_key': False}
                          }, rows[0])

    def testRowTransform(self):
        dbFile = ':memory:'
        db = DB(dbFile, extensions=[
            'contrib/sqlite3-pcre/pcre'
        ])
        pipes = Pipes()
        orm = ORM(db)

        def rowTransform(row, inverse):

            dc = 1 if inverse else -1
            res = {**row}
            if 'c' in res:
                res['c'] = res['c'] + dc

            if inverse:
                res['isInDB'] = True
            else:
                res.pop('isInDB', None)

            return res

        db.query(
            'CREATE TABLE strvec3 (id TEXT, a DOUBLE PRECISION, b DOUBLE PRECISION, c DOUBLE PRECISION, PRIMARY KEY (id))')
        tableSpec = {
            'name': "strvec3",
            'rowTransform': rowTransform,
            'columnSpecs': {
                'id': {'definition': "TEXT NOT NULL", 'transform': Ts.str, },
                'a': {'definition': "DOUBLE PRECISION", 'transform': Ts.int},
                'b': {'definition': "DOUBLE PRECISION", 'transform': Ts.int, },
                'c': {'definition': "DOUBLE PRECISION", 'transform': Ts.int, },
            },
            'primaryKeys': ["id"]
        }
        assert True == orm.tableExists(tableSpec)
        assert False == orm.ensureTable(tableSpec)

        rows = orm.insert(tableSpec, [
            {'id': '111', 'a': 1, 'b': 1, 'c': 1, 'isInDB': False},
            {'id': '123', 'a': 1, 'b': 2, 'c': 3, 'isInDB': False},
            {'id': '222', 'a': 2, 'b': 2, 'c': 2, 'isInDB': False},
        ], fetchAll=True, returning=['id', 'c'])
        self.assertEqual(3, len(rows))
        rows = {r['id']: r for r in rows}
        self.assertDictEqual(
            {'id': '111', 'c': 1, 'isInDB': True},
            rows['111']
        )
        self.assertDictEqual(
            {'id': '123', 'c': 3, 'isInDB': True},
            rows['123']
        )
        self.assertDictEqual(
            {'id': '222', 'c': 2, 'isInDB': True},
            rows['222']
        )

        rows = orm.upsert(tableSpec, [
            {'id': '222', 'a': 2, 'b': 2, 'c': 2, 'isInDB': False},
            {'id': '321', 'a': 3, 'b': 2, 'c': 1, 'isInDB': False},
            {'id': '333', 'a': 1, 'b': 3, 'c': 3, 'isInDB': False},
        ], fetchAll=True, returning=['id', 'c'])
        self.assertEqual(3, len(rows))
        rows = {r['id']: r for r in rows}
        self.assertDictEqual(
            {'id': '222', 'c': 2, 'isInDB': True},
            rows['222']
        )
        self.assertDictEqual(
            {'id': '321', 'c': 1, 'isInDB': True},
            rows['321']
        )
        self.assertDictEqual(
            {'id': '333', 'c': 3, 'isInDB': True},
            rows['333']
        )
        rows = list(orm.upsert(tableSpec, [
            {'id': '111', 'a': 1, 'b': 1, 'c': 1, 'isInDB': False},
            {'id': '321', 'a': 3, 'b': 2, 'c': 1, 'isInDB': False},
            {'id': '333', 'a': 1, 'b': 3, 'c': 3, 'isInDB': False},
        ], returning=['id', 'c']))

        qpT = orm.select(tableSpec)
        qpT = pipes.member(qpT, 'id', ['333'])
        qpT = pipes.order(qpT, ['id'])
        rows = orm.query(qpT, fetchAll=True, debug=False)
        self.assertEqual(1, len(rows))
        self.assertDictEqual(
            {
                'id': '333', 'a': 1, 'b': 3, 'c': 3, 'isInDB': True},
            rows[0]
        )

        qpT = orm.select(tableSpec)
        qpT = pipes.member(qpT, 'id', ['333'])
        qpT = pipes.order(qpT, ['id'])
        rows = {r['id']: r for r in orm.query(qpT, debug=False)}
        row333 = rows['333']

        orm.update(tableSpec, [row333])
        qpT = orm.select(tableSpec)
        qpT = pipes.member(qpT, 'id', ['333'])
        qpT = pipes.order(qpT, ['id'])
        rows = [r for r in orm.query(qpT, debug=False)]
        self.assertEqual(1, len(rows))
        self.assertEqual(
            row333,
            rows[0]
        )
        orm.upsert(tableSpec, [row333])
        qpT = orm.select(tableSpec)
        qpT = pipes.member(qpT, 'id', ['333'])
        qpT = pipes.order(qpT, ['id'])
        rows = [r for r in orm.query(qpT, debug=False)]
        self.assertEqual(1, len(rows))
        self.assertEqual(
            row333,
            rows[0]
        )

        qpT = orm.select(tableSpec)
        qpT = pipes.order(qpT, ['id'])
        rows = [r for r in orm.query(qpT, debug=False)]
        assert rows[0]['id'] == '111'
        assert rows[0]['a'] == 1
        assert rows[0]['isInDB']
        assert rows[2]['id'] == '222'
        assert rows[2]['a'] == 2
        assert rows[2]['isInDB']
        assert rows[3]['id'] == '321'
        assert rows[3]['a'] == 3
        assert rows[3]['isInDB']

    def test_constant_rows(self):
        dbFile = ':memory:'
        db = DB(dbFile, extensions=[
            'contrib/sqlite3-pcre/pcre'
        ])

        colTypeMap = {
            'TextString': 'text',
            'Float1': 'real',
            'FloatString': lambda v: f'cast({v} as text)'
        }

        expRows = [
            {'TextString': "'text1'", 'Float1': 1.0, 'FloatString': "'1.0'"},
            {'TextString': "'text2'", 'Float1': "'10.0'", 'FloatString': 10.0},
            {'TextString': None, 'Float1': None, 'FloatString': None},
            {'TextString': None, 'FloatString': None},
        ]

        q = db.constantRows(colTypeMap=colTypeMap, rows=expRows)

        actRows = db.query((q,), fetchAll=True)

        self.assertEqual(len(expRows), len(actRows))
        self.assertEqual({
            'TextString': 'text1', 'Float1': 1.0, 'FloatString': '1.0'
        }, actRows[0])
        self.assertEqual({
            'TextString': 'text2', 'Float1': 10.0, 'FloatString': '10.0'
        }, actRows[1])
        self.assertEqual({
            'TextString': None, 'Float1': None, 'FloatString': None
        }, actRows[2])
        self.assertEqual({
            'TextString': None, 'Float1': None, 'FloatString': None
        }, actRows[3])


if __name__ == '__main__':

    unittest.main()
