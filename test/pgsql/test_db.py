import unittest
import testing.postgresql

import tempfile
from collections import OrderedDict

from db.pgsql.db import DB
from db.pgsql.util import Util
from db.ts import Ts
from db.pgsql.orm import ORM
from db.pgsql.pipes import Pipes

class TestDB(unittest.TestCase):

    def test_Util(self):
        #P
        util = Util()
        q = '''
        SELECT %(p1)s, p2
        FROM a as _t
        WHRER %(_p3)s > %(5p)s
        '''
        p = {
            'p1': 1,
            'p2': 2,
            '_p3': 3,
            'p4': 4,
            '5p': 5,
        }
        pStripped = util.pStrip(q, p)
        #print(pStripped)
        self.assertEqual({
            'p1': 1,
            '_p3': 3,
            '5p': 5
        }, pStripped)

        #quote
        col = 'alfa'
        assert util.quote(col) == '"alfa"'
        assert util.quote(col, False) == 'alfa'
        
        cols = ['alfa', 'beta']
        #print('cols', util.quote(cols))
        assert util.quote(cols) == ['"alfa"', '"beta"']
        assert util.quote(cols, False) == ['alfa', 'beta']


        
    def test_schema(self):

        with testing.postgresql.Postgresql() as testpg:        

            #DB
            db = DB(url=testpg.url())
            util = Util()
            db.query('CREATE SCHEMA "t mp"')
            db.query('CREATE TABLE strvec3 (id TEXT, a DOUBLE PRECISION, b DOUBLE PRECISION, c DOUBLE PRECISION, PRIMARY KEY (id))')
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)");

            db.query('CREATE TABLE {} (id TEXT, a DOUBLE PRECISION, b DOUBLE PRECISION, c DOUBLE PRECISION, PRIMARY KEY (id))'.format(util.quote('t mp._tstrvec3')), debug=False)
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('111', 10, 10, 10)".format(util.quote('t mp._tstrvec3')));
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('123', 10, 20, 30)".format(util.quote('t mp._tstrvec3')));
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('222', 20, 20, 20)".format(util.quote('t mp._tstrvec3')));
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('321', 30, 20, 10)".format(util.quote('t mp._tstrvec3')));
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('333', 30, 30, 30)".format(util.quote('t mp._tstrvec3')));

            #attached
            row = db.query("SELECT * FROM {} WHERE id = '321'".format(util.quote('t mp._tstrvec3')));
            self.assertEqual(len(row), 1)
            row = row[0]
            self.assertEqual({
                'id': '321',
                'a': 30,
                'b': 20,
                'c': 10,
            }, row)


    def test_orm(self):
        
        with testing.postgresql.Postgresql() as testpg:        

            #DB
            db = DB(url=testpg.url())
            util = Util()
            pipes = Pipes()
            orm = ORM(db)
            db.query('CREATE SCHEMA "t mp"')
            db.query('CREATE TABLE strvec3 (id TEXT, a DOUBLE PRECISION, b DOUBLE PRECISION, c DOUBLE PRECISION, PRIMARY KEY (id))')
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)");

            db.query('CREATE TABLE {} (id TEXT, a DOUBLE PRECISION, b DOUBLE PRECISION, c DOUBLE PRECISION, PRIMARY KEY (id))'.format(util.quote('t mp._tstrvec3')), debug=False)
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('111', 10, 10, 10)".format(util.quote('t mp._tstrvec3')));
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('123', 10, 20, 30)".format(util.quote('t mp._tstrvec3')));
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('222', 20, 20, 20)".format(util.quote('t mp._tstrvec3')));
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('321', 30, 20, 10)".format(util.quote('t mp._tstrvec3')));
            db.query("INSERT INTO {} (id, a, b, c) VALUES ('333', 30, 30, 30)".format(util.quote('t mp._tstrvec3')));

            ##ORM

            #createTable, dropTable, insert, update
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
                        'transform': lambda n, *_: '_' + Ts.str(n,True), #prefixed by _ twice!
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

            #print(tableSpec['views'])
            orm.createTable(tableSpec)
            #print(tableSpec['columnSpecs'])
            #DB.__DEBUG__ = True
            assert True == orm.tableExists(tableSpec)
            self.assertFalse(orm.ensureTable(tableSpec))
            orm.dropTable(tableSpec)
            assert True == orm.ensureTable(tableSpec)
            assert True == orm.tableExists(tableSpec)
            assert False == orm.ensureTable(tableSpec)
            assert True == orm.tableExists(tableSpec)
            orm.insert(tableSpec, [
                { 'team_id': 't1', 'name': 'n1', 'country': '1', 'verified': True, },
                { 'team_id': 't2', 'name': 'n2', 'country': '2', 'verified': 0, }
            ], debug=False)
            #assert 1 == 0
            rows = db.query(orm.select(tableSpec))
            rows.sort(key=lambda row: row['team_id'])
            assert 'n1' == rows[0]['name']
            assert True == rows[0]['verified']
            assert 'n2' == rows[1]['name']
            assert False == rows[1]['verified']

            rows = db.query(orm.view(tableSpec, view='prefixed'))
            rows.sort(key=lambda row: row['team_id'])
            assert '__n1' == rows[0]['name']
            assert True == rows[0]['verified']
            assert '__n2' == rows[1]['name']
            assert False == rows[1]['verified']

            orm.update(tableSpec, [
                { 'team_id': 't1', 'verified': False, },
                { 'team_id': 't2', 'verified': 1, }
            ])
            rows = db.query(orm.select(tableSpec))
            rows.sort(key=lambda row: row['team_id'])
            assert 'n1' == rows[0]['name']
            assert False == rows[0]['verified']
            assert 'n2' == rows[1]['name']
            assert True == rows[1]['verified']

            orm.dropTable(tableSpec)

            #upsert Todo: Test with composite keys!
            orm.createTable(tableSpec)
            assert True == orm.tableExists(tableSpec)

            orm.upsert(tableSpec, [
                { 'team_id': 't2', 'name': 'n2', 'country': '2', 'verified': 0, 'index': 0}
            ])
            rows = db.query(orm.select(tableSpec))
            rows.sort(key=lambda row: row['team_id'])
            assert len(rows) == 1
            assert 'n2' == rows[0]['name']
            assert False == rows[0]['verified']

            orm.upsert(tableSpec, [
                { 'team_id': 't1', 'name': 'nn1', 'country': '1', 'verified': 0, 'index': 10},
                { 'team_id': 't2', 'name': 'n2', 'country': '2', 'verified': True, 'index': 20}
            ])
            rows = db.query(orm.select(tableSpec))
            rows.sort(key=lambda row: row['team_id'])
            assert len(rows) == 2
            assert 't1' == rows[0]['team_id']
            assert 'nn1' == rows[0]['name']
            assert False == rows[0]['verified']
            assert 't2' == rows[1]['team_id']
            assert 'n2' == rows[1]['name']
            assert True == rows[1]['verified']

            orm.dropTable(tableSpec)

    def test_legacy(self):


        with testing.postgresql.Postgresql() as testpg:        
            db = DB(testpg.url())
            util = Util()
            pipes = Pipes();
            orm = ORM(db);

            db.query('CREATE TABLE strvec3 (id TEXT, a DOUBLE PRECISION, b DOUBLE PRECISION, c DOUBLE PRECISION, PRIMARY KEY (id))')
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)");
            tableSpec = {
                'name': "strvec3",
                'columnSpecs': {
                    'id': { 'definition': "TEXT NOT NULL", 'transform': Ts.str, },
                    'a': { 'definition': "DOUBLE PRECISION" },
                    'b': { 'definition': "DOUBLE PRECISION", 'transform': Ts.str, },
                    'c': { 'definition': "DOUBLE PRECISION", 'transform': Ts.str, },
                },
                'primaryKeys': ["id"]
            }

            assert True == orm.tableExists(tableSpec)
            assert False == orm.ensureTable(tableSpec)

            qpT = orm.select(tableSpec)
            qpT = pipes.order(qpT, ['id'])
            rows = db.query(qpT, debug=False)
            #for row in rows:
            #    print(row)
            assert rows[0]['id'] == '111'
            assert rows[0]['a'] == 1
            assert rows[2]['id'] == '222'
            assert rows[2]['a'] == 2
            assert rows[3]['id'] == '321'
            assert rows[3]['a'] == 3
            ##Pipes
            # equals
            q = 'SELECT * FROM strvec3'
            p = []
            q,p, T = pipes.equals((q, p), map=[
                {'a': 1, 'b': 1, 'c': 1},
                {'a': 3, 'b': 2, 'c': 1},
            ])
            rows = db.query((q,p, T))
            assert len(rows) == 2

            # alias
            q = 'SELECT * FROM strvec3'
            p = []
            T = {
                'id': lambda v, *_: v.uppercase(),
                '_a_': lambda v, *_: v*10,
            }
            qpT = pipes.aliases((q, p, T), aliasExprMap={
                '_id_': 'id',
                '_a_': 'a * 2',
            }, quote=False)
            qpT = pipes.order(qpT, ['_id_'])
            qpT = pipes.limit(qpT, 1)
            rows = db.query(qpT, debug=False)
            assert len(rows) == 1
            assert rows[0]['_id_'] == '111'
            assert rows[0]['_a_'] == 20

            # matches
            q = 'SELECT * FROM strvec3'
            p = []
            q,p, T = pipes.matches((q, p), map=[
                {'id': r'[13].[13]'},
            ])
            qpT = pipes.order(qpT, ['id'])
            rows = db.query((q,p, T), debug=None)
            assert len(rows) == 4
            assert rows[0]['id'] == '111'
            assert rows[1]['id'] == '123'
            assert rows[2]['id'] == '321'
            assert rows[3]['id'] == '333'

            # map, pipes
            q = 'SELECT * FROM strvec3'
            p = []
            qpT = pipes.concat((q, p), pipes=[ 
                [pipes.equals, {'map': {'a': 1, 'b': 1, 'c': 1}}]
            ])
            rows = db.query((qpT))
            assert len(rows) == 1
            assert rows[0]['id'] == '111'

            # pipesLike, pipesPipes
            q = 'SELECT * FROM strvec3'
            p = []
            q,p, T = pipes.concat((q, p), pipes=[ 
                [pipes.like, {'expr': 'id', 'pattern': '%2%'}],
                [pipes.order, {'exprs': ['id'], 'orders':['DESC']}],
            ])
            rows = db.query((q,p, T))
            assert len(rows) == 3
            assert rows[2]['id'] == '123'
            assert rows[1]['id'] == '222'
            assert rows[0]['id'] == '321'

            # pipesValues, order, limit
            q = 'SELECT * FROM strvec3'
            p = []
            q,p, T = pipes.concat((q, p), pipes=[ 
                [pipes.member, {'expr': 'a', 'values':['2', '3']}],
                [pipes.order, {'exprs': ['a', 'b', 'c'], 'orders':['DESC', 'DESC', 'DESC']}],
                [pipes.limit, {'limit': 10}],
            ])
            rows = db.query((q,p, T))
            assert len(rows) == 3
            assert rows[0]['id'] == '333'
            assert rows[1]['id'] == '321'
            assert rows[2]['id'] == '222'

            q = 'SELECT * FROM strvec3'
            p = []
            q,p, T = pipes.concat((q, p), [
                [pipes.member, {'expr': 'a', 'values':['2', '3']}],
                [pipes.order, {'exprs': ['a', 'b', 'c'], 'orders':['DESC', 'DESC', 'DESC']}],
                [pipes.limit, {'limit': 2, 'offset': 1}]
            ])
            #print(rows)
            rows = db.query((q,p, T))
            
            #[id, a , b, c] => ('222', 2, 2, 2), ('321', 3, 2, 1), ('333', 3, 3, 3)
            assert len(rows) == 2
            assert rows[0]['id'] == '321'
            assert rows[1]['id'] == '222'

            # pipesOr
            q = 'SELECT * FROM strvec3'
            p = []
            q,p, T = pipes.concat((q, p, T), pipes=[
                [
                    pipes.any, {
                        'pipes': [
                            [pipes.equals, {'map': {'id': '222'}}],
                            [pipes.equals, {'map': {'id': '333'}}],
                        ]
                    }
                ],
                [pipes.order, {'exprs': ['id'], 'orders':['DESC']}],
            ])
            rows = db.query((q,p, T))
            assert len(rows) == 2
            assert rows[0]['id'] == '333'
            assert rows[1]['id'] == '222'

            # pipesAnd
            q = 'SELECT * FROM strvec3'
            p = []
            q,p, T = pipes.concat((q, p), pipes=[
                [
                    pipes.all, {
                        'pipes': [
                            [pipes.equals, {'map': {'a': '1'}}],
                            [pipes.equals, {'map': {'c': '1'}}],
                        ]
                    }
                ],
                [pipes.order, {'exprs': ['id'], 'orders':['DESC']}],
            ])
            rows = db.query((q,p, T))
            assert len(rows) == 1
            assert rows[0]['id'] == '111'


            #pipes as list, string and callable
            q = 'SELECT * FROM strvec3'
            p = []
            q,p,T = pipes.concat((q, p, None), [
                [pipes.member((q, p, T), expr= 'b', values=['2'])],
                #[pipes.member, {'expr': 'b', 'values':['2']}],
                ['order', {'exprs': ['a', 'b', 'c'], 'orders':['DESC', 'DESC', 'DESC']}],
                [pipes.limit, {'limit': 2, 'offset': 2}]
            ])
            rows = db.query((q,p,T))
            assert len(rows) == 1
            assert rows[0]['id'] == '123'


        #aggregate
        with testing.postgresql.Postgresql() as testpg:        
            db = DB(testpg.url())
            util = Util()
            pipes = Pipes();
            orm = ORM(db);

            db.query('CREATE TABLE strvec3 (id TEXT, a DOUBLE PRECISION, b DOUBLE PRECISION, c DOUBLE PRECISION, PRIMARY KEY (id))')
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('111', 1, 1, 1)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('113', 1, 1, 3)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('123', 1, 2, 3)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('222', 2, 2, 2)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('321', 3, 2, 1)");
            db.query("INSERT INTO strvec3 (id, a, b, c) VALUES ('333', 3, 3, 3)");
            q = 'SELECT * FROM strvec3'
            p = []
            q,p,T = pipes.order(
                pipes.aggregate((q, p,T), {
                    'sum_a': 'sum(a)',
                }, ['id'], quote=True
                ), ['id']
            )
            rows = db.query((q,p, T))
            #print(rows)
            assert len(rows) == 6
            assert rows[0]['id'] == '111'
            assert rows[0]['sum_a'] == 1
            assert rows[5]['id'] == '333'
            assert rows[5]['sum_a'] == 3

            #DB.__DEBUG__ = True
            q = 'SELECT * FROM strvec3'
            p = []
            q,p,T = pipes.order(
                pipes.aggregate((q, p,T), {
                    'sum_a': 'sum(a)',
                    'sum_c': 'sum(c)',
                }, {
                    'subid': "substr(id, 1 ,2)",
                    'a': 'a',
                }, quote=True
                ), ['subid']
            )
            rows = db.query((q,p,T))
            #print(rows)
            assert len(rows) == 5
            assert rows[0]['subid'] == '11'
            assert rows[0]['a'] == 1
            assert rows[0]['sum_a'] == 2
            assert rows[0]['sum_c'] == 4
            assert rows[4]['subid'] == '33'
            assert rows[4]['a'] == 3
            assert rows[4]['sum_a'] == 3
            assert rows[4]['sum_c'] == 3

        

if __name__ == '__main__':

    unittest.main()
