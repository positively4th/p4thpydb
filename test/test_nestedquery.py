import re
import unittest

from db.nestedquery import NestedQuery
from db.util import Util

stripRE = re.compile(r'[\r\n\t\s]')

next0 = Util.nextUniq
Util.nextUniq = lambda suffix, prefix, *args, **kwargs: next0('', prefix, sep='_', caster=str)


class TestNestedQuery(unittest.TestCase):

    def test_d1w1_sub_all(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<=q0>:'
        qs = {
            'q0': 'select col from :<=q1>:',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = 'select * from (select col from (select col from q2) _q1) _q0'
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))

        q = 'select * from :<=q0>: _q0'
        qs = {
            'q0': 'select col from :<=q1>: _q1',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = 'select * from (select col from (select col from q2) _q1) _q0'
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))

    def test_d1w1_sub_child(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<q0>:'
        qs = {
            'q0': 'select col from :<=q1>:',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = 'WITH' + ',\n'.join([
            '"q0_0" AS ({})'.format(qs['q0']).replace(':<=q1>:', '(' + qs['q1'] + ') _q1'),
        ]) + '{}'.format(q).replace(':<q0>:', '"q0_0"')
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))


        Util.uniqifyCtr = -1
        q = 'select * from :<q0>:'
        qs = {
            'q0': 'select col from :<=q1>: _q1',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = 'WITH' + ',\n'.join([
            '"q0_0" AS ({})'.format(qs['q0']).replace(':<=q1>:', '(' + qs['q1'] + ')'),
        ]) + '{}'.format(q).replace(':<q0>:', '"q0_0"')
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))

    def test_d1w1_sub_parent(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<=q0>:'
        qs = {
            'q0': 'select col from :<q1>:',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = 'WITH' + ',\n'.join([
            '"q1_0" AS ({})'.format(qs['q1']),
        ]) + '{}'.format(q)\
            .replace(':<=q0>:', '(' + qs['q0'] + ') _q0')\
            .replace(':<q1>:', '"q1_0"')
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))

    def test_d1w1_cte(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<q0>:'
        qs = {
            'q0': 'select col from :<q1>:',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = 'WITH' + ',\n'.join([
            '"q1_0" AS ({})'.format(qs['q1']),
            '"q0_1" AS ({})'.format(qs['q0']).replace(':<q1>:', '"q1_0"'),
        ]) + '{}'.format(q).replace(':<q0>:', '"q0_1"')
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))

    def test_d1w1_temp(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<q0>:'
        qs = {
            'q0': 'select col from :<q1>:',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildTempQueries(q, qs)
        exp0 = '''
        CREATE TEMP TABLE "q1_0" AS select col from q2;
        -- next query
        CREATE TEMP TABLE "q0_1" AS select col from "q1_0";
        -- next query
        DROP TABLE "q1_0"
        '''
        exp1 = '''
        select * from "q0_1"
        '''
        exp2 = '''
        DROP TABLE "q0_1"
        '''
        self.assertEqual(stripRE.sub('', exp0), stripRE.sub('', act[0]))
        self.assertEqual(stripRE.sub('', exp1), stripRE.sub('', act[1]))
        self.assertEqual(stripRE.sub('', exp2), stripRE.sub('', act[2]))

    def test_d1w1_temp_materialize(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<+q0>:'
        qs = {
            'q0': 'select col from :<-q1>:',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildTempQueries(q, qs)
        exp0 = '''
        CREATE TEMP TABLE "q1_0" AS select col from q2;
        -- next query
        CREATE TEMP TABLE "q0_1" AS select col from "q1_0";
        -- next query
        DROP TABLE "q1_0"
        '''
        exp1 = '''
        select * from "q0_1"
        '''
        exp2 = '''
        DROP TABLE "q0_1"
        '''
        self.assertEqual(stripRE.sub('', exp0), stripRE.sub('', act[0]))
        self.assertEqual(stripRE.sub('', exp1), stripRE.sub('', act[1]))
        self.assertEqual(stripRE.sub('', exp2), stripRE.sub('', act[2]))

    def test_materizlize_d1w1(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<+q0>:'
        qs = {
            'q0': 'select col from :<-q1>:',
            'q1': 'select col from q2',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = 'WITH' + ',\n'.join([
            '"q1_0" AS NOT MATERIALIZED ({})'.format(qs['q1']),
            '"q0_1" AS MATERIALIZED ({})'.format(qs['q0']).replace(':<-q1>:', '"q1_0"'),
        ]) + '{}'.format(q).replace(':<+q0>:', '"q0_1"')
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))


    def test_square_nested(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<a>:'
        qs = {
            'a': 'select 1 + _b.v + _c.v as v from :<b>: _b, :<c>: _c',
            'b': 'select _d.v as v from :<d>: _d',
            'c': 'select _d.v as v from :<d>: _d',
            'd': 'select 4 as v',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = '''
        WITH "d_0" AS(select 4 as v),
            "b_1" AS(select _d.v as v from "d_0" _d),
            "c_2" AS(select _d.v as v from "d_0" _d),
            "a_3" AS(select 1 + _b.v + _c.v as v from "b_1" _b, "c_2" _c)
        select * from "a_3"
        '''
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))

    def test_square_nested_materialized(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<a>:'
        qs = {
            'a': 'select 1 + _b.v + _c.v as v from :<b>: _b, :<c>: _c',
            'b': 'select _d.v as v from :<+d>: _d',
            'c': 'select _d.v as v from :<-d>: _d',
            'd': 'select 4 as v',
        }

        act = NestedQuery.buildCTE(q, qs)
        exp = '''
        WITH "d_0" AS MATERIALIZED (select 4 as v),
        "d_1" AS NOT MATERIALIZED (select 4 as v),
        "b_2" AS(select _d.v as v from "d_0" _d),
        "c_3" AS(select _d.v as v from "d_1" _d),
        "a_4" AS(select 1 + _b.v + _c.v as v from "b_2" _b, "c_3" _c)
        select * from "a_4"        '''
        self.assertEqual(stripRE.sub('', exp), stripRE.sub('', act))


    def test_square_temp_materialized(self):
        Util.uniqifyCtr = -1

        q = 'select * from :<a>:'
        qs = {
            'a': 'select 1 + _b.v + _c.v as v from :<b>: _b, :<c>: _c',
            'b': 'select _d.v as v from :<+d>: _d',
            'c': 'select _d.v as v from :<-d>: _d',
            'd': 'select 4 as v',
        }

        act = NestedQuery.buildTempQueries(q, qs)
        exp0 = '''
        CREATE TEMP TABLE "d_0" AS select 4 as v;
        -- next query
        CREATE TEMP TABLE "b_1" AS select _d.v as v from "d_0" _d;
        -- next query
        CREATE TEMP TABLE "c_2" AS select _d.v as v from "d_0" _d;
        -- next query
        DROP TABLE "d_0";
        -- next query
        CREATE TEMP TABLE "a_3" AS select 1 + _b.v + _c.v as v from "b_1" _b, "c_2" _c;
        -- next query
        DROP TABLE "c_2";
        -- next query
        DROP TABLE "b_1"
        '''
        exp1 = '''
        select * from "a_3"
        '''
        exp2 = '''
        DROP TABLE "a_3"
        '''
        self.assertEqual(stripRE.sub('', exp0), stripRE.sub('', act[0]))
        self.assertEqual(stripRE.sub('', exp1), stripRE.sub('', act[1]))
        self.assertEqual(stripRE.sub('', exp2), stripRE.sub('', act[2]))

if __name__ == '__main__':
    unittest.main()
