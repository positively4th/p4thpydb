import unittest
import ramda as R


class TestDiffer(unittest.TestCase):

    def test_legacy(self):
        import tempfile
        from uuid import uuid4
        from os import path

        from db.sqlite.differ import Differ
        from db.sqlite.differ import QueryRunner
        from db.sqlite.differ import QueryFactory
        from db.sqlite.db import DB

        DB.__DEBUG__ = False

        dbFile = ':memory:'
        db = DB(dbFile, attaches={
            # 'tmp': tmpSchemaFile,
        })

        db.query(
            'CREATE TABLE user (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)')
        db.query('INSERT INTO user (name, age) VALUES ("author1", 31)')
        db.query('INSERT INTO user (name, age) VALUES ("author2", 32)')
        db.query('INSERT INTO user (name, age) VALUES ("author3", 33)')
        db.query(
            'CREATE TABLE book (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, author TEXT)')
        db.query('INSERT INTO book (title, author) VALUES ("book1", "author1")')
        db.query('INSERT INTO book (title, author) VALUES ("book2", "author2")')
        db.query('INSERT INTO book (title, author) VALUES ("book3", "author3")')

        queryRunner = QueryRunner(db)
        queryFactory = QueryFactory()

        # SqliteQueryFactory: columnsQuery
        flatTableColumnRows = queryRunner.run(queryFactory.columnsQuery())
        # print(flatTableColumnRows)
        assert len(flatTableColumnRows) == 6
        assert {'table': 'main.book', 'column': 'author',
                'isPrimaryKey': 0} in flatTableColumnRows
        assert flatTableColumnRows[1] == {
            'table': 'main.book', 'column': 'id', 'isPrimaryKey': 1}
        assert flatTableColumnRows[2] == {
            'table': 'main.book', 'column': 'title', 'isPrimaryKey': 0}
        assert flatTableColumnRows[3] == {
            'table': 'main.user', 'column': 'age', 'isPrimaryKey': 0}
        assert flatTableColumnRows[4] == {
            'table': 'main.user', 'column': 'id', 'isPrimaryKey': 1}
        assert flatTableColumnRows[5] == {
            'table': 'main.user', 'column': 'name', 'isPrimaryKey': 0}

        allTables = R.pipe(
            R.pluck('table'),
            R.uniq,
        )(flatTableColumnRows)
        # print(allTables)
        self.assertSetEqual(set(allTables), set(['main.book', 'main.user']))

        # SqliteQueryRunner: exportToFile
        dumpPath = path.join(tempfile.gettempdir(), str(uuid4()))
        restorePath = path.join(tempfile.gettempdir(), str(uuid4()))
        db.exportToFile(dumpPath)
        restoreDB = DB(restorePath)
        queryRunnerRestore = QueryRunner(restoreDB)
        restoreDB.exportToFile(dumpPath, invert=True)
        booksRestore = queryRunnerRestore.run('SELECT * FROM book ORDER BY id')
        books = queryRunner.run('SELECT * FROM book ORDER BY id')
        assert booksRestore == books
        usersRestore = queryRunnerRestore.run('SELECT * FROM user ORDER BY id')
        users = queryRunner.run('SELECT * FROM user ORDER BY id')
        assert usersRestore == users

        # SqliteQueryFactory: cloneTables
        # tables = ['book', 'user']
        # qps = queryFactory.cloneTables(tables)
        # nameMap = dict(zip(tables, qps.keys()))
        # print(nameMap)
        # queryRunner.run(qps)
        # booksClone = queryRunner.run('SELECT * FROM "{}" ORDER BY id'
        #                             .format(nameMap['book']));
        # books = queryRunner.run('SELECT * FROM book ORDER BY id');
        # assert booksClone == books
        # usersClone = queryRunner.run('SELECT * FROM "{}" ORDER BY id'
        #                             .format(nameMap['user']));
        # users = queryRunner.run('SELECT * FROM user ORDER BY id');
        # assert usersClone == users

        # DBDiff
        dbFile = ':memory:'
        db = DB(dbFile, attaches={
            # 'tmp': tmpSchemaFile,
        })

        db.query(
            'CREATE TABLE user (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)')
        db.query('INSERT INTO user (name, age) VALUES ("author1", 31)')
        db.query('INSERT INTO user (name, age) VALUES ("author2", 32)')
        db.query('INSERT INTO user (name, age) VALUES ("author3", 33)')
        db.query(
            'CREATE TABLE book (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, author TEXT)')
        db.query('INSERT INTO book (title, author) VALUES ("book1", "author1")')
        db.query('INSERT INTO book (title, author) VALUES ("book2", "author2")')
        db.query('INSERT INTO book (title, author) VALUES ("book3", "author3")')

        # queryRunner = QueryRunner(db);
        # queryFactory = QueryFactory(db);
        differ = Differ(db)

        # DBDiff: differ - no diff
        spec = differ.prepare()
        # print(spec)

        assert 'tables' in spec
        assert len(spec['tables']) == 2

        assert 'columns' in spec
        assert len(spec['columns']) == 6
        assert {'table': 'main.book', 'column': 'author',
                'isPrimaryKey': 0} in spec['columns']
        assert {'table': 'main.book', 'column': 'id',
                'isPrimaryKey': 1} in spec['columns']
        assert {'table': 'main.book', 'column': 'title',
                'isPrimaryKey': 0} in spec['columns']
        assert {'table': 'main.user', 'column': 'age',
                'isPrimaryKey': 0} in spec['columns']
        assert {'table': 'main.user', 'column': 'id',
                'isPrimaryKey': 1} in spec['columns']
        assert {'table': 'main.user', 'column': 'name',
                'isPrimaryKey': 0} in spec['columns']

        assert 'tableCloneMap' in spec
        assert len(spec['tableCloneMap']) == 2
        # print('tableCloneMap', spec['tableCloneMap'])
        assert 'main_book' in spec['tableCloneMap']['main.book']
        assert 'main_user' in spec['tableCloneMap']['main.user']

        differ.diff(spec)
        # print(spec)
        assert 'tableDiffMap' in spec
        assert 'main.user' in spec['tableDiffMap']
        assert 'deleted' in spec['tableDiffMap']['main.user']
        assert len(spec['tableDiffMap']['main.user']['deleted']) == 0
        assert 'main.book' in spec['tableDiffMap']
        assert 'deleted' in spec['tableDiffMap']['main.book']
        assert len(spec['tableDiffMap']['main.book']['deleted']) == 0
        differ.finalize(spec)

        # DBDiff: differ - All diffs
        spec = differ.prepare(spec={})
        db.query('DELETE FROM user WHERE id = 2')
        db.query('DELETE FROM user WHERE id = 3')
        db.query("UPDATE user SET name = 'author1_1' WHERE id = 1")
        db.query('INSERT INTO user (name, age) VALUES ("author4", 34)')

        db.query("UPDATE book SET title = 'book1_1' WHERE id = 1")
        db.query("UPDATE book SET title = 'book1_2' WHERE id = 1")
        db.query("UPDATE book SET title = 'book1_3' WHERE id = 1")
        db.query('DELETE FROM book WHERE id = 1')
        db.query("UPDATE book SET title = 'book2_2' WHERE id = 2")
        db.query(
            "UPDATE book SET title = 'book3_3', author = 'author3_3' WHERE id = 3")
        db.query('INSERT INTO book (title, author) VALUES ("book4", "author4")')
        db.query('INSERT INTO book (title, author) VALUES ("book5", "author5")')

        differ.diff(spec)
        # print(spec)
        assert 'primaryKeys' in spec
        assert spec['primaryKeys'] == ['id']

        assert 'tableDiffMap' in spec
        assert 'main.user' in spec['tableDiffMap']

        assert 'deleted' in spec['tableDiffMap']['main.user']
        assert len(spec['tableDiffMap']['main.user']['deleted']) == 2
        assert spec['tableDiffMap']['main.user']['deleted'][0] == {
            'id': 2, 'name': 'author2', 'age': 32}
        assert spec['tableDiffMap']['main.user']['deleted'][1] == {
            'id': 3, 'name': 'author3', 'age': 33}

        assert 'changed' in spec['tableDiffMap']['main.user']
        assert len(spec['tableDiffMap']['main.user']['changed']) == 1
        assert spec['tableDiffMap']['main.user']['changed'][0] == {
            'id': 1, 'name': 'author1_1', 'age': 31}

        assert 'created' in spec['tableDiffMap']['main.user']
        assert len(spec['tableDiffMap']['main.user']['created']) == 1
        assert spec['tableDiffMap']['main.user']['created'][0] == {
            'id': 4, 'name': 'author4', 'age': 34}

        assert 'main.book' in spec['tableDiffMap']
        assert 'deleted' in spec['tableDiffMap']['main.book']
        assert len(spec['tableDiffMap']['main.book']['deleted']) == 1
        assert spec['tableDiffMap']['main.book']['deleted'][0] == {
            'id': 1, 'title': 'book1_3', 'author': 'author1'}

        assert 'changed' in spec['tableDiffMap']['main.book']
        assert len(spec['tableDiffMap']['main.book']['changed']) == 2
        assert spec['tableDiffMap']['main.book']['changed'][0] == {
            'id': 2, 'title': 'book2_2', 'author': 'author2'}
        assert spec['tableDiffMap']['main.book']['changed'][1] == {
            'id': 3, 'title': 'book3_3', 'author': 'author3_3'}

        assert 'created' in spec['tableDiffMap']['main.book']
        assert len(spec['tableDiffMap']['main.book']['created']) == 2
        assert spec['tableDiffMap']['main.book']['created'][0] == {
            'id': 4, 'title': 'book4', 'author': 'author4'}
        assert spec['tableDiffMap']['main.book']['created'][1] == {
            'id': 5, 'title': 'book5', 'author': 'author5'}


if __name__ == '__main__':
    unittest.main()
