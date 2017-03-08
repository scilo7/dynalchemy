from sqlalchemy import Integer, String, Enum, Float
import unittest
import sqlalchemy

from dynalchemy.models import DTable, DColumn


class TestDColumn(unittest.TestCase):

    def test_get_basic_type(self):

        klass = DColumn(kind='Integer')._get_type()
        self.assertEqual(klass, Integer)

    def test_get_complex_type(self):

        kind = DColumn(kind='String', length=25)._get_type()
        self.assertEqual(kind.__class__, String)
        self.assertEqual(kind.length, 25)

    def test_get_type_enum(self):

        kind = DColumn(kind='Enum', choices=['a', 'b', 'c'])._get_type()
        self.assertEqual(kind.__class__, Enum)

    def test_get_default_int(self):

        col = DColumn(name='bob', kind='Integer', default='100')
        self.assertEqual(col._get_default(), 100)

        col = DColumn(name='bob', kind='Integer', default='-100')
        self.assertEqual(col._get_default(), -100)

    def test_get_default_bool(self):

        col = DColumn(kind='Boolean', default='1')
        self.assertEqual(col._get_default(), True)

        col = DColumn(kind='Boolean', default='true')
        self.assertEqual(col._get_default(), True)

        col = DColumn(kind='Boolean', default='0')
        self.assertEqual(col._get_default(), False)

    def test_get_default_float(self):

        col = DColumn(kind='Float', default='1')
        self.assertEqual(col._get_default(), 1.0)

        col = DColumn(kind='Float', default='-1.0001')
        self.assertEqual(col._get_default(), -1.0001)

    def test_get_args(self):

        args = DColumn(kind='Float', default='1')._get_args()
        self.assertEqual(args, {'default': 1.0})

    def test_get_serialization_fields(self):
        fields = DColumn.get_serialization_fields('String')
        self.assertEqual(len(fields), 5)
        self.assertEqual(fields[0]['name'], 'name')
        self.assertEqual(fields[1]['name'], 'kind')

    def test_serialize(self):

        col = DColumn(name='bob', kind='Float', default='1')
        fields = col.serialize()
        self.assertEqual(len(fields), 5)
        self.assertEqual(fields[0]['name'], 'name')
        self.assertEqual(fields[0]['value'], 'bob')

    def test_to_sa(self):

        col = DColumn(name='bob', kind='Float', default='1').to_sa()
        self.assertEqual(col.name, 'bob')
        #self.assertEqual(col.type, sqlalchemy.sql.sqltypes.Float)


'''class TestDColumn(unittest.TestCase):

    def test_to_sa(self):

        klass = self.reg._create(
            DTable(
                collection='animal',
                name='bird',
                columns=[
                    DColumn(name='one', kind='String'),
                    DColumn(name='two', kind='Integer')
                ]
            )
        )
        self.assertEqual(klass.__tablename__, 'animal__bird')
        self.assertTrue(hasattr(klass, 'id'))
        self.assertTrue(hasattr(klass, 'one'))'''


if __name__ == '__main__':
    unittest.main()
