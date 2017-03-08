from sqlalchemy import Integer, String, Enum
import unittest

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
        print fields
        self.assertEqual(len(fields), 5)
        self.assertEqual(fields[0]['name'], 'name')



if __name__ == '__main__':
    unittest.main()
