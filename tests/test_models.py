from sqlalchemy import Integer, String, Enum, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dynalchemy import Registry

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

    def test_get_name_fk(self):

        col = DColumn(name='rel', kind='Integer',
            parent_relationship={'collection': 'animal'})
        self.assertEqual(col.get_name(), 'rel__fk')

    def test_get_name(self):

        col = DColumn(name='rel', kind='Integer')
        self.assertEqual(col.get_name(), 'rel')

    def test_is_relationship(self):

        col = DColumn(name='rel', kind='Integer',
            parent_relationship={'collection': 'animal'})
        self.assertTrue(col.is_relationship())

    def test_get_sa_relationship(self):

        engine = create_engine('sqlite:///:memory:', echo=False)
        base = declarative_base(bind=engine)
        reg = Registry(base, sessionmaker(bind=engine)())

        Bird = reg.add('animal', 'bird')
        rel = DColumn(
            name='rel', kind='Integer',
            parent_relationship={'collection': 'animal', 'name': 'bird', 'backref': 'birds'})\
            .get_sa_relationship(reg)
        self.assertEqual(rel.backref, 'birds')


class TestDTable(unittest.TestCase):

    def test_relationship_parent(self):

        engine = create_engine('sqlite:///:memory:', echo=False)
        base = declarative_base(bind=engine)
        session = sessionmaker(bind=engine)()
        reg = Registry(base, session)

        Bird = reg.add('animal', 'bird', columns=[
            dict(name='name', kind='String'),
            dict(name='nb_wings', kind='Integer'),
            dict(name='color', kind='String')
        ])
        Food = reg.add(
            'food', 'food',
            columns=[
                dict(name='name', kind='String'),
                dict(
                    name='predator',
                    kind='Integer',
                    parent_relationship={
                        'collection': 'animal', 'name': 'bird', 'backref': 'predators'}
            )]
        )
        pinson = Bird(name='pinson', color='red')
        session.add(pinson)
        session.commit()
        corn = Food(name='corn', predator=pinson)
        # or corn = Food(bird__fk=pinson.id)
        # session.add(corn)
        session.commit()
        session.expunge_all()

        corn = session.query(Food).filter_by(name='corn').one()
        self.assertEqual(corn.predator.name, 'pinson')


if __name__ == '__main__':
    unittest.main()


'''    def test_to_sa(self):

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