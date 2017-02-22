from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=create_engine('sqlite:///:memory:', echo=False))
Base = declarative_base()
session = Session()

from dynalchemy import Registry

reg = Registry(Base, session)

reg.add('animal', 'bird', schema='toto', columns=[
    dict(name='name', kind='string', max=200, null=False),
    dict(name='nb_wings', kind='integer', max=200, null=False),
    dict(name='color', kind='string', max=200, choices=['a', 'b', 'c']),
])

Bird = reg.get('animal', 'bird')
print 'all: ', reg.list('animal')
print 'one: ', Bird

session.add(Bird(name='pinson', nb_wings=4, color='lightblue'))
session.commit()
print 'birds', session.query(Bird).all()
pinson = session.query(Bird).filter_by(name='pinson').one()
print 'pinson color', pinson.color

reg.add_column('animal', 'bird', dict(name='food', kind='string', max=200))
session.flush()

print dir(pinson)
pinson = session.query(Bird).filter_by(name='pinson').one()
print dir(pinson)