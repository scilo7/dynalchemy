# dynalchemy
dynamic table creation in python apps


API
```python
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dynalchemy import Registry

engine = create_engine('sqlite:///:memory:', echo=True)
base = declarative_base(bind=engine)
session = sessionmaker(bind=engine)()
reg = Registry(base, session)

# STEP 1: declare tables
# Create table & model bird in collection animal
reg.add('animal', 'bird', columns=[
    dict(name='name', kind='String'),
    dict(name='nb_wings', kind='Integer'),
    dict(name='color', kind='String')
])

# Create table and model Seed in model food
# Declare a many to many relation on birds as predators
reg.add(
    'food', 'seed',
    columns=[
        dict(name='name', kind='String'),
        dict(
            name='predators',
            kind='Relation',
            relation={
                'collection': 'animal',
                'name': 'bird',
                'backref': 'foods',
                'type': 'many'}
    )]
)

# STEP 2: Use them !
Bird = reg.get('animal', 'bird')
Seed = reg.get('food', 'seed')
pinson = Bird(name='pinson', color='red')
merle = Bird(name='merle', color='black')
corn = Seed(name='corn', predators=[pinson, merle])
session.add(corn)
session.commit()
```
