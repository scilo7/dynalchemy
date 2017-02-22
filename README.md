# dynalchemy
dynamic table creation in python apps


API
```python
from dynalchemy import Registry

registry = Registry(Base, session)

registry.add('animal', 'bird', schema='toto', columns=[
        dict(name='nb_wings', type='int', max=200, null=False),
        dict(name='wing_color', type='str', max=200, choices=['a', 'b', 'c']),
    ],
    relations=[]
)

klass = registry.get('animal', 'bird')
registry.deprecate(klass.wings)
registry.deprecate(klass)
registry.add_column(klass,
    dict(name='nb_wings', type='int', max=200, null=False))
```
