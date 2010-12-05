
import getty
import sqlalchemy as sa

from base import ModelBase

class Config(ModelBase):
    __tablename__ = 'config'

    # mapped attributes
    name = sa.Column(sa.String, primary_key = True)
    value = sa.Column(sa.String, nullable = False)

    def __init__(self, name, value):
        self.name = name
        self.value = value

