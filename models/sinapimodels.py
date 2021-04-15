from sqlalchemy import (Column, Float, ForeignKey, Integer, Sequence, String,
                        Table, Boolean)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint, Index
from sqlalchemy.sql import func

from . import Base, session


class Composition(Base):
    __tablename__ = 'compositions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    code = Column(String(16), nullable=False, unique=True)
    units = Column(String(8), nullable=False)

    group_id = Column(Integer, ForeignKey('groups.id'), nullable=False)
    group = relationship('Group', back_populates='compositions', uselist=False)

    steps = relationship(
        'Step',
        cascade="all, delete, delete-orphan",
        back_populates='composition'
    )

    def __init__(self, name: str, code: str, units: str, group: 'Group'):
        self.name = name
        self.code = code
        self.units = units

        self.group = group

        session.add(self)

    def __repr__(self):
        return f"<Composition(code={self.code})>"

    @property
    def direct_resources_attributions(self) -> 'Attribution':
        '''  Insumos diretos, não provenientes de composições auxiliares '''
        atts = session.query(Step.attributions).filter(
            Step.composition_id == self.id,
            Step.direct == True
        ).all()

        return atts

    @property
    def grouped_attributions(self):
        attr = session.query(
            Attribution,
            func.sum(Step.coefficient * Attribution.coefficient).label('sum_coefficient')
        ) \
            .filter(Attribution.step_id.in_([step.id for step in self.steps])) \
            .filter(Step.id == Attribution.step_id) \
            .group_by(Attribution.resource_id) \
            .all()

        return attr

    @property
    def cost(self):
        cost = sum([step.cost * step.coefficient for step in self.steps])

        return cost


class Step(Base):
    __tablename__ = 'steps'

    id = Column(Integer, primary_key=True, autoincrement=True)
    coefficient = Column(Float, nullable=False)
    name = Column(String, nullable=False)
    direct = Column(Boolean, nullable=False, default=False)
    composition_id = Column(Integer, ForeignKey('compositions.id'), nullable=False)

    attributions = relationship(
        'Attribution', back_populates='step',
        cascade="all, delete, delete-orphan",
        foreign_keys="[Attribution.step_id]"
    )
    resources = association_proxy("attributions", "resource")

    composition = relationship('Composition', back_populates='steps', uselist=False)

    def __init__(self, name: str, coefficient: float):
        self.name = name
        self.coefficient = coefficient

    def __repr__(self):
        return f"<Step(name={self.name}, coefficient={self.coefficient})>"

    @property
    def cost(self):
        cost = sum([attribution.coefficient * attribution.resource.cost for attribution in self.attributions])

        return cost


class Attribution(Base):
    __tablename__ = 'attributions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    resource_id = Column(Integer, ForeignKey('resources.id'))
    step_id = Column(Integer, ForeignKey('steps.id'))
    coefficient = Column(Float, nullable=False)

    step = relationship("Step", back_populates='attributions', uselist=False)

    resource = relationship("Resource", back_populates='attributions', uselist=False)

    def __init__(self, resource: "Resource", coefficient: float):
        self.resource = resource
        self.coefficient = coefficient

    def __repr__(self):
        return f"<Attribution(resource={self.resource.code}, coefficient={self.coefficient})>"


class Resource(Base):
    __tablename__ = 'resources'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    code = Column(String(16), nullable=False, unique=True)
    units = Column(String(8), nullable=False)
    cost = Column(Float(precision=2), nullable=False)

    attributions = relationship('Attribution', back_populates='resource')

    compositions = association_proxy('attributions', 'step')

    impact = relationship('Impact', back_populates='resource', uselist=False)

    process = relationship('Process', back_populates='resource', uselist=False)

    def __init__(self, name: str, code: str, units: str, cost: float):
        self.name = name
        self.code = code
        self.units = units
        self.cost = cost

        session.add(self)

    def __repr__(self):
        return f"<Resource(code={self.code}, cost={self.cost})>"


class Group(Base):
    __tablename__ = 'groups'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    code = Column(String(16), nullable=False, unique=True)
    parent_id = Column(Integer, ForeignKey("groups.id"))

    parent = relationship("Group", back_populates='children', uselist=False, remote_side=[id])
    children = relationship("Group", back_populates='parent')

    compositions = relationship('Composition', back_populates='group')

    def __init__(self, name: str, code: str):
        self.name = name
        self.code = code
        self.__query__()
        session.add(self)

    @classmethod
    def __query__(cls):
        cls.query = session.query(cls)


class Impact(Base):
    __tablename__ = 'impacts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    units = Column(String, nullable=True)
    ozone_formation_terrestrial_ecosystems = Column(Float, nullable=False)
    water_consumption = Column(Float, nullable=False)
    marine_eutrophication = Column(Float, nullable=False)
    marine_ecotoxicity = Column(Float, nullable=False)
    land_use = Column(Float, nullable=False)
    fine_particulate_matter_formation = Column(Float, nullable=False)
    mineral_resource_scarcity = Column(Float, nullable=False)
    ionizing_radiation = Column(Float, nullable=False)
    human_non_carcinogenic_toxicity = Column(Float, nullable=False)
    freshwater_eutrophication = Column(Float, nullable=False)
    terrestrial_acidification = Column(Float, nullable=False)
    fossil_resource_scarcity = Column(Float, nullable=False)
    global_warming = Column(Float, nullable=False)
    stratospheric_ozone_depletion = Column(Float, nullable=False)
    terresetrial_ecotoxicity = Column(Float, nullable=False)
    human_carcinogenic_toxicity = Column(Float, nullable=False)
    freshwater_ecotoxicity = Column(Float, nullable=False)
    ozone_formation_human_health = Column(Float, nullable=False)

    resource_id = Column(Integer, ForeignKey("resources.id"))

    resource = relationship("Resource", back_populates='impact', uselist=False)

    def __init__(self, resource, units, ozone_formation_terrestrial_ecosystems, water_consumption,
                 marine_eutrophication, marine_ecotoxicity, land_use,
                 fine_particulate_matter_formation, mineral_resource_scarcity,
                 ionizing_radiation, human_non_carcinogenic_toxicity, freshwater_eutrophication,
                 terrestrial_acidification, fossil_resource_scarcity, global_warming,
                 stratospheric_ozone_depletion, terresetrial_ecotoxicity,
                 human_carcinogenic_toxicity, freshwater_ecotoxicity, ozone_formation_human_health):

        self.resource = resource
        self.units = units
        self.ozone_formation_terrestrial_ecosystems = ozone_formation_terrestrial_ecosystems
        self.water_consumption = water_consumption
        self.marine_eutrophication = marine_eutrophication
        self.marine_ecotoxicity = marine_ecotoxicity
        self.land_use = land_use
        self.fine_particulate_matter_formation = fine_particulate_matter_formation
        self.mineral_resource_scarcity = mineral_resource_scarcity
        self.ionizing_radiation = ionizing_radiation
        self.human_non_carcinogenic_toxicity = human_non_carcinogenic_toxicity
        self.freshwater_eutrophication = freshwater_eutrophication
        self.terrestrial_acidification = terrestrial_acidification
        self.fossil_resource_scarcity = fossil_resource_scarcity
        self.global_warming = global_warming
        self.stratospheric_ozone_depletion = stratospheric_ozone_depletion
        self.terresetrial_ecotoxicity = terresetrial_ecotoxicity
        self.human_carcinogenic_toxicity = human_carcinogenic_toxicity
        self.freshwater_ecotoxicity = freshwater_ecotoxicity
        self.ozone_formation_human_health = ozone_formation_human_health


class Process(Base):
    __tablename__ = 'processess'

    id = Column(Integer, primary_key=True, autoincrement=True)

    units = Column(String, nullable=True)
    flow = Column(String, nullable=False)
    category = Column(String, nullable=False)
    uncertainty = Column(String, nullable=True)
    avoided_waste = Column(String, nullable=True)
    provider = Column(String, nullable=True)
    data_quality_entry = Column(String, nullable=True)
    description = Column(String, nullable=True)

    resource_id = Column(Integer, ForeignKey("resources.id"))

    resource = relationship("Resource", back_populates='process', uselist=False)

    def __init__(self, resource, units, flow, category, uncertainty, avoided_waste, provider, data_quality_entry, description):
        self.resource = resource
        self.units = units
        self.flow = flow
        self.category = category
        self.uncertainty = uncertainty
        self.avoided_waste = avoided_waste
        self.provider = provider
        self.data_quality_entry = data_quality_entry
        self.description = description