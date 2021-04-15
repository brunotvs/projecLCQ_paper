import csv
from typing import List, Union, Iterator
import uuid


ListOfCompositionGroups = List["CompositionGroup"]
ListOfCompositions = List["Composition"]

def _hash_code(code: str) -> int:
    x = 0
    for i, char in enumerate(code):
        x += ord(char) * i

    return x

class Composition(object):

    def __init__(self, name: str, code: str, units: str, source: str, link: str):
        super().__init__()

        self.code = code
        self.name = name
        self.units = units
        self.link = link

    @property
    def code(self) -> str:
        return self._code

    @code.setter
    def code(self, value):
        self._code = value

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def units(self) -> str:
        return self._units

    @units.setter
    def units(self, value: str):
        self._units = value.strip()

    @property
    def link(self) -> str:
        return self._link

    @link.setter
    def link(self, value):
        self._link = value

    @property
    def id_(self) -> int:
        return Sinapi.__compositions_id__[self.code]

    def groups(self) -> ListOfCompositionGroups:
        for group in Sinapi.__composition_group__[self.id_]:
            yield group

    def attributions(self) -> Iterator['attribution']:
        for attrib in Sinapi.__composition_id_attributions__[self.id_]:
            yield attrib

    def resources(self) -> Iterator['attribution']:
        for attrib in self.attributions():
            if attrib.item_type == 'INSUMO':
                yield attrib

            else:
                coef = attrib.coef
                for res in attrib.item().resources():
                    sub_attrib = attribution(self.code, res.item_code, res.coef * coef, res.item_type)
                    yield sub_attrib

    def compositions(self) -> Iterator['attribution']:
        for attrib in self.attributions():
            if attrib.item_type == 'COMPOSICAO':
                yield attrib

    def direct_resources(self) -> Iterator['attribution']:
        for attrib in self.attributions():
            if attrib.item_type == 'INSUMO':
                yield attrib

    def grouped_resources(self) -> List['attribution']:
        resources = {}

        for att in self.resources():
            code = att.item_code
            if code in resources:
                resources[code].coef += att.coef
                continue

            resources[code] = attribution(att.composition_code, att.item_code, att.coef, att.item_type)

        return [att for _, att in resources.items()]

class CompositionGroup(object):

    def __init__(self, name: str, code: str):
        super().__init__()

        self.name = name
        self.code = code

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value: object):
        self._name = value

    @property
    def code(self):
        return self._code

    @code.setter
    def code(self, value: object):
        self._code = value

    @property
    def id_(self):
        return Sinapi.__groups_id__[self.code]

    def parent(self) -> 'CompositionGroup':
        return Sinapi.__group_parents__.get(self.id_, None)

    def child(self) -> Union[ListOfCompositionGroups, None]:
        for child in Sinapi.__group_child__.get(self.id_, []):
            yield child

    def compositions(self) -> Union[ListOfCompositions, None]:
        for composition in Sinapi.__group_composition__[self.id_]:
            yield composition


class Resource(object):

    def __init__(self, code: str, name: str, units: str, source: str, cost: str):
        super().__init__()

        self.code = code
        self.name = name
        self.units = units
        self.source = source
        self.cost = cost

    @property
    def code(self):
        return self._code

    @code.setter
    def code(self, value: object):
        self._code = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value: object):
        self._name = value

    @property
    def units(self):
        return self._units

    @units.setter
    def units(self, value: str):
        self._units = value.strip()

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, value: object):
        self._source = value

    @property
    def cost(self):
        return self._cost

    @cost.setter
    def cost(self, value: object):
        self._cost = value

    @property
    def id_(self):
        return Sinapi.__resources_id__[self.code]


class attribution(object):

    __item_types__ = ['Composition', 'Resource']

    def __init__(self, composition_code: str, item_code: str, coef: str, item_type: str):
        super().__init__()

        self.composition_code = composition_code
        self.item_code = item_code
        self.coef = coef
        self.item_type = item_type

        composition_id = Sinapi.__compositions_id__[composition_code]

        if f'{composition_code}.{item_code}' not in Sinapi.__attribution_id__:
            id_ = uuid.uuid3(uuid.NAMESPACE_OID,f'{composition_code}.{item_code}').int % 10**10

            Sinapi.__attribution_id__[f'{composition_code}.{item_code}'] = id_
            Sinapi.__attributions__[id_] = self

    @property
    def item_type(self):
        return self._item_type

    @item_type.setter
    def item_type(self, value: object):
        self._item_type = value

    @property
    def composition_code(self):
        return self._composition_code

    @composition_code.setter
    def composition_code(self, value: object):
        self._composition_code = value

    @property
    def item_code(self):
        return self._item_code

    @item_code.setter
    def item_code(self, value: str):
        self._item_code = value

    @property
    def coef(self):
        return self._coef

    @coef.setter
    def coef(self, value: str):
        self._coef = value

    def item(self) -> Union[Resource, Composition]:
        if self.item_type == 'INSUMO':
            return Sinapi.__resources__[Sinapi.__resources_id__[self.item_code]]
        elif self.item_type == 'COMPOSICAO':
            return Sinapi.__compositions__[Sinapi.__compositions_id__[self.item_code]]
        else:
            raise ValueError(f'invalid item_code: {item_code}')

    @property
    def composition_id(self):
        return Sinapi.__compositions_id__[self.composition_code]

    @property
    def item_id(self):
        return Sinapi.__compositions_id__[self.item_code] if self.item_code in Sinapi.__compositions_id__ else Sinapi.__resources_id__[self.item_code]

    @property
    def id_(self):
        return Sinapi.__attribution_id__[f'{self.composition_code}.{self.item_code}']


class Sinapi(object):

    # Composition.id: Compostion
    __compositions__ = {}
    #Composition.code: Composition.id
    __compositions_id__ = {}

    # GroupComposition.id: GroupComposition
    __groups__ = {}
    # GroupComposition.code: GroupComposition.id
    __groups_id__ = {}
    # GroupComposition.id: set(GroupComposition.id's)
    __group_child__ = {}
    # GroupComposition.id: set(GroupComposition.id's)
    __group_parents__ = {}

    # Composition.id: set(GroupCompostion)
    __composition_group__ = {}
    # GroupComposition.id: set(Compostion)
    __group_composition__ = {}

    # Resource.code: Resource
    __resources__ = {}
    # Resource.id: Resource.code
    __resources_id__ = {}

    # Composition.id: list(attribution)
    __composition_id_attributions__ = {}
    # {Composition.code}.{Item.code}: attribution.id
    __attribution_id__ = {}
    # attribution.id: attribution
    __attributions__ = {}

    def __init__(self, /, synthetic_path: str = None, analytic_path: str = None, resource_path: str= None):
        super().__init__()
        if synthetic_path:
            self.synthetic_parse(synthetic_path)
        if analytic_path:
            self.analytic_parse(analytic_path)
        if resource_path:
            self.resource_parse(resource_path)

    @classmethod
    def synthetic_parse(cls, synthetic_path) -> None:
        cls.check_synthetic(synthetic_path)

        with open(synthetic_path, 'r', encoding='utf8') as synthetic:
            reader = csv.DictReader(synthetic, ['class_name', 'class_code', 'type_name', 'type_code', 'group_code', 'group_name',
                                                'composition_code', 'composition_name', 'units', 'source', 'cost', 'link'])

            for i, row in enumerate(reader):

                if i == 0:
                    continue
                class_name = row['class_name'] if row['class_name'] else None
                class_code = row['class_code'] if row['class_code'] else None
                type_name = row['type_name'] if row['type_name'] else None
                type_code = row['type_code'] if row['type_code'] else None
                group_code = row['group_code'] if row['group_code'] else None
                group_name = row['group_name'] if row['group_name'] else None
                composition_name = row['composition_name'] if row['composition_name'] else None
                composition_code = row['composition_code'] if row['composition_code'] else None
                units = row['units'] if row['units'] else None
                source = row['source'] if row['source'] else None
                link = row['link'] if row['link'] else None

                composition = cls.add_composition(composition_name, composition_code, units, source, link)
                cls.add_group(class_name, class_code, composition)
                cls.add_group(type_name, type_code, composition, class_code)
                cls.add_group(group_name, group_code, composition, type_code)

    @classmethod
    def analytic_parse(cls, analytic_path: str) -> None:
        cls.check_analytic(analytic_path)

        with open(analytic_path, 'r', encoding='utf8') as attrib:
            reader = csv.DictReader(attrib, ['class_name', 'class_code', 'type_name', 'type_code', 'group_code', 'group_name',
                                             'composition_code', 'composition_name', 'units', 'source', 'cost', 'item_type',
                                             'item_code', 'item_name', 'item_units', 'item_source', 'coef', 'unit_cost', 'total_cost',
                                             'labor_cost', 'labor_percentage', 'materia_cost', 'material_percentage',
                                             'equipment_cost', 'equipment_percentage', 'third_party_cost',
                                             'third_party_percentage', 'other_cost', 'other_percentage', 'link'
                                             ])

            for j, row in enumerate(reader):

                if j == 0:
                    continue
                class_name = row['class_name'] if row['class_name'] else None
                class_code = row['class_code'] if row['class_code'] else None
                type_name = row['type_name'] if row['type_name'] else None
                type_code = row['type_code'] if row['type_code'] else None
                group_code = row['group_code'] if row['group_code'] else None
                group_name = row['group_name'] if row['group_name'] else None
                composition_code = row['composition_code'] if row['composition_code'] else None
                composition_name = row['composition_name'] if row['composition_name'] else None
                units = row['units'] if row['units'] else None
                source = row['source'] if row['source'] else None
                cost = float(row['cost'].replace('.', '').replace(',', '.')) if row['cost'] else None
                item_type = row['item_type'] if row['item_type'] else None
                item_code = row['item_code'] if row['item_code'] else None
                item_name = row['item_name'] if row['item_name'] else None
                item_units = row['item_units'] if row['item_units'] else None
                item_source = row['item_source'] if row['item_source'] else None
                unit_cost = float(row['unit_cost'].replace('.', '').replace(',', '.')) if row['unit_cost'] else None
                coef = float(row['coef'].replace('.', '').replace(',', '.')) if row['coef'] else None
                link = row['link'] if row['link'] else None

                if not item_type:
                    continue

                composition = cls.add_composition(composition_name, composition_code, units, source, link)
                cls.add_group(class_name, class_code, composition)
                cls.add_group(type_name, type_code, composition, class_code)
                cls.add_group(group_name, group_code, composition, type_code)
                if item_type == 'INSUMO':
                    cls.add_resource(item_code, item_name, item_units, item_source, unit_cost)
                cls.add_attribution(composition_code, item_code, coef, item_type)

    @classmethod
    def resource_parse(cls, resource_path: str) -> None:
        cls.check_resource(resource_path)

        with open(resource_path, 'r', encoding='utf8') as resource:
            reader = csv.DictReader(resource, ['code', 'name', 'units', 'source', 'cost'])

            for i, row in enumerate(reader):

                if i == 0:
                    continue

                code = row['code'] if row['code'] else None
                name = row['name'] if row['name'] else None
                units = row['units'] if row['units'] else None
                source = row['source'] if row['source'] else None
                cost = float(row['cost'].replace('.', '').replace(',', '.')) if row['cost'] else None

                if not code:
                    break

                resource = cls.add_resource(code, name, units, source, cost)

    @classmethod
    def add_group(cls, name: str, code: str, composition: Composition, parent_code: str = None) -> CompositionGroup:
        if not name:
            return

        if code in cls.__groups_id__:
            id_ = cls.__groups_id__[code]
            group = cls.__groups__[id_]

        else:
            id_ = len(cls.__groups_id__) + 1
            group = CompositionGroup(name, code)
            parent_id = cls.__groups_id__[parent_code] if parent_code else None

            cls.__groups_id__[code] = id_
            cls.__groups__[id_] = group

            if parent_id in cls.__group_child__:
                cls.__group_child__[parent_id].add(group)
            else:
                cls.__group_child__[parent_id] = {group}

            if id_ not in cls.__group_parents__ and parent_id:
                cls.__group_parents__[id_] = cls.__groups__[parent_id]

        if composition.id_ in cls.__composition_group__:
            cls.__composition_group__[composition.id_].add(group)
        else:
            cls.__composition_group__[composition.id_] = {group}

        if id_ in cls.__group_composition__:
            cls.__group_composition__[id_].add(composition)
        else:
            cls.__group_composition__[id_] = {composition}

        return group

    @classmethod
    def add_composition(cls, name: str, code: str, units: str, source: str, link: str) -> Composition:
        if not name:
            return

        if code in cls.__compositions_id__:
            id_ = cls.__compositions_id__[code]
        else:
            id_ = len(cls.__compositions_id__) + 1
            cls.__compositions_id__[code] = id_
            composition = Composition(name, code, units, source, link)
            cls.__compositions__[id_] = composition

        return cls.__compositions__[id_]

    @classmethod
    def add_resource(cls, code: str, name: str, units: str, source: str, cost: str) -> Resource:
        if not name:
            return

        if code in cls.__resources_id__:
            id_ = cls.__resources_id__[code]
        else:
            id_ = len(cls.__resources_id__) + 1
            cls.__resources_id__[code] = id_
            resource = Resource(code, name, units, source, cost)
            cls.__resources__[id_] = resource

        return cls.__resources__[id_]

    @classmethod
    def add_attribution(cls, composition_code: str, item_code: str, coef: str, item_type: str) -> attribution:
        if not composition_code:
            return

        composition_id = cls.__compositions_id__[composition_code]

        if f'{composition_code}.{item_code}' in cls.__attribution_id__:
            id_ = cls.__attribution_id__[f'{composition_code}.{item_code}']
        else:
            id_ = uuid.uuid3(uuid.NAMESPACE_OID,f'{composition_code}.{item_code}').int % 10**10

            cls.__attribution_id__[f'{composition_code}.{item_code}'] = id_

            attrib = attribution(composition_code, item_code, coef, item_type)
            cls.__attributions__[id_] = attrib

            if composition_id in cls.__composition_id_attributions__:
                cls.__composition_id_attributions__[composition_id].append(attrib)
            else:
                cls.__composition_id_attributions__[composition_id] = [attrib]

        return cls.__attributions__[id_]

    @staticmethod
    def check_synthetic(synthetic_path: str):
        pass

    @staticmethod
    def check_analytic(analytic_path: str):
        pass

    @staticmethod
    def check_resource(resource_path: str):
        pass

    def get_composition_by_id(self, id_: int) -> Composition:
        return self.__compositions__[id_]

    def get_composition_by_code(self, code: str) -> Composition:
        return self.__compositions__[self.__compositions_id__[code]]

    def get_group_by_id(self, id_: int) -> CompositionGroup:
        return self.__groups__[id_]

    def get_group_by_code(self, code: str) -> CompositionGroup:
        return self.__groups__[self.__groups_id__[code]]

    def get_resource_by_id(self, id_: int) -> Resource:
        return self.__resources__[id_]

    def get_resource_by_code(self, code: str) -> Resource:
        return self.__resources__[self.__resources_id__[code]]

    def iter_higher_groups(self) ->Iterator[CompositionGroup]:
        '''yields all groups that have no parents'''
        for group in self.__group_child__[None]:
            yield group

    def iter_compositions(self) -> Iterator[Composition]:
        for _, composition in self.__compositions__.items():
            yield composition

    def iter_resources(self) -> Iterator[Resource]:
        for _, resource in self.__resources__.items():
            yield resource


