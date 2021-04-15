import time

import sinapi
from navisworks import Navisworks, NavisworksElement, NavisworksUnits
from navisworks import catalog as cat_
from navisworks import configuration
from navisworks import properties as prop_
from sqlalchemy.sql import func

from models import Base, engine, session
from models import sinapimodels as sm

Base.metadata.create_all(engine)

units_convert = {
    'KG': {'Coefficient': 1, 'formula': 'ModelWeight', 'units': 'kilogram'},
    'UN': {'Coefficient': 1, 'formula': 'Count', 'units': 'count'},
    'M2': {'Coefficient': 1, 'formula': 'ModelArea', 'units': 'squaremeter'},
    'H': {'Coefficient': 1, 'formula': 'ModelTime', 'units': 'hour'},
    'M': {'Coefficient': 1, 'formula': 'ModelLength', 'units': 'meter'},
    'L': {'Coefficient': 1 / 1000, 'formula': 'ModelVolume', 'units': 'cubicmeter'},
    '18L': {'Coefficient': 18 / 1000, 'formula': 'ModelVolume', 'units': 'cubicmeter'},
    '200KG': {'Coefficient': 200, 'formula': 'ModelWeight', 'units': 'kilogram'},
    'M3': {'Coefficient': 1, 'formula': 'ModelVolume', 'units': 'cubicmeter'},
    'MÊS': {'Coefficient': 1, 'formula': 'ModelTime', 'units': 'month'},
    'DM3': {'Coefficient': 1 / 1000, 'formula': 'ModelVolume', 'units': 'cubicmeter'},
    'PAR': {'Coefficient': 1, 'formula': 'Count', 'units': 'count'},
    'JG': {'Coefficient': 1, 'formula': 'Count', 'units': 'count'},
    'MIL': {'Coefficient': 1000, 'formula': 'Count', 'units': 'count'},
    'T': {'Coefficient': 1 / 907.1847, 'formula': 'ModelWeight', 'units': 'kilogram'},
    '50KG': {'Coefficient': 50, 'formula': 'ModelWeight', 'units': 'kilogram'},
    'CJ': {'Coefficient': 1, 'formula': 'Count', 'units': 'count'},
    '100M': {'Coefficient': 100, 'formula': 'ModelLength', 'units': 'meter'},
    'CENTO': {'Coefficient': 100, 'formula': 'Count', 'units': 'count'},
    'GL': {'Coefficient': 1, 'formula': 'Count', 'units': 'count'},
    'SC25KG': {'Coefficient': 25, 'formula': 'ModelWeight', 'units': 'kilogram'},
    '310ML': {'Coefficient': 310 / 1000000, 'formula': 'Volume', 'units': 'cubicmeter'},
    'CHP': {'Coefficient': 1, 'formula': 'ModelTime', 'units': 'hour'},
    'CHI': {'Coefficient': 1, 'formula': 'ModelTime', 'units': 'hour'},
    'ML': {'Coefficient': 1 / 1000000, 'formula': 'ModelVolume', 'units': 'cubicmeter'},
    'MES': {'Coefficient': 1, 'formula': 'ModelTime', 'units': 'month'},
    'KW/H': {'Coefficient': 1, 'formula': '', 'units': ''},
    'M2XMES': {'Coefficient': 720, 'formula': 'ModelArea*ModelTime', 'units': ''},
    'MXMES': {'Coefficient': 1, 'formula': 'ModelLength*ModelTime', 'units': ''},
    'TXKM': {'Coefficient': 1 / 907.1847, 'formula': 'ModelWeight*ModelDistance', 'units': ''},
    'M3XKM': {'Coefficient': 1, 'formula': 'ModelVolume*ModelDistance', 'units': ''},
    'KGXKM': {'Coefficient': 1, 'formula': 'ModelWeight*ModelDistance', 'units': ''},
    'UNXKM': {'Coefficient': 1, 'formula': 'Count*ModelDistance', 'units': ''},
    'M2XKM': {'Coefficient': 1, 'formula': 'ModelArea*ModelDistance', 'units': ''},
    'LXKM': {'Coefficient': 1 / 1000, 'formula': 'ModelVolume*ModelDistance', 'units': ''},
    'MXKM': {'Coefficient': 1, 'formula': 'ModelLength*ModelDistance', 'units': ''}
}


def create_column(name: str, type_: str, purpose: str, formula: str, formula_varies: bool,
                  units: str, units_group: str, units_varies: bool) -> configuration.Column:
    col = configuration.Column(name)
    col.type_.value = type_
    col.purpose.value = purpose
    col.formula.value = formula
    col.formula.varies = formula_varies
    col.units.value = units
    col.units.group = units_group
    col.units.varies = units_varies

    return col


def set_columnRef(table: configuration.Table, column: configuration.Column, **fields):
    col_ref = table.add_columnRef(column)

    for field_name, props in fields.items():
        field: configuration.Field = col_ref.__getattribute__(field_name)
        if not field:
            field = col_ref.create_field(field_name)

        for prop, value in props.items():
            field.__setattr__(prop, value)


count = [1]


def create_groups(group_db: sm.Group, catalog: cat_.Catalog):

    item_group = cat_.ItemGroup(group_db.id, group_db.name, group_db.code, group_db.parent_id)
    catalog.add_item_group(item_group)

    x = session.query(func.count(sm.Composition.id)).scalar()
    for composition in group_db.compositions:
        print(f'{count[0]} / {x}')
        count[0] += 1
        create_full_composition(composition, catalog)


def create_composition(composition_db: sm.Composition, catalog: cat_.Catalog) -> cat_.Item:
    item = cat_.Item(
        composition_db.id,
        composition_db.name,
        composition_db.code,
        composition_db.group_id,
        description=composition_db.name)
    primary_quantity = item.get_variable('PrimaryQuantity')
    primary_quantity.formula = '=' + units_convert[composition_db.units]['formula']
    primary_quantity.units = units_convert[composition_db.units]['units']

    catalog.add_item(item)

    for attribution in composition_db.grouped_attributions:
        create_attribution(attribution.Attribution, item, catalog, attribution.sum_coefficient)

    return item


def create_full_composition(composition_db: sm.Composition, catalog: cat_.Catalog) -> cat_.Item:
    item = cat_.Item(
        composition_db.id,
        composition_db.name,
        composition_db.code,
        composition_db.group_id,
        description=composition_db.name)
    primary_quantity = item.get_variable('PrimaryQuantity')
    primary_quantity.formula = '=' + units_convert[composition_db.units]['formula']
    primary_quantity.units = units_convert[composition_db.units]['units']

    catalog.add_item(item)

    for step in composition_db.steps:
        step: sm.Step = step

        if len(step.attributions):
            s = cat_.Step(step.id, step.name, step.composition_id)
            c = s.get_variable('Coefficient')
            c.formula = f"={step.coefficient}"
            item.append(s)

            for attribution in step.attributions:
                attribution: sm.Attribution = attribution

                step_resource = cat_.StepResource(attribution.id, attribution.step_id, attribution.resource_id)
                s.append(step_resource)

                coef = cat_.Variable('Coefficient', formula='=0')
                step_resource.add_variable(coef)

                primary_quantity = cat_.Variable('PrimaryQuantity')
                primary_quantity.units = units_convert[attribution.resource.units]['units']
                primary_quantity.formula = f'{item.get_variable("PrimaryQuantity").formula}*{coef.name}'
                step_resource.add_variable(primary_quantity)

                coef = step_resource.get_variable('Coefficient')
                coef.formula = f"={step.coefficient * attribution.coefficient * units_convert[attribution.resource.units]['Coefficient']}"

    return item


def create_resources(resource_db: sm.Resource, catalog: cat_.Catalog) -> cat_.Resource:
    resource = cat_.Resource(resource_db.id, resource_db.name, resource_db.code, description=resource_db.name)
    catalog.add_resource(resource)

    cost = resource.get_variable('UnitCost')
    cost.formula = f"={resource_db.cost / units_convert[resource_db.units]['Coefficient']}"

    primary_quantity = resource.get_variable('PrimaryQuantity')
    primary_quantity.units = units_convert[resource_db.units]['units']

    if resource_db.impact is not None:
        ozone_formation_terrestrial_ecosystems = resource.get_variable("OzoneFormationTerrestrialEcosystems")
        ozone_formation_terrestrial_ecosystems.formula = f"={resource_db.impact.ozone_formation_terrestrial_ecosystems:.13f} * Weight"

        water_consumption = resource.get_variable("WaterConsumption")
        water_consumption.formula = f"={resource_db.impact.water_consumption:.13f} * Weight"

        marine_eutrophication = resource.get_variable("MarineEutrophication")
        marine_eutrophication.formula = f"={resource_db.impact.marine_eutrophication:.13f} * Weight"

        marine_ecotoxicity = resource.get_variable("MarineEcotoxicity")
        marine_ecotoxicity.formula = f"={resource_db.impact.marine_ecotoxicity:.13f} * Weight"

        land_use = resource.get_variable("LandUse")
        land_use.formula = f"={resource_db.impact.land_use:.13f} * Weight"

        fine_particulate_matter_formation = resource.get_variable("FineParticulateMatterFormation")
        fine_particulate_matter_formation.formula = f"={resource_db.impact.fine_particulate_matter_formation:.13f} * Weight"

        mineral_resource_scarcity = resource.get_variable("MineralResourceScarcity")
        mineral_resource_scarcity.formula = f"={resource_db.impact.mineral_resource_scarcity:.13f} * Weight"

        ionizing_radiation = resource.get_variable("IonizingRadiation")
        ionizing_radiation.formula = f"={resource_db.impact.ionizing_radiation:.13f} * Weight"

        human_non_carcinogenic_toxicity = resource.get_variable("HumanNonCarcinogenicToxicity")
        human_non_carcinogenic_toxicity.formula = f"={resource_db.impact.human_non_carcinogenic_toxicity:.13f} * Weight"

        freshwater_eutrophication = resource.get_variable("FreshwaterEutrophication")
        freshwater_eutrophication.formula = f"={resource_db.impact.freshwater_eutrophication:.13f} * Weight"

        terrestrial_acidification = resource.get_variable("TerrestrialAcidification")
        terrestrial_acidification.formula = f"={resource_db.impact.terrestrial_acidification:.13f} * Weight"

        fossil_resource_scarcity = resource.get_variable("FossilResourceScarcity")
        fossil_resource_scarcity.formula = f"={resource_db.impact.fossil_resource_scarcity:.13f} * Weight"

        global_warming = resource.get_variable("GlobalWarming")
        global_warming.formula = f"={resource_db.impact.global_warming:.13f} * Weight"

        stratospheric_ozone_depletion = resource.get_variable("StratosphericOzoneDepletion")
        stratospheric_ozone_depletion.formula = f"={resource_db.impact.stratospheric_ozone_depletion:.13f} * Weight"

        terresetrial_ecotoxicity = resource.get_variable("TerresetrialEcotoxicity")
        terresetrial_ecotoxicity.formula = f"={resource_db.impact.terresetrial_ecotoxicity:.13f} * Weight"

        human_carcinogenic_toxicity = resource.get_variable("HumanCarcinogenicToxicity")
        human_carcinogenic_toxicity.formula = f"={resource_db.impact.human_carcinogenic_toxicity:.13f} * Weight"

        freshwater_ecotoxicity = resource.get_variable("FreshwaterEcotoxicity")
        freshwater_ecotoxicity.formula = f"={resource_db.impact.freshwater_ecotoxicity:.13f} * Weight"

        ozone_formation_human_health = resource.get_variable("OzoneFormationHumanHealth")
        ozone_formation_human_health.formula = f"={resource_db.impact.ozone_formation_human_health:.13f} * Weight"

    return resource


_attrib = set()


def create_attribution(
        attribution_db: sm.Attribution,
        item: cat_.Item,
        catalog: cat_.Catalog,
        coefficient: int) -> cat_.StepResource:

    step_resource = cat_.StepResource(attribution_db.id, attribution_db.step_id, attribution_db.resource_id)
    coef = cat_.Variable('Coefficient', formula='=0')
    step_resource.add_variable(coef)

    primary_quantity = cat_.Variable('PrimaryQuantity')
    primary_quantity.units = units_convert[attribution_db.resource.units]['units']
    primary_quantity.formula = f'{item.get_variable("PrimaryQuantity").formula}*{coef.name}'
    step_resource.add_variable(primary_quantity)
    item.append(step_resource)

    coef = step_resource.get_variable('Coefficient')
    coef.formula = f"={float(coef.formula.lstrip('=')) + attribution_db.coefficient * coefficient * units_convert[attribution_db.resource.units]['Coefficient']}"


def main():

    NavisworksUnits.set_unit_system('Metric')

    properties = prop_.TakeoffProperties()
    workbook = configuration.Workbook()
    takeoff = cat_.Takeoff()
    catalog = cat_.Catalog()

    global_configuration = configuration.GlobalConfiguration('Full')
    workbook.add_global_configuration(global_configuration)

    global_configuration.add_currency(configuration.Currency('real', 'BRL', 'R$'))

    takeoff.add_catalog(catalog)
    catalog_configuration = cat_.ConfigFile(workbook)

    takeoff.add_config_file(catalog_configuration)

    ObjectResource = configuration.ObjectResource()
    workbook.add_table(ObjectResource)

    StepResource = configuration.StepResource()
    workbook.add_table(StepResource)

    ObjectStep = configuration.ObjectStep()
    workbook.add_table(ObjectStep)

    Step = configuration.Step()
    workbook.add_table(Step)

    Object = configuration.Object()
    workbook.add_table(Object)

    Item = configuration.Item()
    workbook.add_table(Item)

    ItemGroup = configuration.ItemGroup()
    workbook.add_table(ItemGroup)

    Resource = configuration.Resource()
    workbook.add_table(Resource)

    ResourceGroup = configuration.ResourceGroup()
    workbook.add_table(ResourceGroup)
    _prop = [
        [
            'GeneralProperties', [
                ['ModelLength', 'length', 'feet', 'meter', 'Number', 'Calculation', True],
                ['ModelWidth', 'length', 'feet', 'meter', 'Number', 'Calculation', True],
                ['ModelThickness', 'length', 'feet', 'meter', 'Number', 'Calculation', True],
                ['ModelHeight', 'length', 'feet', 'meter', 'Number', 'Calculation', True],
                ['ModelPerimeter', 'length', 'feet', 'meter', 'Number', 'Calculation', True],
                ['ModelArea', 'area', 'squarefeet', 'squaremeter', 'Number', 'Calculation', True],
                ['ModelVolume', 'volume', 'cubicfeet', 'cubicmeter', 'Number', 'Calculation', True],
                ['ModelWeight', 'weight', 'pound', 'kilogram', 'Number', 'Calculation', True]
            ]
        ],
        [
            'AdditionalProperties', [
                ['ModelTime', 'time', 'hour', 'hour', 'Number', 'Calculation', True],
                ['ModelDistance', 'length', 'mile', 'kilometer', 'Number', 'Calculation', True]
            ]
        ]
    ]

    for prop_group in _prop:
        pg = prop_.PropertyGroup(prop_group[0])

        for prop in prop_group[1]:
            p = prop_.Property(*prop)

            pg.add_property(p)

        properties.add_property_group(pg)

    columns = [
        [
            create_column('Object', 'String', 'Input', None, False, None, 'any', False), {
                ObjectResource: {},
                ObjectStep: {},
                Object: {}
            }

        ],
        [
            create_column('ObjectId', 'String', 'Input', None, False, None, 'any', False), {
                ObjectResource: {},
                ObjectStep: {},
                Object: {}
            }

        ],
        [
            create_column('Description1', 'String', 'Input', None, False, None, 'any', False), {
                ObjectResource: {},
                Object: {}
            }
        ],
        [
            create_column('Description2', 'String', 'Input', None, False, None, 'any', False), {
                ObjectResource: {},
                Object: {}
            }
        ],
        [
            create_column('CreatedPhase', 'String', 'Input', None, False, None, 'any', False), {
                ObjectResource: {},
                Object: {}
            }
        ],
        [
            create_column('DemolishedPhase', 'String', 'Input', None, False, None, 'any', False), {
                ObjectResource: {},
                Object: {}
            }
        ]

    ]

    index = len(columns)
    for n, prop in enumerate(properties.properties(), index):
        col1, col2 = prop.as_column()

        columns.insert(
            n,
            [
                col1, {
                    ObjectResource: {},
                    ObjectStep: {},
                    Object: {}
                }
            ]
        )
        columns.append(
            [
                col2, {
                    ObjectResource: {},
                    StepResource: {},
                    ObjectStep: {},
                    Step: {},
                    Object: {},
                    Item: {},
                    Resource: {},
                }
            ]
        )

    columns += [
        [
            create_column('Count', 'Number', 'RollUp', None, True, 'count', 'any', False), {
                ObjectResource: {},
                StepResource: {},
                ObjectStep: {},
                Object: {},
                Item: {
                    'formula': {'value': '=1'}
                },
                Resource: {
                    'formula': {'value': '=1'}
                },
            }
        ],
        [
            create_column('PrimaryQuantity', 'Number', 'RollUp', None, True, None, 'any', True), {
                ObjectResource: {},
                StepResource: {},
                Object: {},
                Item: {},
                Resource: {}
            }
        ],
        [
            create_column('Coefficient', 'Number', 'RollUp', None, True, None, 'dimensionless', False), {
                ObjectResource: {},
                ObjectStep: {},
                StepResource: {},
                Resource: {
                    'formula': {'value': '=1'}
                },
                Step: {
                    'formula': {'value': '=1'}
                }
            }
        ],
        [
            create_column('UnitCost', 'Number', 'RollUp', None, True, 'currency', 'currency', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('Cost', 'Number', 'RollUp', None, True, 'currency', 'currency', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {
                    'formula': {'value': '=PrimaryQuantity*UnitCost'}
                }
            }
        ],
        [
            create_column('OzoneFormationTerrestrialEcosystems', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('WaterConsumption', 'Number', 'RollUp', None, True, 'cubicmeter', 'volume', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('MarineEutrophication', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('MarineEcotoxicity', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('LandUse', 'Number', 'RollUp', None, True, 'squaremeter', 'area', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('FineParticulateMatterFormation', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('MineralResourceScarcity', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('IonizingRadiation', 'Number', 'RollUp', None, True, None, 'dimensionless', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('HumanNonCarcinogenicToxicity', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('FreshwaterEutrophication', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('TerrestrialAcidification', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('FossilResourceScarcity', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('GlobalWarming', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('StratosphericOzoneDepletion', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('TerresetrialEcotoxicity', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('HumanCarcinogenicToxicity', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('FreshwaterEcotoxicity', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ],
        [
            create_column('OzoneFormationHumanHealth', 'Number', 'RollUp', None, True, 'kilogram', 'weight', False), {
                ObjectResource: {},
                StepResource: {},
                Resource: {}
            }
        ]
    ]

    for column_data in columns:
        column = column_data[0]
        global_configuration.add_column(column)

        for table, fields in column_data[1].items():
            set_columnRef(table, column, **fields)

    print('Recursos')
    resources = session.query(sm.Resource).all()
    for resource in resources:
        create_resources(resource, catalog)

    print('Groups')
    groups = session.query(sm.Group).all()
    for group in groups:
        create_groups(group, catalog)

    # print('Comps')
    # compositions = session.query(sm.Composition).all()
    # for n, composition in enumerate(compositions, 1):
    #     print(f'{n}/{len(compositions)}')
    #     create_full_composition(composition, catalog)
    # print(n)

    print('update')
    catalog_configuration.update_workbook()

    print('catalog')
    Navisworks(takeoff).write('output/Catalog_impacts.xml')


if __name__ == "__main__":
    ti = time.time()
    main()
    tf = time.time()
    print(tf - ti)
