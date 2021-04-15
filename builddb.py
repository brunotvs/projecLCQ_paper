import os
import time
from typing import List

import sinapi
from sqlalchemy.sql import func

from models import Base, engine, session
from models import sinapimodels as sm

import csv

try:
    os.remove('sinapi.db')
except BaseException:
    pass
Base.metadata.create_all(engine)


def main():
    Sinapi = sinapi.Sinapi(
        # analytic_path=r'D:\Users\Bruno\OneDrive\Documentos\Aulas\Mestrado\Dissertação\Sinapi\CSV\SINAPI_Custo_Ref_Composicoes_Analitico_MG_202004_Desonerado.csv')
        analytic_path=r'sinapi_csv\Analitico.CSV')

    for resource in Sinapi.iter_resources():
        resource: sinapi.Resource = resource
        sm.Resource(resource.name, resource.code, resource.units, resource.cost)

    for group in Sinapi.iter_higher_groups():
        group: sinapi.CompositionGroup = group
        groups(group)

    for composition, group_db in compositions.items():
        sm.Composition(composition.name, composition.code, composition.units, group_db)

    for composition, _ in compositions.items():
        create_attributions(composition)

    for composition in session.query(sm.Composition).filter_by(code=87502).all():
        for att in composition.grouped_attributions:
            print(att)

        print('\n\n')

    impacts(r"lci_csv\recipe_midpoint_h.csv")
    processess(r"lci_csv\flows.csv")


compositions = {}


def create_attributions(composition: sinapi.Composition):
    comp_db: sm.Composition = session.query(sm.Composition).filter_by(code=composition.code).first()
    session.add(comp_db)
    assert comp_db

    dir_step = sm.Step(composition.name, 1)
    dir_step.direct = True
    comp_db.steps.append(dir_step)
    for direct_resource_attribution in composition.direct_resources():
        direct_resource_attribution: sinapi.attribution = direct_resource_attribution
        res_sinapi: sinapi.Resource = direct_resource_attribution.item()

        resource = session.query(sm.Resource).filter_by(code=res_sinapi.code).one()
        dir_step.attributions.append(sm.Attribution(resource, direct_resource_attribution.coef))

    for composition_attribution in composition.compositions():
        composition_attribution: sinapi.attribution = composition_attribution
        aux_comp: sinapi.Composition = composition_attribution.item()
        step = sm.Step(aux_comp.name, composition_attribution.coef)

        comp_db.steps.append(step)

        for resource_attribution in aux_comp.grouped_resources():
            resource_attribution: sinapi.attribution = resource_attribution
            res_sinapi: sinapi.Resource = resource_attribution.item()

            resource = session.query(sm.Resource).filter_by(code=res_sinapi.code).one()
            step.attributions.append(sm.Attribution(resource, resource_attribution.coef))


def groups(group: sinapi.CompositionGroup, parent: sm.Group = None):

    group_db = sm.Group(group.name, group.code)
    if parent:
        parent.children.append(group_db)

    for comp in group.compositions():
        compositions[comp] = group_db

    for child in group.child():
        groups(child, group_db)


def impacts(file_pah: str):
    with open(file_pah, 'r', encoding='utf8') as file:
        reader = csv.DictReader(file,
                                ["resource_id",
                                 "units",
                                 "OzoneFormationTerrestrialEcosystems",
                                 "WaterConsumption",
                                 "MarineEutrophication",
                                 "MarineEcotoxicity",
                                 "LandUse",
                                 "FineParticulateMatterFormation",
                                 "MineralResourceScarcity",
                                 "IonizingRadiation",
                                 "HumanNonCarcinogenicToxicity",
                                 "FreshwaterEutrophication",
                                 "TerrestrialAcidification",
                                 "FossilResourceScarcity",
                                 "GlobalWarming",
                                 "StratosphericOzoneDepletion",
                                 "TerresetrialEcotoxicity",
                                 "HumanCarcinogenicToxicity",
                                 "FreshwaterEcotoxicity",
                                 "OzoneFormationHumanHealth"])
        next(reader)
        for row in reader:
            resource: sm.Resource = session.query(sm.Resource).filter_by(id=row["resource_id"]).one()

            sm.Impact(resource,
                      row["units"],
                      row["OzoneFormationTerrestrialEcosystems"],
                      row["WaterConsumption"],
                      row["MarineEutrophication"],
                      row["MarineEcotoxicity"],
                      row["LandUse"],
                      row["FineParticulateMatterFormation"],
                      row["MineralResourceScarcity"],
                      row["IonizingRadiation"],
                      row["HumanNonCarcinogenicToxicity"],
                      row["FreshwaterEutrophication"],
                      row["TerrestrialAcidification"],
                      row["FossilResourceScarcity"],
                      row["GlobalWarming"],
                      row["StratosphericOzoneDepletion"],
                      row["TerresetrialEcotoxicity"],
                      row["HumanCarcinogenicToxicity"],
                      row["FreshwaterEcotoxicity"],
                      row["OzoneFormationHumanHealth"])


def processess(file_pah: str):
    with open(file_pah, 'r', encoding='utf8') as file:
        reader = csv.DictReader(file, ["resource_id", "units", "flow", "category", "uncertainty",
                                       "avoided_waste", "provider", "data_quality_entry", "description"])
        next(reader)
        for row in reader:
            resource: sm.Resource = session.query(sm.Resource).filter_by(id=row["resource_id"]).one()
            sm.Process(resource, row["units"], row["flow"], row["category"], row["uncertainty"],
                       row["avoided_waste"], row["provider"], row["data_quality_entry"], row["description"])


if __name__ == "__main__":
    ti = time.time()
    main()
    session.commit()
    tf = time.time()
    print(tf - ti)
