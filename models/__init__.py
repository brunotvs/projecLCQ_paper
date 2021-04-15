import os

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///sinapi.db')
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


def main():
    from .sinapimodels import Resource, Composition, Group, Attribution
    try:
        os.remove("sinapi.db")
    except:
        pass
    
    Base.metadata.create_all(engine)

    # Teste de 3 recursos
    rasteleiro, alimentacao, transporte, exames, seguro, ferramentas, epi = (
        Resource('RASTELEIRO', '25961', 'H', 16.04),
        Resource('ALIMENTACAO - HORISTA (COLETADO CAIXA)', '37370', 'H', 2.62),
        Resource('TRANSPORTE - HORISTA (COLETADO CAIXA)', '37371', 'H', 0.57),
        Resource('EXAMES - HORISTA (COLETADO CAIXA)', '37372', 'H', 0.34),
        Resource('SEGURO - HORISTA (COLETADO CAIXA)', '37373', 'H', 0.04),
        Resource('FERRAMENTAS - FAMILIA OPERADOR ESCAVADEIRA - HORISTA (ENCARGOS COMPLEMENTARES - COLETADO CAIXA)', '43464', 'H', 0.01),
        Resource('EPI - FAMILIA OPERADOR ESCAVADEIRA - HORISTA (ENCARGOS COMPLEMENTARES - COLETADO CAIXA)', '43488', 'H', 0.65)
    )

    g1 = Group('SERVICOS DIVERSOS', 'SEDI')
    g2 = Group('OUTROS', '318')

    g1.children.append(g2)

    # Criar uma composição
    rast_comp = Composition('RASTELEIRO COM ENCARGOS COMPLEMENTARES', '88314', 'H', g2)
    curso_comp = Composition('CURSO DE CAPACITAÇÃO PARA RASTELEIRO (ENCARGOS COMPLEMENTARES) - HORISTA', '95376', 'H', g2)

    # Adicionar recursos à composição
    rast_comp.attributions.append(Attribution(rasteleiro, 1, rast_comp))
    rast_comp.attributions.append(Attribution(alimentacao, 1, rast_comp))
    rast_comp.attributions.append(Attribution(transporte, 1, rast_comp))
    rast_comp.attributions.append(Attribution(exames, 1, rast_comp))
    rast_comp.attributions.append(Attribution(seguro, 1, rast_comp))
    rast_comp.attributions.append(Attribution(ferramentas, 1, rast_comp))
    rast_comp.attributions.append(Attribution(epi, 1, rast_comp))
    rast_comp.attributions.append(Attribution(rasteleiro, 0.0041, curso_comp))

    session.add(rast_comp)
    session.commit()

    print(rast_comp.cost)

    rc: Composition = session.query(Composition).get(1)
    for ac in rc.direct_resources_attributions:
        print(ac.resource.cost)


if __name__ == "__main__":
    main()
