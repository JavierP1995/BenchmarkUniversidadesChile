from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DateTime, Table, ForeignKey
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql.expression import true
from sqlalchemy.sql.sqltypes import BOOLEAN, Boolean, Float

#Conexion entre la aplicacion y el gestor de DB
engine = create_engine('postgresql://postgres:admin123@localhost:5432/BaseSIES')

#Genera la clase base para poder crear la cantidad de modelos que queramos
Base = declarative_base()

#Recibe la clase base como parametro porque necesita heredear de ella


class sies_tiempo_d(Base):

    __tablename__ = 'sies_tiempo_d'

    id = Column(Integer, primary_key = True, autoincrement = True)

    año_registro = Column(Integer, nullable = False)

    sies_personal_academico_f = relationship("sies_personal_academico_f") 
    sies_carrera_registro_f = relationship("sies_carrera_registro_f")

class sies_contrato_d(Base):

    __tablename__ = 'sies_contrato_d'

    id = Column(Integer, primary_key = True, autoincrement = True)

    tipo = Column(String, nullable = False)

    sies_personal_academico_f = relationship("sies_personal_academico_f") 
 
class sies_institucion_d(Base):

    __tablename__ = 'sies_institucion_d'

    id = Column(Integer, primary_key = True, nullable = False)

    nombre_institucion = Column(String)
    tipo_institucion = Column(String, nullable = True)
    clasificacion_nivel_1 = Column(String)
    clasificacion_nivel_2 = Column(String)
    clasificacion_nivel_3 = Column(String)

    sies_personal_academico_f = relationship("sies_personal_academico_f") 
    sies_sede_d = relationship("sies_sede_d")

class sies_sede_d(Base):

    __tablename__ = 'sies_sede_d'

    id = Column(Integer, primary_key = True, autoincrement = True)
    id_institucion = Column(Integer, ForeignKey('sies_institucion_d.id'))

    nombre_sede = Column(String, nullable = False)
    comuna = Column(String)
    provincia = Column(String)
    region = Column(String)

    sies_carrera_registro_f = relationship("sies_carrera_registro_f")

class sies_area_conocimiento_d(Base):

    __tablename__ = 'sies_area_conocimiento_d'

    id = Column(Integer, primary_key = True, autoincrement = True)

    area_conocimiento = Column(String, nullable = False)

    sies_carrera_registro_f = relationship("sies_carrera_registro_f")

class sies_area_generica_d(Base):

    __tablename__ = 'sies_area_generica_d'

    id = Column(Integer, primary_key = True, autoincrement = True)

    area_generica = Column(String, nullable = False)

    sies_carrera_registro_f = relationship("sies_carrera_registro_f")

class sies_subarea_oecd_d(Base):

    __tablename__ = 'sies_subarea_oecd_d'

    id = Column(Integer, primary_key = True, autoincrement = True)
    id_area_oecd = Column(Integer, ForeignKey('sies_area_oecd_d.id'))

    area_subarea_oecd = Column(String, nullable = False)

    sies_carrera_registro_f = relationship("sies_carrera_registro_f")

class sies_area_oecd_d(Base):

    __tablename__ = 'sies_area_oecd_d'

    id = Column(Integer, primary_key = True, autoincrement = True)

    area_oecd = Column(String, nullable = False)

    sies_subarea_oecd_d = relationship("sies_subarea_oecd_d")


class sies_personal_academico_f(Base):

    __tablename__ = 'sies_personal_academico_f'

    id = Column(Integer, primary_key=True, autoincrement = True)
    id_tiempo = Column(Integer, ForeignKey('sies_tiempo_d.id'))
    id_contrato = Column(Integer, ForeignKey('sies_contrato_d.id'))
    id_institucion = Column(Integer, ForeignKey('sies_institucion_d.id'))

    cantidad_total = Column(Integer)
    cantidad_hombres = Column(Integer)
    cantidad_mujeres = Column(Integer)
    cantidad_hombres_edad_menos_35 = Column(Integer)
    cantidad_hombres_edad_entre_35_44 = Column(Integer)
    cantidad_hombres_edad_entre_45_54 = Column(Integer)
    cantidad_hombres_edad_entre_55_64 = Column(Integer)
    cantidad_hombres_edad_mas_64 = Column(Integer)
    cantidad_mujeres_edad_menos_35 = Column(Integer)
    cantidad_mujeres_edad_entre_35_44 = Column(Integer)
    cantidad_mujeres_edad_entre_45_54 = Column(Integer)
    cantidad_mujeres_edad_entre_55_64 = Column(Integer)
    cantidad_mujeres_edad_mas_64 = Column(Integer)
    cantidad_trabaja_1_institucion = Column(Integer)
    cantidad_trabaja_2_instituciones = Column(Integer)
    cantidad_trabaja_3_mas_instituciones = Column(Integer)
    cantidad_doctorado = Column(Integer)
    cantidad_magister = Column(Integer)
    cantidad_profesional = Column(Integer)
    cantidad_licenciatura = Column(Integer)
    cantidad_tecnico_superior = Column(Integer)
    cantidad_tecnico_medio = Column(Integer)
    cantidad_enseñanza_media = Column(Integer)
    cantidad_especialidad_medica = Column(Integer)
    cantidad_region_1 = Column(Integer)
    cantidad_region_2 = Column(Integer)
    cantidad_region_3 = Column(Integer)
    cantidad_region_4 = Column(Integer)
    cantidad_region_5 = Column(Integer)
    cantidad_region_6 = Column(Integer)
    cantidad_region_7 = Column(Integer)
    cantidad_region_8 = Column(Integer)
    cantidad_region_9 = Column(Integer)
    cantidad_region_10 = Column(Integer)
    cantidad_region_11 = Column(Integer)
    cantidad_region_12 = Column(Integer)
    cantidad_region_13 = Column(Integer)
    cantidad_region_14 = Column(Integer)
    cantidad_region_15 = Column(Integer)
    cantidad_nacional = Column(Integer)
    cantidad_extranjero = Column(Integer)
    cantidad_menos_11_horas_academicas = Column(Integer)
    cantidad_entre_11_23_horas_academicas = Column(Integer)
    cantidad_entre_23_39_horas_academicas = Column(Integer)
    cantidad_mas_39_horas_academicas = Column(Integer)
    
    promedio_edad = Column(Float)
    promedio_edad_hombres = Column(Float)
    promedio_edad_mujeres = Column(Float)
    promedio_horas_academicas = Column(Float)
    promedio_horas_academicas_hombres = Column(Float)
    promedio_horas_academicas_mujeres = Column(Float)

class sies_carrera_registro_f(Base):

    __tablename__ = 'sies_carrera_registro_f'

    id = Column(Integer, primary_key=True, autoincrement = True)
    id_tiempo = Column(Integer, ForeignKey('sies_tiempo_d.id'))
    id_sede = Column(Integer, ForeignKey('sies_sede_d.id'))
    id_area_conocimiento = Column(Integer, ForeignKey('sies_area_conocimiento_d.id'))
    id_area_generica = Column(Integer, ForeignKey('sies_area_generica_d.id'))
    id_subarea_oecd = Column(Integer, ForeignKey('sies_subarea_oecd_d.id'))

    codigo_unico = Column(String, nullable = False)
    nombre_carrera = Column(String)
    modalidad = Column(String) 
    jornada = Column(String) 
    version = Column(Integer) 
    tipo_carrera = Column(String) 
    plan_especial = Column(String) 
    duracion_estudios = Column(Integer) 
    duracion_titulacion = Column(Integer) 
    duracion_total = Column(Integer) 
    regimen = Column(String)
    duracion_formal_regimen = Column(Integer) 
    nombre_titulo = Column(String) 
    grado_academico = Column(String) 
    nivel_global = Column(String)
    nivel_carrera = Column(String)
    demre = Column(Integer) 
    acreditacion = Column(String) 
    elegibilidad_beca_pedagogia = Column(String) 
    vigencia = Column(String) 
    año_inicio = Column(String) 
    pedagogia_medicina_otro = Column(String)
    requisito_ingreso = Column(String)
    semestres_reconocidos = Column(Integer)
    ramos_requeridos_egreso = Column(Integer)
    area_actual = Column(String)   
    ponderacion_notas = Column(Integer)
    ponderacion_ranking = Column(Integer)
    ponderacion_lenguaje = Column(Integer)
    ponderacion_matematicas = Column(Integer)
    ponderacion_historia = Column(Integer)
    ponderacion_ciencias = Column(Integer)
    ponderacion_otros = Column(Integer)
    vacantes_semestre_1 = Column(Integer)
    vacantes_semestre_2 = Column(Integer)
    costo_matricula = Column(Integer)
    costo_titulacion = Column(Integer)
    costo_certificado = Column(Integer)
    costo_arancel = Column(Integer)

    sies_carrera_titulados_f = relationship("sies_carrera_titulados_f")
    sies_carrera_matriculas_f = relationship("sies_carrera_matriculas_f")
    
class sies_carrera_titulados_f(Base):

    __tablename__ = 'sies_carrera_titulados_f'

    id = Column(Integer, primary_key=True, autoincrement = True)
    id_carrera_registro = Column(Integer, ForeignKey('sies_carrera_registro_f.id'))
    
    cantidad_total_titulados = Column(Integer)
    cantidad_titulados_hombres = Column(Integer)
    cantidad_titulados_mujeres = Column(Integer)
    cantidad_titulados_edad_menos_20 = Column(Integer)
    cantidad_titulados_edad_entre_20_24 = Column(Integer)
    cantidad_titulados_edad_entre_25_29 = Column(Integer)
    cantidad_titulados_edad_entre_30_34 = Column(Integer)
    cantidad_titulados_edad_entre_35_39 = Column(Integer)
    cantidad_titulados_edad_mas_39 = Column(Integer)
    promedio_edad_titulados = Column(Float)
    promedio_edad_titulados_hombres = Column(Float)
    promedio_edad_tituladas_mujeres = Column(Float)

class sies_carrera_matricula_f(Base):

    __tablename__ = 'sies_carrera_matricula_f'

    id = Column(Integer, primary_key=True, autoincrement = True)
    id_carrera_registro = Column(Integer, ForeignKey('sies_carrera_registro_f.id'))

    cantidad_matriculados_total = Column(Integer)
    cantidad_matriculados_hombres = Column(Integer)
    cantidad_matriculados_mujeres = Column(Integer)
    cantidad_total_matriculados_1er_año = Column(Integer)
    cantidad_hombres_matriculados_1er_año = Column(Integer)
    cantidad_mujeres_matriculados_1er_año = Column(Integer)
    cantidad_matriculados_menos_20 = Column(Integer)
    cantidad_matriculados_entre_20_24 = Column(Integer)
    cantidad_matriculados_entre_25_29 = Column(Integer)
    cantidad_matriculados_entre_30_34 = Column(Integer)
    cantidad_matriculados_entre_35_39 = Column(Integer)
    cantidad_matriculados_mas_39 = Column(Integer)
    cantidad_matriculados_total_tes = Column(Integer)
    cantidad_matriculados_tes_municipal = Column(Integer)
    cantidad_matriculados_tes_particular_subvencionado = Column(Integer)
    cantidad_matriculados_tes_particular_pagado = Column(Integer)
    cantidad_matriculados_tes_corpor_admin_delegada = Column(Integer)
    cantidad_matriculados_tes_servicio_local = Column(Integer)
    porcentaje_cobertura_matriculados_tes = Column(Float)
    cantidad_matriculados_establecimiento_hc = Column(Integer)
    cantidad_matriculados_establecimiento_tp = Column(Integer)
    cantidad_matriculados_adulto = Column(Integer)
    cantidad_matriculados_joven = Column(Integer)
    promedio_edad_matriculados = Column(Float)
    promedio_edad_matriculados_hombres = Column(Float)
    promedio_edad_matriculados_mujeres = Column(Float)

#Establece la relacion entre la conexion y los modelos
Session = sessionmaker(engine)
session = Session()

if __name__ == '__main__':
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    session.commit()