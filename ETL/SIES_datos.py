from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DateTime, Table, ForeignKey
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql.expression import true
from sqlalchemy.sql.sqltypes import BOOLEAN, Boolean, Float
from time import time
from sqlalchemy import Date, VARCHAR
import csv
from numpy import genfromtxt
import pandas as pd

engine = create_engine('postgresql://postgres:admin123@localhost:5432/BaseSIES')
engine.begin()

# Lectura de archivos CSV
oferta_historico_df = pd.read_csv('ETL\Oferta_Academica_Historico.csv', error_bad_lines = False, sep = ";")
matriculas_historico_df = pd.read_csv('ETL\Matriculas_Historico.csv', error_bad_lines = False, sep = ";")
titulados_historico_df = pd.read_csv('ETL\Titulados_Historico.csv', error_bad_lines = False, sep = ";")

# Lectura de personal academico
personal_academico_comun_df = pd.DataFrame()
for i in range(7):
    aux_df = pd.read_csv('ETL\Personal_Academico_Comun_' + str(2013 + i) + '.csv', error_bad_lines = False,
    skiprows = 2, usecols = [i for i in range(59)],  sep = ";")
  
    aux_df.columns = ['id_institucion', 'nombre', 'cantidad_total', 'cantidad_mujeres', 'cantidad_hombres',
    'promedio_edad', 'promedio_edad_mujeres', 'promedio_edad_hombres', 'cantidad_mujeres_edad_menos_35', 'cantidad_mujeres_edad_entre_35_44',
    'cantidad_mujeres_edad_entre_45_54', 'cantidad_mujeres_edad_entre_55_64', 'cantidad_mujeres_edad_mas_64', 'sin_info_edad_mujeres',
    'cantidad_hombres_edad_menos_35', 'cantidad_hombres_edad_entre_35_44', 'cantidad_hombres_edad_entre_45_54', 'cantidad_hombres_edad_entre_55_64',
    'cantidad_hombres_edad_mas_64', 'sin_info_edad_hombres', 'cantidad_trabaja_1_institucion', 'cantidad_trabaja_2_instituciones', 
    'cantidad_trabaja_3_mas_instituciones', 'cantidad_doctorado', 'cantidad_magister', 'cantidad_especialidad_medica', 'cantidad_profesional', 'cantidad_licenciatura',
    'cantidad_tecnico_superior', 'cantidad_tecnico_medio', 'cantidad_enseñanza_media', 'sin_info_formacion', 'cantidad_region_1', 'cantidad_region_2',
    'cantidad_region_3', 'cantidad_region_4', 'cantidad_region_5', 'cantidad_region_6', 'cantidad_region_7', 'cantidad_region_8', 'cantidad_region_9',
    'cantidad_region_10', 'cantidad_region_11', 'cantidad_region_12', 'cantidad_region_13', 'cantidad_region_14', 'cantidad_region_15', 'sin_info_region',
    'cantidad_nacional', 'cantidad_extranjero', 'sin_info_origen', 'promedio_horas_academicas', 'promedio_horas_academicas_mujeres', 'promedio_horas_academicas_hombres',
    'cantidad_menos_11_horas_academicas', 'cantidad_entre_11_23_horas_academicas', 'cantidad_entre_23_39_horas_academicas', 'cantidad_mas_39_horas_academicas',
    'sin_info_horas']
    aux_años = [str(i + 6)]*len(aux_df)
    aux_jornada = [str(1)]*len(aux_df)
    aux_df['id_tiempo'] = aux_años
    aux_df['id_contrato'] = aux_jornada
    aux_df.drop(aux_df[aux_df.id_institucion == '0'].index, inplace=True)
    personal_academico_comun_df = pd.concat([personal_academico_comun_df, aux_df], ignore_index = True, sort = True)


# Ingreso de años
años = {'año_registro': [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]}
años_df = pd.DataFrame(años, columns = ['año_registro'])
años_df.to_sql(con = engine, index = False, name = 'sies_tiempo_d', if_exists = 'append')

# Ingreso de contratos
contratos = {'tipo': ['No JCE', 'JCE']}
contratos_df = pd.DataFrame(contratos, columns = ['tipo'])
contratos_df.to_sql(con = engine, index = False, name = 'sies_contrato_d', if_exists = 'append')

# Ingreso de instituciones
instituciones_oferta_df = oferta_historico_df.iloc[:, [2, 3, 4, 12, 13, 14]]
instituciones_oferta_df.columns = ['clasificacion_nivel_1', 'clasificacion_nivel_2', 'clasificacion_nivel_3', 
'tipo_institucion', 'id', 'nombre_institucion']
instituciones_personal_df = personal_academico_comun_df.iloc[:, [46, 48]]
instituciones_personal_df.columns = ['id', 'nombre_institucion']
instituciones_personal_df['id'] = instituciones_personal_df['id'].fillna('delete')
instituciones_personal_df.drop(instituciones_personal_df[instituciones_personal_df.id == 'delete'].index, inplace=True)
aux_cla1 = ['desconocido']*len(instituciones_personal_df)
aux_cla2 = ['desconocido']*len(instituciones_personal_df)
aux_cla3 = ['desconocido']*len(instituciones_personal_df)
aux_tipo = ['desconocido']*len(instituciones_personal_df)
instituciones_personal_df['clasificacion_nivel_1'] = aux_cla1
instituciones_personal_df['clasificacion_nivel_2'] = aux_cla2
instituciones_personal_df['clasificacion_nivel_3'] = aux_cla3
instituciones_personal_df['tipo_institucion'] = aux_tipo
instituciones_df = pd.concat([instituciones_oferta_df, instituciones_personal_df], ignore_index = True, sort = True)

instituciones_df = instituciones_df.drop_duplicates(subset = ['id'], keep = 'first')
instituciones_df.to_sql(con = engine, index = False, name = 'sies_institucion_d', if_exists = 'append')

# Ingreso de sedes
sedes_df = oferta_historico_df.iloc[:, [5,6,7,13,16]]
sedes_df.columns = ['region','provincia','comuna','id_institucion','nombre_sede']
sedes_df = sedes_df.drop_duplicates(subset=['id_institucion','nombre_sede'], keep='first')
sedes_df.to_sql(con = engine, index = False, name = 'sies_sede_d', if_exists = 'append')

# Ingreso de personal academico
personal_academico_comun_df.drop(['nombre', 'sin_info_edad_mujeres', 'sin_info_edad_hombres', 'sin_info_formacion', 'sin_info_region',
'sin_info_origen', 'sin_info_horas'], axis = 1, inplace = True)
columns = personal_academico_comun_df.columns


for i in columns:
    if 'id_' not in i: 
        personal_academico_comun_df[i] = personal_academico_comun_df[i].replace(r'\.', '', regex = True)
        personal_academico_comun_df[i] = personal_academico_comun_df[i].replace(r'\,', '.', regex = True)
        personal_academico_comun_df[i] = personal_academico_comun_df[i].replace(r' \-   ', '0', regex = True)
        personal_academico_comun_df[i] = personal_academico_comun_df[i].replace(r'\-', '0', regex = True)
        personal_academico_comun_df[i] = personal_academico_comun_df[i].astype(float)
        
        if 'cantidad' in i:    
            personal_academico_comun_df[i] = personal_academico_comun_df[i].fillna(0)
            personal_academico_comun_df[i] = personal_academico_comun_df[i].astype(int)

personal_academico_comun_df.to_sql(con = engine, index = False, name = 'sies_personal_academico_f', if_exists = 'append')

# Ingreso de area_generica
area_generica_df = oferta_historico_df.iloc[:, [11]]
area_generica_df.columns = ['area_generica']
area_generica_df = area_generica_df.drop_duplicates(subset=['area_generica'], keep='first')
area_generica_df.to_sql(con = engine, index = False, name = 'sies_area_generica_d', if_exists = 'append')

# Ingreso de area_conocimiento
area_conocimiento_df = oferta_historico_df.iloc[:, [8]]
area_conocimiento_df.columns = ['area_conocimiento']
area_conocimiento_df = area_conocimiento_df.drop_duplicates(subset=['area_conocimiento'], keep='first')
area_conocimiento_df.to_sql(con = engine, index = False, name = 'sies_area_conocimiento_d', if_exists = 'append')

#Ingreso area_oecd
area_oecd_df = oferta_historico_df.iloc[:, [9]]
area_oecd_df.columns = ['area_oecd']
area_oecd_df = area_oecd_df.drop_duplicates(subset=['area_oecd'], keep='first')
area_oecd_df.to_sql(con = engine, index = False, name = 'sies_area_oecd_d', if_exists = 'append')

#Ingreso subarea_oecd
subarea_oecd_df = oferta_historico_df.iloc[:, [9,10]]
subarea_oecd_df.columns = ['area_oecd','area_subarea_oecd']   
subarea_oecd_df = subarea_oecd_df.drop_duplicates(subset=['area_subarea_oecd'], keep='first')


print('0')
# agregar columna con claves foraneas de area_oecd
area_oecd_table = pd.read_sql(sql = "select * from sies_area_oecd_d", con = engine)
for index, row in subarea_oecd_df.iterrows():
    subarea_oecd_df.at[index, 'id_area_oecd'] = area_oecd_table.loc[
        (area_oecd_table['area_oecd'] == subarea_oecd_df.at[index, 'area_oecd']).idxmax(),'id']

subarea_oecd_df.iloc[:, [1,2]].to_sql(con = engine, index = False, name = 'sies_subarea_oecd_d', if_exists = 'append')



# Ingreso carrera_registro
carrera_registro_df = oferta_historico_df.iloc[:, [10,11,8,0,16,13,    1,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,50,51,52,53,54,55,56,57,58,59,60,61,62]]
carrera_registro_df.columns = ['subarea_ecde','area_generica','area_conocimiento','año','nombre_sede','id_institucion',

               'codigo_unico','nombre_carrera','modalidad','jornada','version','tipo_carrera','plan_especial','duracion_estudios','duracion_titulacion','duracion_total',
               'regimen','duracion_formal_regimen','nombre_titulo','grado_academico','nivel_global','nivel_carrera','demre', 'acreditacion','elegibilidad_beca_pedagogia',
               'vigencia', 'año_inicio', 'pedagogia_medicina_otro','requisito_ingreso','semestres_reconocidos','ramos_requeridos_egreso','area_actual',
               'ponderacion_notas', 'ponderacion_ranking', 'ponderacion_lenguaje', 'ponderacion_matematicas', 'ponderacion_historia', 'ponderacion_ciencias', 'ponderacion_otros',
               'vacantes_semestre_1','vacantes_semestre_2','costo_matricula','costo_titulacion','costo_certificado','costo_arancel']

# agregar columna con claves foraneas de subarea_oecd
print('1')
subarea_oecd_table = pd.read_sql(sql = "select * from sies_subarea_oecd_d", con = engine)
for index, row in carrera_registro_df.iterrows():
    carrera_registro_df.at[index, 'id_subarea_oecd'] = subarea_oecd_table.loc[
        (subarea_oecd_table['area_subarea_oecd'] == carrera_registro_df.at[index, 'subarea_ecde']).idxmax(),'id']

# agregar columna con claves foraneas de area_generica
print('2')
area_generica_table = pd.read_sql(sql = "select * from sies_area_generica_d", con = engine)
for index, row in carrera_registro_df.iterrows():
    carrera_registro_df.at[index, 'id_area_generica'] = area_generica_table.loc[
        (area_generica_table['area_generica'] == carrera_registro_df.at[index, 'area_generica']).idxmax(),'id']

# agregar columna con claves foraneas de area_conocimiento
print('3')
area_conocimiento_table = pd.read_sql(sql = "select * from sies_area_conocimiento_d", con = engine)
for index, row in carrera_registro_df.iterrows():
    carrera_registro_df.at[index, 'id_area_conocimiento'] = area_conocimiento_table.loc[
        (area_conocimiento_table['area_conocimiento'] == carrera_registro_df.at[index, 'area_conocimiento']).idxmax(),'id']
        
# agregar columna con claves foraneas de tiempo
for index, row in carrera_registro_df.iterrows():
    carrera_registro_df.at[index, 'id_tiempo'] = int(carrera_registro_df.at[index, 'año'][-4:]) - 2007

# agregar columna con claves foraneas de sede
print('4')
sede_table = pd.read_sql(sql = "select * from sies_sede_d", con = engine)
for index, row in carrera_registro_df.iterrows():
    carrera_registro_df.at[index, 'id_sede'] = sede_table.loc[((sede_table['nombre_sede'] == carrera_registro_df.at[index, 'nombre_sede']) & (sede_table['id_institucion'] == carrera_registro_df.at[index, 'id_institucion'])).idxmax(),'id']


#Ingreso a la base de datos
carrera_registro_df.iloc[:, 6:].to_sql(con = engine, index = False, name = 'sies_carrera_registro_f', if_exists = 'append')


#Obtenemos los valores de la tabla sies_carrera_registro_f para obtener el id(clave foranea)
carrera_registro_table = pd.read_sql(sql = "select * from sies_carrera_registro_f", con = engine)

#Ingreso carrera titulados
carrera_titulados_df = titulados_historico_df.iloc[:, [0,26,   1,2,3,28,29,30,31,32,33,35,36,37]]
carrera_titulados_df.columns = ['id_tiempo','codigo_unico',
'cantidad_total_titulados', 'cantidad_titulados_mujer', 'cantidad_titulados_hombre', 
'cantidad_titulados_edad_menos_20', 'cantidad_titulados_edad_entre_20_24', 'cantidad_titulados_edad_entre_25_29',
'cantidad_titulados_edad_entre_30_34','cantidad_titulados_edad_entre_35_39','cantidad_titulados_edad_mas_39',
'promedio_edad_titulados','promedio_edad_titulados_mujeres','promedio_edad_titulados_hombres']

#se obtiene id_tiempo para buscar clave foranea
print('5')
for index, row in carrera_titulados_df.iterrows():
    carrera_titulados_df.at[index, 'id_tiempo'] = int(carrera_titulados_df.at[index, 'id_tiempo'][-4:]) - 2007

#Obtencion clave foranea carrera_registro, cuando coincide el id_tiempo y el codigo_unico
print('6')
for index, row in carrera_titulados_df.iterrows():
    carrera_titulados_df.at[index, 'id_carrera_registro'] = carrera_registro_table.loc[((carrera_registro_table['id_tiempo'] == carrera_titulados_df.at[index, 'id_tiempo']) & (carrera_registro_table['codigo_unico'] == carrera_titulados_df.at[index, 'codigo_unico'])).idxmax(),'id']

carrera_titulados_df.iloc[:, 2:].to_sql(con = engine, index = False, name = 'sies_carrera_titulados_f', if_exists = 'append')

#Ingreso carrera matriculados
carrera_matriculados_df = matriculas_historico_df.iloc[:, [0,30,   1,2,3,4,5,6,32,33,34,35,36,37,47,42,43,44,45,46,48,49,50,51,52,39,40,41]]
carrera_matriculados_df.columns = ['id_tiempo','codigo_unico',
'cantidad_matriculados_total', 'cantidad_matriculados_mujeres', 'cantidad_matriculados_hombres', 
'cantidad_total_matriculados_1er_año', 'cantidad_mujeres_matriculados_1er_año', 'cantidad_hombres_matriculados_1er_año',
'cantidad_matriculados_menos_20','cantidad_matriculados_entre_20_24','cantidad_matriculados_entre_25_29',
'cantidad_matriculados_entre_30_34','cantidad_matriculados_entre_35_39','cantidad_matriculados_mas_39','cantidad_matriculados_total_tes',
'cantidad_matriculados_tes_municipal','cantidad_matriculados_tes_particular_subvencionado','cantidad_matriculados_tes_particular_pagado',
'cantidad_matriculados_tes_corpor_admin_delegada','cantidad_matriculados_tes_servicio_local','porcentaje_cobertura_matriculados_tes',
'cantidad_matriculados_establecimiento_hc','cantidad_matriculados_establecimiento_tp','cantidad_matriculados_adulto',
'cantidad_matriculados_joven','promedio_edad_matriculados','promedio_edad_matriculados_mujeres','promedio_edad_matriculados_hombres']

#se obtiene id_tiempo para buscar clave foranea
print('7')
for index, row in carrera_matriculados_df.iterrows():
    carrera_matriculados_df.at[index, 'id_tiempo'] = int(carrera_matriculados_df.at[index, 'id_tiempo'][-4:]) - 2007

print('8')
for index, row in carrera_matriculados_df.iterrows():
    carrera_matriculados_df.at[index, 'id_carrera_registro'] = carrera_registro_table.loc[((carrera_registro_table['id_tiempo'] == carrera_matriculados_df.at[index, 'id_tiempo']) & (carrera_registro_table['codigo_unico'] == carrera_matriculados_df.at[index, 'codigo_unico'])).idxmax(),'id']

carrera_matriculados_df.iloc[:, 2:].to_sql(con = engine, index = False, name = 'sies_carrera_matricula_f', if_exists = 'append')

#carrera_matriculados_df.to_sql(con = engine, index = False, name = 'sies_carrera_matricula_f', if_exists = 'append')
#Falta clave foranea
#id = Column(Integer, primary_key=True, autoincrement = True)
#id_carrera_registro = Column(Integer, ForeignKey('sies_carrera_registro_f.id'))