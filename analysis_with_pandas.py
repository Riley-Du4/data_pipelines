#1. Load the Dataframe
import pandas as pd
import numpy as np
df=pd.read_csv('griselbridge.csv',index_col= 'bridgeid')
df['material'].replace(to_replace=np.nan,value="unknown", inplace=True)
print(df)
#2. Aggregating by Groups
print(df[['material','length']].groupby('material').agg(['mean','median']))
print("""the average for brick bridges is 2.9 with a median of 2.8.
The average for stone bridges is 3.04 with a median of 3.1.
The average for unknown materials is 4 with a median of 4.2.
The average for wood bridges is 2.9 with a median of 2.9""")
print(df[['age','cursed','mosscover']].groupby('cursed').describe())
print("Cursed bridges tend to be older on average and more covered in moss.")
#3 Creating a Calculated Column
df['mossqty']=df['mosscover']*df['length']
df['mossrate']=df['mossqty']/df['age']
print(df[['mosscover','length','age','mossqty','mossrate']])
print(df[['mossrate','material']].groupby('material').agg(['mean','max','min']))
print(df[['mossrate','covered']].groupby('covered').agg(['mean','median']))
print("""On average, moss grows .85 linear units per epoch on brick; .89 on stone; 1.4 on wood; and .98 on unknown materials.""")
print("""Covered bridges encourages slower moss growth.""")
#4 create Conditional Columns
df['guardian'].dropna(inplace=True)

df['flow']= np.where(
    (df['waterbody'] == 'river') | (df['waterbody'] == 'creek'), "y","n")

conditions = [df['guardian'] == 'orge',
              df['guardian'] == 'troll',
              df['guardian'] == 'squirrel',
              df['guardian'] == 'none']
values = [.1,.2,.01, 0]
df['threatlvl'] = np.select(conditions,values)
print(df[['waterbody','flow','guardian','threatlvl']])
#5 Follow-Up Questions
print(df[['flow','threatlvl']].groupby('flow').agg(['mean','median']))
print("bridges over non-flowing water are more dangerous")
print(df[['material','threatlvl']].groupby('material').agg(['mean','median']))
print("On average bridges made of unknown material are more dangerous")
print(df[['material','flow','threatlvl']].groupby(['material','flow']).agg('mean'))
print("Exluding unkown material, the bridges with the highest threat level are bridges made of wood and no flow.")
#6 "Removing" Rows from a Dataframe
df2 = df[(df['age'] > 1) & (df['material'] == 'stone')|(df['material']=='brick') & (df['length'] >= 4.2)]
print(df2)
print("The number of bridges that meet the requirements are", df2['material'].agg('count'))
#7 Creating a New Dataframe Excluding Nulls
dfa = df.dropna()
print(dfa)
print("The number of records is", dfa['material'].agg('count'))
#8 Challenge
A= dfa[(dfa.material == 'wood') & (dfa.waterbody == 'river')]
print(A)
B= dfa[(dfa.material == 'brick') & (dfa.waterbody == 'creek')]
print(B)
C= dfa[(dfa.material == 'stone') & (dfa.waterbody == 'marsh')]
print(C)
print("A Threat level probability:", A['threatlvl'].agg('mean'))
print("B Threat level probability:", B['threatlvl'].agg('mean'))
print("C Threat level probability:", C['threatlvl'].agg('mean'))
print("I would prefer to cross bridge B because the probability of being attacked is 7%.")
