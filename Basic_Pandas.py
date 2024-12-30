#1. Import pandas
import pandas as pd
#read csv into df
df = pd.read_csv('ecto-oldmain.csv', index_col='date')
print(df)
#2. Create a new dataframe from an existing dataframe.
dfnew = df[['Security','Wails','Jump-Scares']]
print(dfnew)
#3. Print only certain columns
print(df[['Jump-Scares','AvgTemp']])
#4. Reference a single row
print(df.loc['17-Oct'])
#5. Rename Columns
df.rename(columns={'Wails':'Shrieks','Security':'Guard'}, inplace = True)
print(df)
#6. Replace values within the dataframe
df['Guard'].replace(to_replace = ["H","O","S"], value = ["Hildegard","Oscar","Sherry"], inplace = True)
print(df)
#7. Replace null values
import numpy as np
df['AvgTemp'].replace(to_replace=np.nan,value='32',inplace = True)
print(df)
#8. Concatenating dataframes
df2 = pd.read_csv('ecto-rainfall.csv', index_col='Date')
print(df2)
df2['Precip'].replace(to_replace=np.nan,value='0',inplace=True)
dffull = pd.concat([df,df2],axis=1)
print(dffull)
#9. Aggregation
print("The average Ectograph value is %.3f." % (dffull['Ectograph'].agg('mean')))
#10. Aggregation with a group.
dfx = dffull[['Shrieks','Guard']]
print(dfx[['Guard','Shrieks']].groupby('Guard').describe())
print("Hildegard is associated with the fewest shrieks with an average of 9.4.")
