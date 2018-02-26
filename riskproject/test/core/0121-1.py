import pandas as pd
import numpy as np
import sys


mer_id=pd.Series(np.array(["11","11","11","22","22","33","33","44"]))
device_id=pd.Series(np.array(["A","A","B","B","C","C","C","C"]))
inst_trace=pd.Series(np.array([1,2,3,4,5,6,7,8]))
df=pd.DataFrame({"mer_id":mer_id,"device_id":device_id,"inst_trace":inst_trace})
# print(type(s1))
print(df)
aa = pd.pivot_table(df, index=["mer_id","device_id"],values=["inst_trace"],aggfunc=len).reset_index()
ee = aa.sort_values(by="inst_trace", axis=0,ascending=False)
print(ee)

sys.exit(0)
trx_count = pd.pivot_table(df, index=["mer_id"],values=["inst_trace"],aggfunc=len).reset_index()
print(trx_count)

ds_count = pd.pivot_table(df, index=["mer_id","device_id"],aggfunc=len).reset_index()
# print(ds_count)
ds_count2 = pd.pivot_table(ds_count, index=["mer_id"],values=["device_id"],aggfunc=len).reset_index()
print(ds_count2)
merge = pd.merge(left=trx_count,right=ds_count2,how="inner")
print(merge)