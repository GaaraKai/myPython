import pandas as pd
import numpy as np
import sys
import pandas as pd

words = ['aa','cc','dd','aa','aa','aa','aa','aa','a']
u_words = set(words)
tc_rst = pd.DataFrame({})
for wo in u_words:
    print('The number ' + wo + ' is ' + str(words.count(wo)))
    df = pd.DataFrame({"mer_a": [wo],
                       "mer_b": [str(words.count(wo))]})
    tc_rst = tc_rst.append(df, ignore_index=True)
print(tc_rst)
