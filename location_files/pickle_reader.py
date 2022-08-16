import pandas as pd

obj = pd.read_pickle(r'postal_codes.pickle')
dict_items = obj.items()
print("ok")
