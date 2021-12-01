import numpy as np
import os
import pandas as pd
import matplotlib.pyplot as plt

online_offline_data_file = os.path.join(os.getcwd(), "data_preparation", "generated_files",
                                        "online_offline_files.txt")
online_offline_dataframe = pd.read_csv(online_offline_data_file, )
dframe_grouped = online_offline_dataframe.groupby("line_id")
print(online_offline_dataframe)
for state, dframe in dframe_grouped:
    plt.figure()
    dframe.plot(x="actual_time", y='speed')
    plt.show()
    break
