import os

from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.graphics.tsaplots import plot_acf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class DisplayDelay:
    actual_time = 0
    delay = 0
    dframe = None

    def __init__(self, generated_files_folder):
        self.generated_files_folder = generated_files_folder
        self.generated_files = os.listdir(generated_files_folder)

    def simplify_data(self):
        file = os.path.join(self.generated_files_folder, self.generated_files[0])
        dframe = pd.read_csv(file)
        dframe = dframe.sort_values(by=["actual_time"])
        dframe = dframe.groupby(['actual_time']).mean()
        self.dframe = dframe
        self.actual_time = np.array(dframe.index.tolist())
        self.delay = dframe['delay'].tolist()

    def plot_data(self):
        fig, axes = plt.subplots()
        # limiting the number of ticks
        axes.plot(self.actual_time, self.delay)
        for i, tick in enumerate(axes.xaxis.get_ticklabels()):
            if i % 50 == 0:
                tick.set_visible(True)
                continue
            tick.set_visible(False)
        plt.show()

    def plot_correlated_data(self):
        data = self.dframe[['delay']]
        plot_acf(x=data, lags=50)
        plt.show()

    def data_decomposition(self):
        data = self.dframe[['delay']]
        result = seasonal_decompose(x=data, model='additive', period=60)

        figure = result.plot()
        for axe in figure.axes:
            for i, tick in enumerate(axe.xaxis.get_ticklabels()):
                if i % 50 == 0:
                    tick.set_visible(True)
                    continue
                tick.set_visible(False)

        plt.show()
