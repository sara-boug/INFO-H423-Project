import os

from statsmodels.tsa.seasonal import seasonal_decompose
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from data_analysis.data_forcaster import DataForcaster


class DisplayDelay:
    metro_data = {'dframe': pd.DataFrame(), 'time': [], 'delay': [], 'ticks': 1}
    tram_data = {'dframe': pd.DataFrame(), 'time': [], 'delay': [], 'ticks': 1}
    bus_data = {'dframe': pd.DataFrame(), 'time': [], 'delay': [], 'ticks': 510}
    # containing the whole data
    dframe = {'dframe': pd.DataFrame(), 'time': [], 'delay': [], 'ticks': 510}

    def __init__(self, data_file):
        # self.generated_files_folder = generated_files_folder
        self.data_file = data_file

    def parse_file(self):

    def simplify_data(self):
        global_dframe = pd.DataFrame()
        dframe = pd.read_csv(self.data_file)
        print(dframe)
        """dframe = dframe.groupby(['actual_time']).mean()
        global_dframe = pd.concat([global_dframe, dframe])
        self.__set_vehicle_data(dframe)

        self.metro_data = self.__set_data(self.metro_data)
        self.bus_data = self.__set_data(self.bus_data, True)
        self.tram_data = self.__set_data(self.tram_data)

        # set the global data
        self.dframe['dframe'] = global_dframe
        self.dframe = self.__set_data(self.dframe, True)"""

    def plot_data(self):
        plt.minorticks_off()
        fig, axes = plt.subplots(3)
        self.__plot_subdata(self.tram_data, axes[0], title='Tram Data', color_map='limegreen')
        self.__plot_subdata(self.metro_data, axes[1], title='Metro Data', color_map='aqua')
        self.__plot_subdata(self.bus_data, axes[2], title='Bus Data', color_map='darkviolet')

        plt.show()

    @staticmethod
    def __plot_subdata(data, axes, title='', color_map='orange'):
        # limiting the number of ticks
        axes.plot(data['time'], data['delay'], linewidth=1, color=color_map)
        axes.set_xlabel('time of the day')
        axes.set_ylabel('delay (s)')
        axes.set_title(title)
        axes.set_xticks((data.index.tolist())[::data['ticks']], minor=False)
        axes.grid()

    def plot_data_decomposition(self):
        self.plot_subdata_decomposition(data_obj=self.dframe)
        # self.plot_subdata_decomposition(data_obj=self.metro_data)
        plt.show()

    @staticmethod
    def plot_subdata_decomposition(data_obj):
        data = data_obj['dframe']['delay']
        result = seasonal_decompose(x=data, model='additive', period=180)
        figure = result.plot(resid=False)
        for axe in figure.axes:
            axe.set_xticks(data.index[::data_obj['ticks']], minor=False)
            axe.grid()

    def __set_vehicle_data(self, dframe: pd.DataFrame):
        metro_condition = ((dframe['line_id'] == 1) | (dframe['line_id'] == 2) | (dframe['line_id'] == 5) | (dframe[
                                                                                                                 'line_id'] == 6))
        tram_condition = ((dframe['line_id'] == 3) | (dframe['line_id'] == 4) | (dframe['line_id'] == 7))
        bus_condition = dframe['line_id'] > 7

        # metro data
        data_frame = self.__arrange_data(dframe[metro_condition])
        self.metro_data['dframe'] = pd.concat([self.metro_data['dframe'], data_frame])
        # tram data
        data_frame = self.__arrange_data(dframe.loc[tram_condition])
        self.tram_data['dframe'] = pd.concat([self.tram_data['dframe'], data_frame])
        # bus data data
        data_frame = self.__arrange_data(dframe.loc[bus_condition])
        self.bus_data['dframe'] = pd.concat([self.bus_data['dframe'], data_frame])

    @staticmethod
    def __arrange_data(dframe):
        dframe = dframe.groupby(['actual_time']).mean()
        dframe = dframe.sort_index()
        return dframe

    @staticmethod
    def __set_data(obj, remove_outlier=False):
        if remove_outlier:
            obj['dframe'] = obj['dframe'][obj['dframe']['delay'] <= 1000]
        dframe = obj['dframe'].groupby(['actual_time']).mean().sort_index()
        obj['dframe'] = dframe
        obj['time'] = np.array(dframe.index.tolist())
        obj['delay'] = dframe['delay'].tolist()
        return obj

    def start_forcasting(self):
        forcaster = DataForcaster(self.dframe['dframe'])
        forcaster.set_data_shape()
        # forcaster.plot()
        # forcaster.define_d_param()
        # forcaster.define_p_q_param()
        forcaster.perform_training()
