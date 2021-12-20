import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error


class DataForcaster:
    p = 1
    q = 1
    d = 0
    delays = None

    def __init__(self, dframe_data: pd.DataFrame):
        self.dframe_data = dframe_data

    def set_data_shape(self):
        self.dframe_data.index = pd.to_datetime(self.dframe_data.index, format="%d/%m/%Y %H:%M:%S",
                                                infer_datetime_format=True)
        self.dframe_data = self.dframe_data.groupby(pd.Grouper(freq='5min')).mean()
        self.dframe_data = self.dframe_data.dropna()
        self.delays = self.dframe_data['delays'].tolist()

    def plot(self, delay=None, d=0):
        if delay is None:
            delay = self.delays
        time = self.dframe_data.index.tolist()
        for i in range(d): time.pop()
        fig, axes = plt.subplots()
        axes.plot(time, delay, linewidth=1, color='orange')
        axes.set_xlabel('Date')
        axes.set_ylabel('Delay(s)')
        axes.set_xticks(time[::700], minor=False)
        plt.grid()
        plt.show()

    def define_params(self, plot=None):
        self.p = 1
        self.q = 1
        self.d = 0
        model = ARIMA(self.delays, order=(0, 0, 0))
        model_fit = model.fit(disp=1)
        data = model_fit.resid
        print(model_fit.summary())
        figure, axes = plt.subplots(2)
        plot_acf(x=data, lags=25, ax=axes[0])
        plot_pacf(x=data, lags=25, ax=axes[1])
        plt.show()

    def perform_training(self):
        data_size = len(self.delays)

        percentage = int(data_size * 0.66)
        time_index = (self.dframe_data.index.tolist())[percentage:data_size-1]
        train, test = self.delays[0:percentage], self.delays[percentage:data_size - 1]

        history = [element for element in train]
        predictions = list()
        for i in range(len(test)):
            model = ARIMA(history, order=(self.p, self.d, self.q))
            model_fit = model.fit()
            predicted = (model_fit.forecast())
            predictions.append(predicted[0])
            expected = test[i]
            history.append(expected)
        mse = mean_squared_error(test, predictions)
        plt.plot(time_index,test, )
        plt.plot(time_index,predictions,  color='red', alpha=0.5)
        plt.xlim([np.min(time_index), np.max(time_index)])
        plt.title(' The mean Squared Error {}'.format(mse))
        plt.show()
